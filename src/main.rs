mod server;
mod shutdown;

use std::string::ToString;
use tokio::net::{TcpListener};
use scylla::{Session, SessionBuilder};
use anyhow::Result;
use std::time::Duration;
use server::Scired;
use clap::{Parser};
use config::Config;
use tokio::sync::{broadcast, mpsc};
//use serde::{Deserialize, Serialize};
use crate::server::{SciredConfig,OpsConsistency};
use tracing::{info, error};
use tokio::signal;


// TODO:
// - graceful restart
// - proper logging and tracing
// - use lib.rs
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let settings = Config::builder()
        .add_source(config::File::with_name(&cli.scired_config))
        .build()
        .unwrap();
    let cfg: Cfg = settings.try_deserialize().unwrap();

    let session: Session = SessionBuilder::new()
        .known_node(cli.scylla_node)
        .connection_timeout(Duration::from_secs(2))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build().await?;

    let listener = TcpListener::bind(cli.listen_addr).await.unwrap();

    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let sc_config = cfg.build_scired_config();
    let sc = Scired::new(sc_config, session, notify_shutdown, shutdown_complete_tx).await.unwrap();

    //sc.run(listener).await;

    tokio::select! {
        res = sc.run(listener) => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = signal::ctrl_c() => {
            // The shutdown signal has been received.
            info!("shutting down");
            println!("shutting down");
        }
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    let Scired {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = sc;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` below can complete
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;
    Ok(())
}
#[derive(Debug)]
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Peers to connect to.
    #[arg(long = "scylla_node", default_value_t = DEFAULT_SCYLLA_NODE.to_string())]
    scylla_node: String,

    /// Port to listen on for tcp connections.
    #[arg(short = 'l', long = "listen_addr", default_value_t = DEFAULT_LISTEN_ADDR.to_string())]
    listen_addr: String,

    #[arg(long = "scired_config", default_value_t = DEFAULT_SCIRED_CONFIG.to_string())]
    scired_config: String,

    /// Enable debug logging
    #[arg(short = 'd', long = "debug", default_value_t = false)]
    debug: bool,
}

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:6379";
const DEFAULT_SCYLLA_NODE: &str = "127.0.0.1:9042";
const DEFAULT_SCIRED_CONFIG: &str = "config.yaml";

#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct Cfg {
    consistency_level: ConsistencyLevel,
}


use scylla::statement::Consistency;
#[derive(Debug, Default, serde_derive::Deserialize, PartialEq, Eq)]
struct ConsistencyLevel {
    get: String,
    set: String,
}

impl Cfg {
    // TODO: I think this fn shouldn't be here, but i don't where to put it properly
    fn build_scired_config(&self) -> SciredConfig {
       let ops_consistency = OpsConsistency{
           set: self.str_to_consistency(&self.consistency_level.set),
           get: self.str_to_consistency(&self.consistency_level.get),
       };
       SciredConfig {
           ops_consistency: ops_consistency,
       }
    }
    fn str_to_consistency(&self, val: &str) -> Consistency {
        match val.to_lowercase().as_str() {
            "one" => Consistency::One,
            "two"  => Consistency::Two,
            "quorum" => Consistency::Quorum,
            _      => Consistency::Quorum,
        }
    }
}





