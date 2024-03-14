mod server;

use std::string::ToString;
use tokio::net::{TcpListener};
use scylla::{Session, SessionBuilder};
use anyhow::Result;
use std::time::Duration;
use server::Scired;
use clap::{Parser};


// TODO:
// - graceful restart
// - proper logging and tracing
// - config file
// - use lib.rs
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let session: Session = SessionBuilder::new()
        .known_node(cli.scylla_node)
        .connection_timeout(Duration::from_secs(2))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build().await?;

    let listener = TcpListener::bind(cli.listen_addr).await.unwrap();

    let sc = Scired::new(session).await.unwrap();

    sc.run(listener).await;
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

    /// Enable debug logging
    #[arg(short = 'd', long = "debug", default_value_t = false)]
    debug: bool,
}

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:6379";
const DEFAULT_SCYLLA_NODE: &str = "127.0.0.1:9042";





