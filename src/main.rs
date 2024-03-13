mod server;

use tokio::net::{TcpListener};
use scylla::{Session, SessionBuilder};
use anyhow::Result;
use std::time::Duration;
use server::Scired;


// TODO:
// - graceful restart
// - proper logging and tracing
// - config file
#[tokio::main]
async fn main() -> Result<()> {
    let session: Session = SessionBuilder::new()
        .known_node("127.0.0.1:9042".to_string())
        .connection_timeout(Duration::from_secs(2))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .build().await?;

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let sc = Scired::new(session).await.unwrap();

    sc.run(listener).await;
    Ok(())
}





