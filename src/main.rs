mod scyllamgr;

use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection};
use scylla::{Session, SessionBuilder};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;


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
    let sm = scyllamgr::ScyllaMgr::new(session).await.unwrap();
    let sm = Arc::new(sm);

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let sm = sm.clone();
        tokio::spawn(async move {
            handle(socket, sm).await
        });

    }
}

async fn handle(socket: TcpStream, sm: Arc<scyllamgr::ScyllaMgr>) {
    use mini_redis::Command::{self, Get,Set};

    let mut conn = Connection::new(socket);

    while let Some(frame) = conn.read_frame().await.unwrap() {
        let resp = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let val_str = std::str::from_utf8(&cmd.value()).unwrap();
                sm.set(cmd.key(),val_str).await
            }
            Get(cmd) => {
                sm.get(cmd.key().to_string()).await
            }
            cmd=> panic!("unimplemented command {:?}", cmd),
        };
        conn.write_frame(&resp).await.unwrap();
    }
}




