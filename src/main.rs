use std::collections::HashMap;
use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use scylla::{Session, SessionBuilder};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;

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
    let sm = ScyllaMgr::new(session).await.unwrap();
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

async fn handle(socket: TcpStream, sm: Arc<ScyllaMgr>) {
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

struct ScyllaMgr {
    sess: Arc<Session>,
    ps: HashMap<String, Arc<PreparedStatement>>,
}

impl ScyllaMgr {
    async fn new(session: Session) -> Result<ScyllaMgr> {
        let sess = Arc::new(session);
        let mut ps_map = HashMap::new();

        // prepare the get
        let mut pg = sess
            .prepare("select value from scired.strings where key=?")
            .await?;
        pg.set_consistency(Consistency::One); // TODO: make the consisntency level to be configurable
        ps_map.insert("get".to_string(), Arc::new(pg));

        // prepare the set
        let mut ps = sess
            .prepare("insert into scired.strings (key, value) values (?, ?)")
            .await?;
        ps.set_consistency(Consistency::One); // TODO: make the consisntency level to be configurable
        ps_map.insert("set".to_string(), Arc::new(ps));


        let sm = ScyllaMgr{
            sess:sess,
            ps: ps_map,
        };
        Ok(sm)
    }

    async fn get(&self, key: String) -> Frame {
        let sess = self.sess.clone();
        let ps = self.ps.get("get").unwrap().clone();
        match sess.execute(&ps, (key,)).await {
            Ok(res) => {
                let val = res.first_row_typed::<(String,)>();
                match val {
                    Ok((str,)) => Frame::Bulk(Bytes::from(str)),
                    Err(_) => Frame::Null,
                }
            },
            Err(e) => Frame::Error(e.to_string()),
        }
    }

    async fn set(&self, key: &str, val: &str) -> Frame {
        let sess = self.sess.clone();
        let ps = self.ps.get("set").unwrap().clone();
        let res = sess
            .execute(&ps,(key, val))
            .await;
        let status = match res {
            Err(e) => Frame::Error(e.to_string()), // TODO: make sure it follows redis protocol
            Ok(_) => Frame::Simple("OK".to_string()),
        };
        status
    }
}



