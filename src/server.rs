use std::collections::HashMap;
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use scylla::{Session};
use anyhow::Result;
use std::sync::Arc;
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

pub struct Scired {
    sess: Arc<Session>,
    ps: HashMap<String, Arc<PreparedStatement>>,
}

impl Scired {
    pub async fn new(session: Session) -> Result<Scired> {
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


        let sm = Scired {
            sess:sess,
            ps: ps_map,
        };
        Ok(sm)
    }

    pub async fn run(&self, listener: TcpListener) {
        let mut join_set = JoinSet::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let h = Handler {
                sess:self.sess.clone(),
                ps:self.ps.clone(),
            };
            join_set.spawn(async move {
                h.handle(socket).await
            });

        }
    }
}

struct Handler {
    sess: Arc<Session>,
    ps: HashMap<String, Arc<PreparedStatement>>,
}

impl Handler {
    async fn handle(&self, socket: TcpStream) {
        use mini_redis::Command::{self, Get,Set};

        let mut conn = Connection::new(socket);

        while let Some(frame) = conn.read_frame().await.unwrap() {
            let resp = match Command::from_frame(frame).unwrap() {
                Set(cmd) => {
                    let val_str = std::str::from_utf8(&cmd.value()).unwrap();
                    self.set(cmd.key(),val_str).await
                }
                Get(cmd) => {
                    self.get(cmd.key().to_string()).await
                }
                cmd=> panic!("unimplemented command {:?}", cmd),
            };
            conn.write_frame(&resp).await.unwrap();
        }
    }

    pub async fn get(&self, key: String) -> Frame {
        let sess = &self.sess;
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

    pub async fn set(&self, key: &str, val: &str) -> Frame {
        let sess = &self.sess;
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