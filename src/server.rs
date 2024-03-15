use std::collections::HashMap;
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use scylla::{Session};
use anyhow::Result;
use std::sync::Arc;
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};
use tokio::sync::{broadcast, mpsc};
use crate::shutdown::Shutdown;
use tracing::{error, instrument};

pub struct SciredConfig {
    pub ops_consistency: OpsConsistency,
}

pub struct OpsConsistency {
    pub get: Consistency,
    pub set: Consistency,
}

pub struct Scired {
    sess: Arc<Session>,
    ps: HashMap<String, Arc<PreparedStatement>>,
    pub notify_shutdown: broadcast::Sender<()>,
    pub shutdown_complete_tx: mpsc::Sender<()>,
}


impl Scired {
    pub async fn new(cfg: SciredConfig, session: Session,
                     notify_shutdown: broadcast::Sender<()>,
                        shutdown_complete_tx: mpsc::Sender<()>) -> Result<Scired> {
        let sess = Arc::new(session);
        let mut ps_map = HashMap::new();

        // prepare the get
        let mut pg = sess
            .prepare("select value from scired.strings where key=?")
            .await?;
        pg.set_consistency(cfg.ops_consistency.get);
        ps_map.insert("get".to_string(), Arc::new(pg));

        // prepare the set
        let mut ps = sess
            .prepare("insert into scired.strings (key, value) values (?, ?)")
            .await?;
        ps.set_consistency(cfg.ops_consistency.set);
        ps_map.insert("set".to_string(), Arc::new(ps));


        let sm = Scired {
            sess:sess,
            ps: ps_map,
            notify_shutdown:notify_shutdown,
            shutdown_complete_tx:shutdown_complete_tx,
        };
        Ok(sm)
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
        loop {
            let socket = self.accept(&listener).await?;
            let mut h = Handler {
                sess:self.sess.clone(),
                ps:self.ps.clone(),
                shutdown:Shutdown::new(self.notify_shutdown.subscribe()),
                // Notifies the receiver half once all clones are
                // dropped.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                //h.handle(socket).await
                if let Err(err) = h.handle(socket).await {
                    error!(cause = ?err, "connection error");
                }
            });

        }
    }

    async fn accept(&self, listener: &TcpListener) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

#[derive(Debug)]
struct Handler {
    sess: Arc<Session>,
    ps: HashMap<String, Arc<PreparedStatement>>,
    shutdown: Shutdown,
    /// Not used directly. Instead, when `Handler` is dropped...?
    /// TODO: find out how
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    #[instrument(skip(self))]
    async fn handle(&mut self, socket: TcpStream) -> Result<()>{
        use mini_redis::Command::{self, Get,Set};
        let mut conn = Connection::new(socket);
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            let maybe_frame = tokio::select! {
                res = conn.read_frame() => res,
                _ = self.shutdown.recv() => {
                    // If a shutdown signal is received, return from `run`.
                    // This will result in the task terminating.
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Ok(ok_frame) => match ok_frame {
                    Some(frame) => frame,
                    None => return Ok(()),
                }
                Err(_) => return Ok(()),

            };

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
        Ok(())
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