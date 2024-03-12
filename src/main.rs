use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use scylla::{Session, SessionBuilder};
use anyhow::Result;
use std::sync::Arc;
use scylla::frame::response::result;
use scylla::prepared_statement::PreparedStatement;

#[tokio::main]
async fn main() -> Result<()> {
    let session: Session = SessionBuilder::new().known_node("127.0.0.1:9042".to_string()).build().await?;
    let session = Arc::new(session);

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    let prepared_get = Arc::new(
        session
            .prepare("select value from scired.strings where key=?")
            .await?,
    );

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let sess = session.clone();
        let prep_get = prepared_get.clone();
        tokio::spawn(async move {
            handle_scylla(socket, sess, prep_get).await;
        });

    }
}

async fn handle_scylla(socket: TcpStream, sess: Arc<Session>, prep_get: Arc<PreparedStatement>) {
    use mini_redis::Command::{self, Get,Set};

    let mut conn = Connection::new(socket);

    while let Some(frame) = conn.read_frame().await.unwrap() {
        let resp = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let val_str = std::str::from_utf8(&cmd.value()).unwrap();
                let sci_res= sess
                    .query("insert into scired.strings (key, value) values (?, ?)",
                              (cmd.key().to_string(), val_str)) // TODO: make it prepared statements
                    .await;
                let status = match sci_res {
                    Err(e) => Frame::Error(e.to_string()), // TODO: make sure it follows redis protocol
                    Ok(_) => Frame::Simple("OK".to_string()),
                };
                status
            }
            Get(cmd) => {
              match sess.execute(&prep_get, (cmd.key().to_string(),)).await { // TODO: change to map?
                    Ok(res) => {
                        match res.rows {
                            Some(rows) => match get_single_val(rows) {
                                Some(val) => Frame::Bulk(Bytes::from(val)),
                                None => Frame::Null,
                            }
                            None => Frame::Null
                        }
                    },
                    Err(e) => Frame::Error(e.to_string()),
                }
            }

            cmd=> panic!("unimplemented {:?}", cmd),
        };
        conn.write_frame(&resp).await.unwrap();
    }
}

// get a single value from a rows
fn get_single_val(rows: Vec<result::Row>) -> Option<String> {
    rows.first().map(|row|row.columns[0].as_ref()).flatten()
        .map(|r|r.as_text()).flatten()
        .map(|f|f.to_string())
}




