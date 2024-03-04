use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use scylla::{Session, SessionBuilder};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{Sender,Receiver};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let session: Session = SessionBuilder::new().known_node("127.0.0.1:9042".to_string()).build().await?;

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening...");

    let (tx, mut rx) = mpsc::channel(32);

    tokio::spawn(async move {
       scylla_mgr(&mut rx, session).await;
    });


    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let tx2 = tx.clone();
        tokio::spawn(async move {
            //process(socket, db).await;
            process_scylla(socket, tx2).await;
        });

    }
}



async fn scylla_mgr(rx: &mut Receiver<MgrCommand>, sess: Session)  {
    println!("running scylla mgr");

    while let Some(cmd) = rx.recv().await {
        use MgrCommand::*;
        match cmd {
            Get {key, resp} => {
                let mut value = String::from("");
                if let Some(rows) = sess.
                    query(format!("select value from scired.strings where key='{}'", key), &[])
                    .await.unwrap()
                    .rows {
                    for row in rows {
                        let a = row.columns[0].as_ref().unwrap().as_text().unwrap();
                        println!("isi key {} = {}", key, a);
                        value = a.to_string();
                    }
                }
                let _ = resp.send(Ok(value));

                /*let value = sess.
                    query(format!("select value from scired.strings where key='{}'", key), &[])
                    .await.unwrap()
                    .rows.unwrap()
                    .first().unwrap()
                    .columns[0].as_ref().unwrap().
                    as_text().unwrap().to_string();*/
            }
            Set{key, val, resp} => {
                let val_str = std::str::from_utf8(&val).unwrap();
                let sci_res=
                    sess.
                        query("insert into scired.strings (key, value) values (?, ?)",
                               (key, val_str))
                   .await;
                  //.map(|_| ())
                  //.map_err(From::from);
                let status = match sci_res {
                    Err(_) => "",
                    Ok(_) => "OK",
                };
                let _ = resp.send(status.to_string());
            }
        }
    }
}
//type Responder<T> = oneshot::Sender<Result<T>>;
type Responder<T> = oneshot::Sender<T>;
#[derive(Debug)]
enum MgrCommand {
    Get {
        key: String,
        resp: Responder<std::result::Result<String, &'static str>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<String>,
    }
}

async fn process_scylla(socket: TcpStream, mgr_tx: Sender<MgrCommand>) {
    use mini_redis::Command::{self, Get,Set};

    // db hashmap to store the data
    //let mut db = HashMap::new();

    let mut conn = Connection::new(socket);

    while let Some(frame) = conn.read_frame().await.unwrap() {
        //println!("[process_scylla] GOT: {:?}", frame);

        let resp = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                println!("process_scylla set handler: {}", cmd.key());
                let (resp_tx, resp_rx) = oneshot::channel();
                let cmd = MgrCommand::Set {
                    key:cmd.key().to_string(),
                    val:cmd.value().clone(),
                    resp: resp_tx,
                };
                mgr_tx.send(cmd).await.unwrap();

                let resp = resp_rx.await;
                println!("set handler got : {:?}", resp);
                println!("sending SET response to client");
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let cmd = MgrCommand::Get {
                    key:cmd.key().into(),
                    resp:resp_tx,
                };
                mgr_tx.send(cmd).await.unwrap();

                let resp = resp_rx.await.unwrap();
                println!("GET handler GOT: {:?}", resp);

                match resp {
                    Ok(val) => Frame::Bulk(Bytes::from(val)),
                    Err(_) => Frame::Null,
                }
            }

            cmd=> panic!("unimplemented {:?}", cmd),
        };
        conn.write_frame(&resp).await.unwrap();
    }
}





