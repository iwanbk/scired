use std::collections::HashMap;
use mini_redis::{Frame};
use bytes::Bytes;
use scylla::{Session};
use anyhow::Result;
use std::sync::Arc;
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;

pub struct ScyllaMgr {
    sess: Arc<Session>,
    ps: HashMap<String, Arc<PreparedStatement>>,
}

impl ScyllaMgr {
    pub async fn new(session: Session) -> Result<ScyllaMgr> {
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

    pub async fn get(&self, key: String) -> Frame {
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

    pub async fn set(&self, key: &str, val: &str) -> Frame {
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