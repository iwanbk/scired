use mini_redis::{client, Result};
#[tokio::main]
async fn main() -> Result<()> {
    let mut client = client::connect("127.0.0.1:6379").await?;
    let res_set = client.set("keyku", "waduh".into()).await?;
    println!("got SET response from the server: {:?}", res_set);

    let result = client.get("keykus").await?;

    println!("got GET response from the server: {:?}", result);
    Ok(())
}
