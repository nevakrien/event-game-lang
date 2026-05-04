use event_game_lang::{Engine, serve};
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = env::var("BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
        .parse()?;
    let admin_token = env::var("ADMIN_TOKEN")
        .map_err(|_| "ADMIN_TOKEN must be set to protect /admin/ticks/next")?;

    let engine = Arc::new(Engine::new(Vec::new())?);
    serve(addr, engine, admin_token).await?;
    Ok(())
}
