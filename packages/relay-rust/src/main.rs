use std::net::SocketAddr;

use paseo_relay_rust::{run_server_with_state, AppState, RelayConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RelayConfig::from_env()
        .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.log_filter.clone()));
    tracing_subscriber::fmt().with_env_filter(env_filter).init();

    let bind_addr: SocketAddr = config.bind_addr;
    run_server_with_state(bind_addr, AppState::new(config.timings, config.limits)).await?;
    Ok(())
}
