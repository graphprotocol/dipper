mod chain;
mod client;
mod cmd;
mod config;
mod signer;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
pub async fn main() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    // Try to load configuration from .env file
    if let Err(err) = dotenvy::dotenv() {
        if err.not_found() {
            tracing::debug!("No .env file found");
        } else {
            tracing::debug!("Failed to load .env file: {err}");
        }
    } else {
        tracing::debug!("Loaded .env file");
    }

    // Parse and run
    if let Err(err) = cmd::run().await {
        eprintln!("{err}");
        std::process::exit(err.code());
    }
}
