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

    // Parse and run. Each command reads its .env file when it loads its configuration, once it
    // knows whether `--env-file` named one.
    if let Err(err) = cmd::run().await {
        eprintln!("{err}");
        std::process::exit(err.code());
    }
}
