use std::time::Duration;

use anyhow::Context;
use sqlx::{
    Pool, Postgres,
    postgres::{PgConnectOptions, PgPoolOptions},
};

use crate::config::DbConfig;

/// Default max connections
pub const DEFAULT_MAX_CONNECTIONS: u32 = 32;

/// How long a caller waits for a free pooled connection before erroring. Set
/// explicitly so connection-pool starvation surfaces as a bounded error
/// instead of relying on the driver default.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Connect to the database
#[expect(
    clippy::expect_used,
    reason = "predates this lint; fix when next touched"
)]
pub async fn connect(conf: &DbConfig) -> anyhow::Result<Pool<Postgres>> {
    let mut conn_options: PgConnectOptions = conf.url.as_str().parse().expect("Invalid DB URL");
    conn_options = conn_options
        .username(&conf.username)
        .password(&conf.password);

    // Try to connect to the DB
    let pool = PgPoolOptions::new()
        .max_connections(conf.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_with(conn_options)
        .await
        .context("failed to connect to DB")?;

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    /// Postgres's SSLRequest: the message length, 8, then the request code 80877103.
    const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

    /// The first byte of a TLS record holding a handshake message, which a ClientHello is.
    const TLS_HANDSHAKE_RECORD: u8 = 0x16;

    fn db_config(url: &str) -> DbConfig {
        serde_json::from_value(serde_json::json!({
            "url": url,
            "username": "dipper",
            "password": "unused",
        }))
        .expect("a valid db config")
    }

    #[tokio::test]
    async fn sslmode_require_starts_a_tls_handshake() {
        // A server that agrees to TLS and records what the client sends next. It never completes
        // the handshake, so the connect itself fails; only what reached the server matters.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await?;
            let mut request = [0u8; 8];
            socket.read_exact(&mut request).await?;
            socket.write_all(b"S").await?;
            let mut next = [0u8; 1];
            socket.read_exact(&mut next).await?;
            Ok::<_, std::io::Error>((request, next[0]))
        });

        let conf = db_config(&format!("postgres://{addr}/dipper?sslmode=require"));
        let (_, seen) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(5), connect(&conf)),
            tokio::time::timeout(Duration::from_secs(5), server),
        );
        let (request, next) = seen
            .expect("dipper never asked the server for TLS")
            .unwrap()
            .expect("the server read the SSLRequest and the next byte");
        assert_eq!(request, SSL_REQUEST, "dipper opens with an SSLRequest");
        assert_eq!(
            next, TLS_HANDSHAKE_RECORD,
            "after the server agrees, dipper starts a TLS handshake"
        );
    }
}
