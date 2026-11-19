use std::time::Duration;

use anyhow::Context;
use sqlx::{
    Connection as _, PgConnection, Pool, Postgres,
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

    // Wait for the database with 1 connection at a time, each cut off at the time left of the
    // wait, then open the pool. Asking the pool instead would block every attempt for its whole
    // acquire timeout and report only that it timed out, not why.
    let probe = crate::startup::retry_within(
        "database connection",
        conf.connect_wait,
        ConnectFailure::worth_retrying,
        |remaining| {
            let options = conn_options.clone();
            async move {
                let limit = remaining.min(CONNECT_ATTEMPT_TIMEOUT);
                match tokio::time::timeout(limit, PgConnection::connect_with(&options)).await {
                    Ok(Ok(conn)) => {
                        let _ = conn.close().await;
                        Ok(())
                    }
                    Ok(Err(err)) => Err(ConnectFailure::Refused(err)),
                    Err(_) => Err(ConnectFailure::TimedOut(limit)),
                }
            }
        },
    )
    .await;
    if let Err(gave_up) = probe {
        anyhow::bail!(
            "failed to connect to DB after {} tries over {}s: {}",
            gave_up.tries,
            gave_up.elapsed.as_secs(),
            gave_up.error
        );
    }

    let pool = PgPoolOptions::new()
        .max_connections(conf.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_with(conn_options)
        .await
        .context("failed to connect to DB")?;

    Ok(pool)
}

/// The longest 1 connection attempt may take while waiting for the database at startup.
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// Why 1 attempt to reach the database at startup failed.
#[derive(Debug, thiserror::Error)]
enum ConnectFailure {
    #[error("{0}")]
    Refused(sqlx::Error),
    #[error("no answer within {}s", .0.as_secs())]
    TimedOut(Duration),
}

impl ConnectFailure {
    /// Whether the attempt may succeed if made again. A database that cannot be reached, drops
    /// the connection, does not answer, is still starting up or has no connection to spare may
    /// be back soon. Anything else fails the same way each time, and ends the wait at once: a
    /// refused password or a missing database, a certificate the TLS check rejects (rustls
    /// reports those as invalid data), something other than Postgres answering (a protocol
    /// error), or the connection set up wrongly.
    fn worth_retrying(&self) -> bool {
        match self {
            Self::TimedOut(_) => true,
            Self::Refused(sqlx::Error::Io(err)) => err.kind() != std::io::ErrorKind::InvalidData,
            Self::Refused(sqlx::Error::Database(err)) => err.code().is_some_and(|code| {
                code == CANNOT_CONNECT_NOW || code == TOO_MANY_CONNECTIONS || code.starts_with("08")
            }),
            Self::Refused(_) => false,
        }
    }
}

/// Postgres's code for a server still starting up or shutting down (`cannot_connect_now`).
const CANNOT_CONNECT_NOW: &str = "57P03";

/// Postgres's code for a server with no connection slot to spare (`too_many_connections`).
const TOO_MANY_CONNECTIONS: &str = "53300";

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    use tokio::net::TcpListener;

    use super::*;

    fn db_config(url: &str, connect_wait_secs: u64) -> DbConfig {
        serde_json::from_value(serde_json::json!({
            "url": url,
            "username": "dipper",
            "password": "unused",
            "connect_wait": connect_wait_secs,
        }))
        .expect("a valid db config")
    }

    #[tokio::test]
    async fn connect_retries_a_server_that_drops_the_connection() {
        // A server that accepts and at once drops each connection, as one restarting or failing
        // over can. The pool gives up on that at the first try; dipper must keep trying.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicU32::new(0));
        let server = {
            let accepted = Arc::clone(&accepted);
            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::Relaxed);
                    drop(socket);
                }
            })
        };

        let result = connect(&db_config(&format!("postgres://{addr}/dipper"), 2)).await;
        server.abort();

        assert!(result.is_err(), "the server never lets dipper in");
        assert!(
            accepted.load(Ordering::Relaxed) >= 2,
            "dipper tried {} time(s) within its 2s wait",
            accepted.load(Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn connect_gives_up_on_a_silent_server_within_its_wait() {
        // A server that accepts and then says nothing. Each attempt is cut off at the time left,
        // so the wait ends on time instead of after an attempt's own timeout.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });

        let started = std::time::Instant::now();
        let result = connect(&db_config(&format!("postgres://{addr}/dipper"), 2)).await;
        server.abort();

        let err = format!("{:#}", result.expect_err("the server never answers"));
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "took {:?}",
            started.elapsed()
        );
        assert!(err.contains("no answer within"), "{err}");
    }

    fn io(kind: std::io::ErrorKind) -> ConnectFailure {
        ConnectFailure::Refused(sqlx::Error::Io(std::io::Error::from(kind)))
    }

    #[test]
    fn only_failures_that_may_clear_up_are_retried() {
        use std::io::ErrorKind;
        // A database out of reach, dropping connections or not answering may be back soon.
        assert!(io(ErrorKind::ConnectionRefused).worth_retrying());
        assert!(io(ErrorKind::ConnectionReset).worth_retrying());
        assert!(io(ErrorKind::UnexpectedEof).worth_retrying());
        assert!(ConnectFailure::TimedOut(Duration::from_secs(1)).worth_retrying());
        // rustls reports a rejected certificate as invalid data; it will be rejected every time.
        assert!(!io(ErrorKind::InvalidData).worth_retrying());
        // Something other than Postgres answering, or the connection set up wrongly.
        assert!(
            !ConnectFailure::Refused(sqlx::Error::Protocol("unexpected response".into()))
                .worth_retrying()
        );
        assert!(
            !ConnectFailure::Refused(sqlx::Error::Configuration("bad option".into()))
                .worth_retrying()
        );
    }
}
