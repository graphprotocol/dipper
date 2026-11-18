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

    // Connect, waiting out a database that is not reachable yet. The pool itself only retries a
    // refused connection, and only for its acquire timeout.
    let pool = crate::startup::retry_within(
        "database connection",
        conf.connect_wait,
        worth_retrying,
        || {
            PgPoolOptions::new()
                .max_connections(conf.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
                .acquire_timeout(ACQUIRE_TIMEOUT)
                .connect_with(conn_options.clone())
        },
    )
    .await
    .context("failed to connect to DB")?;

    Ok(pool)
}

/// Whether a failed connect may succeed if tried again. A server that answered with an error (a
/// wrong password, a missing database) or a connection set up wrongly (its options, its TLS)
/// will fail the same way each time, so those end the wait at once; a server that could not be
/// reached, or dropped the connection, may be back soon.
fn worth_retrying(err: &sqlx::Error) -> bool {
    matches!(
        err,
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::Protocol(_)
    )
}

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

    #[test]
    fn only_failures_that_may_clear_up_are_retried() {
        // A connection set up wrongly fails the same way every time, so it ends the wait at once
        // and dipper exits with the reason instead of retrying for minutes.
        assert!(!worth_retrying(&sqlx::Error::Configuration(
            "bad option".into()
        )));
        assert!(!worth_retrying(&sqlx::Error::Tls(
            "untrusted certificate".into()
        )));
        // A server out of reach, or one that dropped the connection, may be back soon.
        assert!(worth_retrying(&sqlx::Error::PoolTimedOut));
        assert!(worth_retrying(&sqlx::Error::Io(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset
        ))));
    }
}
