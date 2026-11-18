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
    conn_options = conn_options.username(&conf.username);
    // Without a password, dipper only gets in where the server asks for none.
    if let Some(password) = &conf.password {
        conn_options = conn_options.password(password);
    }

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
    use super::*;

    #[tokio::test]
    async fn it_connects_without_a_password_where_the_server_asks_for_none() {
        // pgtemp's server trusts local connections, as a database behind the Cloud SQL Auth Proxy
        // with automatic IAM authentication asks the client for no password either.
        let temp_db = pgtemp::PgTempDB::new();
        let conf: DbConfig = serde_json::from_value(serde_json::json!({
            "url": format!("postgres://127.0.0.1:{}/{}", temp_db.db_port(), temp_db.db_name()),
            "username": temp_db.db_user(),
        }))
        .unwrap();

        let pool = connect(&conf).await.expect("connects without a password");
        let one: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(one, 1);
    }
}
