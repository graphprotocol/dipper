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
    match &conf.password {
        Some(password) => conn_options = conn_options.password(password),
        None => tracing::info!(
            "db.password is not set: connecting without a password of dipper's own, as through \
             the Cloud SQL Auth Proxy with automatic IAM authentication"
        ),
    }

    // Try to connect to the DB
    let pool = PgPoolOptions::new()
        .max_connections(conf.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS))
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect_with(conn_options)
        .await
        .map_err(|err| explain_refusal(err, conf.password.is_none()))
        .context("failed to connect to DB")?;

    Ok(pool)
}

/// Postgres's code for a failed password check (`invalid_password`).
const INVALID_PASSWORD: &str = "28P01";

/// Adds to a refused password check that dipper had no password to give, which the server's own
/// message ("password authentication failed") does not say.
fn explain_refusal(err: sqlx::Error, no_password: bool) -> anyhow::Error {
    let refused_password = matches!(
        &err,
        sqlx::Error::Database(db) if db.code().as_deref() == Some(INVALID_PASSWORD)
    );
    if no_password && refused_password {
        anyhow::Error::new(err).context(
            "the database asked for a password, but db.password is not set; set it, or connect \
             through something that authenticates for dipper, such as the Cloud SQL Auth Proxy",
        )
    } else {
        err.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temporary Postgres that, unlike pgtemp's default, checks passwords on TCP connections.
    fn password_checking_db() -> (pgtemp::PgTempDB, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("dipper-scram-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let hba = dir.join("pg_hba.conf");
        std::fs::write(
            &hba,
            "local all all trust\nhost all all 127.0.0.1/32 scram-sha-256\nhost all all ::1/128 scram-sha-256\n",
        )
        .unwrap();
        let db = pgtemp::PgTempDBBuilder::new()
            .with_password("s3cret")
            .with_config_param("hba_file", hba.to_str().unwrap())
            .start();
        (db, dir)
    }

    fn db_config(temp_db: &pgtemp::PgTempDB, password: Option<&str>) -> DbConfig {
        let mut json = serde_json::json!({
            "url": format!("postgres://127.0.0.1:{}/{}", temp_db.db_port(), temp_db.db_name()),
            "username": temp_db.db_user(),
        });
        if let Some(password) = password {
            json["password"] = password.into();
        }
        serde_json::from_value(json).unwrap()
    }

    #[tokio::test]
    async fn it_connects_without_a_password_where_the_server_asks_for_none() {
        // pgtemp's default server trusts local connections, as a database behind the Cloud SQL
        // Auth Proxy with automatic IAM authentication asks the client for no password either.
        let temp_db = pgtemp::PgTempDB::new();
        let pool = connect(&db_config(&temp_db, None))
            .await
            .expect("connects without a password");
        let one: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(one, 1);
    }

    #[tokio::test]
    async fn it_says_so_when_a_server_wants_the_password_that_is_not_set() {
        let (temp_db, dir) = password_checking_db();

        let without = connect(&db_config(&temp_db, None)).await;
        let with = connect(&db_config(&temp_db, Some("s3cret"))).await;
        drop(temp_db);
        let _ = std::fs::remove_dir_all(dir);

        let err = format!("{:#}", without.expect_err("the server checks passwords"));
        assert!(err.contains("db.password is not set"), "{err}");
        with.expect("the right password gets in");
    }
}
