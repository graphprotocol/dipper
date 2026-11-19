//! A session advisory lock, held on a database connection of its own.

use std::time::Duration;

use sqlx::{ConnectOptions as _, Connection as _, PgConnection};

use super::PgRegistry;
use crate::result::Result;

/// A connection outside the pool on which 1 session advisory lock is taken and held. Postgres
/// ties such a lock to its connection and gives it up when the connection closes or is lost,
/// so it can't sit on a pooled connection that other queries come and go on.
pub struct AdvisoryLockConnection {
    conn: PgConnection,
    key: i64,
    idle_timeout_refused: Option<String>,
}

impl PgRegistry {
    /// Opens a connection of its own, to the pool's database, to take the lock `key` on. It is
    /// listed in `pg_stat_activity` under the application name `name`, and the database is told
    /// to end it once nothing has been asked on it for `idle_timeout`, which frees the lock.
    pub async fn advisory_lock_connection(
        &self,
        key: i64,
        name: &str,
        idle_timeout: Duration,
    ) -> Result<AdvisoryLockConnection> {
        let options = self.pool.connect_options().as_ref().clone();
        let mut conn = options.application_name(name).connect().await?;
        // Set once connected and not as a startup option, which a database that lacks the
        // setting would refuse the whole connection over. Without a unit the value is in ms.
        let set = sqlx::query("SELECT set_config('idle_session_timeout', $1, false)")
            .bind(idle_timeout.as_millis().to_string())
            .execute(&mut conn)
            .await;
        let idle_timeout_refused = match set {
            Ok(_) => None,
            Err(sqlx::Error::Database(refusal)) => Some(refusal.message().to_owned()),
            Err(error) => return Err(error.into()),
        };
        Ok(AdvisoryLockConnection {
            conn,
            key,
            idle_timeout_refused,
        })
    }
}

impl AdvisoryLockConnection {
    /// What the database said if it would not take the idle timeout: the setting came with
    /// Postgres 14. The lock works all the same, but a holder that vanishes without closing
    /// its socket then keeps it until the database gives up on the connection by itself.
    pub fn idle_timeout_refused(&self) -> Option<&str> {
        self.idle_timeout_refused.as_deref()
    }

    /// Tries to take the lock without waiting; `false` while another connection holds it.
    /// Call it until it says `true` and no more: Postgres counts each time a session takes a lock.
    pub async fn try_lock(&mut self) -> Result<bool> {
        let (taken,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(self.key)
            .fetch_one(&mut self.conn)
            .await?;
        Ok(taken)
    }

    /// Whether the connection still answers. A lock taken on it is held exactly that long.
    /// Asking also counts as use, so it puts off the idle timeout the connection was opened with.
    pub async fn is_alive(&mut self) -> bool {
        self.ping().await.is_ok()
    }

    /// Asks the connection to answer, as [`Self::is_alive`] does, and says why it did not.
    pub async fn ping(&mut self) -> Result<()> {
        self.conn.ping().await?;
        Ok(())
    }

    /// Closes the connection, which gives the lock up at once.
    pub async fn close(self) -> Result<()> {
        self.conn.close().await?;
        Ok(())
    }
}
