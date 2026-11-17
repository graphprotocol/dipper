//! A session advisory lock, held on a database connection of its own.

use sqlx::{ConnectOptions as _, Connection as _, PgConnection};

use super::PgRegistry;
use crate::result::Result;

/// A connection outside the pool on which 1 session advisory lock is taken and held. Postgres
/// ties such a lock to its connection and gives it up when the connection closes or is lost,
/// so it can't sit on a pooled connection that other queries come and go on.
pub struct AdvisoryLockConnection {
    conn: PgConnection,
    key: i64,
}

impl PgRegistry {
    /// Opens a connection of its own, to the pool's database, to take the lock `key` on.
    pub async fn advisory_lock_connection(&self, key: i64) -> Result<AdvisoryLockConnection> {
        let conn = self.pool.connect_options().connect().await?;
        Ok(AdvisoryLockConnection { conn, key })
    }
}

impl AdvisoryLockConnection {
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
    pub async fn is_alive(&mut self) -> bool {
        self.conn.ping().await.is_ok()
    }

    /// Closes the connection, which gives the lock up at once.
    pub async fn close(self) -> Result<()> {
        self.conn.close().await?;
        Ok(())
    }
}
