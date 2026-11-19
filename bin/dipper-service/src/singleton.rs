//! Keeps dipper to 1 running process per database.
//!
//! Dipper keeps its signer's nonce and the locks that keep 2 jobs from acting on the same
//! agreement in memory, so 2 processes on 1 database could send transactions on the same nonce,
//! or offer or cancel the same agreement twice. A deploy's `Recreate` strategy keeps a rollout
//! from overlapping them, but not an eviction, a node drain or a deleted pod: Kubernetes starts
//! the replacement while the old pod is still shutting down. So before it does anything, dipper
//! takes a database lock and holds it for as long as it runs; a 2nd dipper waits for it.
//!
//! The lock is a Postgres session advisory lock, held on a connection of its own. Postgres gives
//! it up when that connection ends: when dipper closes it last thing in its shutdown, when the
//! process dies and its socket closes, or, should the holder vanish without its socket closing,
//! once the connection has been idle for [`IDLE_TIMEOUT`]. A holder that cannot reach its
//! connection for longer than [`KEEPALIVE`] and [`PING_TIMEOUT`] together exits at once, well
//! before the database can give the lock to another.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{sync::Mutex, task::JoinHandle};

use crate::lock::LockConnection;

/// The name the lock's key is derived from.
pub const LOCK_NAME: &str = "dipper/singleton";

/// How long the database waits on a silent lock connection before ending it, which frees the lock.
pub const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the holder checks that its lock connection still answers.
pub const KEEPALIVE: Duration = Duration::from_secs(5);

/// How long 1 check may take before the holder counts the lock as lost.
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// How often a waiting dipper asks for the lock again.
const RETRY: Duration = Duration::from_secs(2);

/// How often a waiting dipper says it is still waiting.
const REPORT_EVERY: Duration = Duration::from_secs(30);

// A holder that cannot reach its connection must have given up before the database can free the
// lock for another, or the 2 could act at once.
const _: () = assert!(KEEPALIVE.as_secs() + PING_TIMEOUT.as_secs() < IDLE_TIMEOUT.as_secs());

/// The key of the singleton lock.
pub fn key() -> i64 {
    crate::lock::key(LOCK_NAME)
}

/// The held lock. Give it up with [`Held::release`] once dipper has stopped acting; dropping it
/// keeps it until the process exits, which frees it as surely.
pub struct Held {
    connection: Arc<Mutex<Option<Box<dyn LockConnection>>>>,
    keepalive: JoinHandle<()>,
}

impl Held {
    /// Stops checking the lock and closes its connection, which frees it at once.
    pub async fn release(self) {
        self.keepalive.abort();
        let _ = self.keepalive.await;
        if let Some(connection) = self.connection.lock().await.take() {
            connection.close().await;
        }
        metrics::gauge!(crate::telemetry::SINGLETON_LOCK_HELD).set(0.0);
        tracing::info!("gave up the singleton lock");
    }
}

/// Takes the singleton lock on `connection`, waiting for as long as another dipper holds it, and
/// keeps checking it from then on. `on_lost` runs once if the lock may have been lost: it must
/// stop this process acting at once, as another may take the lock soon after.
pub async fn hold(
    mut connection: Box<dyn LockConnection>,
    on_lost: impl FnOnce() + Send + 'static,
) -> anyhow::Result<Held> {
    if let Some(refused) = connection.idle_timeout_refused() {
        tracing::warn!(
            refused,
            "the database would not end the singleton lock's connection once idle (it needs \
             Postgres 14 or later); a holder that vanishes without closing its socket keeps the \
             lock until the database notices"
        );
    }
    let waited = wait_for_lock(connection.as_mut()).await?;
    tracing::info!(waited_secs = waited.as_secs(), "holding the singleton lock");
    metrics::gauge!(crate::telemetry::SINGLETON_LOCK_HELD).set(1.0);

    let connection = Arc::new(Mutex::new(Some(connection)));
    let keepalive = tokio::spawn(keep_checking(Arc::clone(&connection), on_lost));
    Ok(Held {
        connection,
        keepalive,
    })
}

/// Asks for the lock until it is given, saying every [`REPORT_EVERY`] that it is still waiting.
/// Returns how long it waited.
async fn wait_for_lock(connection: &mut dyn LockConnection) -> anyhow::Result<Duration> {
    let started = Instant::now();
    let mut reported = Instant::now();
    while !connection.try_lock().await? {
        if reported.elapsed() >= REPORT_EVERY {
            tracing::warn!(
                event = "singleton_lock_waiting",
                waited_secs = started.elapsed().as_secs(),
                "another dipper holds the singleton lock on this database; waiting for it to stop"
            );
            reported = Instant::now();
        }
        tokio::time::sleep(RETRY).await;
    }
    Ok(started.elapsed())
}

/// Checks the lock's connection every [`KEEPALIVE`] until it fails to answer within
/// [`PING_TIMEOUT`], then runs `on_lost`. Ends quietly once [`Held::release`] has taken it.
async fn keep_checking(
    connection: Arc<Mutex<Option<Box<dyn LockConnection>>>>,
    on_lost: impl FnOnce() + Send + 'static,
) {
    loop {
        tokio::time::sleep(KEEPALIVE).await;
        let mut guard = connection.lock().await;
        let Some(connection) = guard.as_mut() else {
            return;
        };
        let alive = tokio::time::timeout(PING_TIMEOUT, connection.is_alive())
            .await
            .unwrap_or(false);
        if !alive {
            metrics::gauge!(crate::telemetry::SINGLETON_LOCK_HELD).set(0.0);
            tracing::error!(
                event = "singleton_lock_lost",
                "the singleton lock's connection did not answer; stopping at once, as another \
                 dipper may take the lock"
            );
            on_lost();
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use async_trait::async_trait;

    use super::*;

    /// A lock connection that answers `try_lock` and `is_alive` from scripts.
    struct Scripted {
        locks: VecDeque<bool>,
        alive: VecDeque<bool>,
        closed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl LockConnection for Scripted {
        fn idle_timeout_refused(&self) -> Option<&str> {
            None
        }

        async fn try_lock(&mut self) -> Result<bool, crate::registry::Error> {
            Ok(self.locks.pop_front().unwrap_or(true))
        }

        async fn is_alive(&mut self) -> bool {
            self.alive.pop_front().unwrap_or(true)
        }

        async fn close(self: Box<Self>) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    fn scripted(locks: &[bool], alive: &[bool]) -> (Box<dyn LockConnection>, Arc<AtomicBool>) {
        let closed = Arc::new(AtomicBool::new(false));
        let connection = Scripted {
            locks: locks.iter().copied().collect(),
            alive: alive.iter().copied().collect(),
            closed: Arc::clone(&closed),
        };
        (Box::new(connection), closed)
    }

    #[test]
    fn the_key_is_fixed_and_apart_from_the_consumers() {
        // Every build must ask for the same key, or 2 versions side by side would not exclude
        // each other.
        assert_eq!(key(), crate::lock::key("dipper/singleton"));
        assert!(key() < 0, "negative, apart from sqlx's migration lock");
        assert_ne!(
            key(),
            crate::lock::key("dipper/indexing-request-consumer/studio.subgraph.indexing.requests")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_2nd_dipper_waits_for_the_lock() {
        let (connection, _) = scripted(&[false, false, false, true], &[]);
        let started = tokio::time::Instant::now();
        let held = hold(connection, || {}).await.unwrap();
        assert_eq!(started.elapsed(), RETRY * 3, "3 refusals, then the lock");
        held.release().await;
    }

    #[tokio::test(start_paused = true)]
    async fn losing_the_lock_stops_the_holder() {
        let (connection, _) = scripted(&[true], &[true, true, false]);
        let (lost_tx, lost_rx) = tokio::sync::oneshot::channel();
        let _held = hold(connection, move || {
            let _ = lost_tx.send(tokio::time::Instant::now());
        })
        .await
        .unwrap();
        let started = tokio::time::Instant::now();
        let lost_at = lost_rx.await.expect("the holder is told the lock is gone");
        assert_eq!(lost_at - started, KEEPALIVE * 3, "the 3rd check fails");
    }

    #[tokio::test(start_paused = true)]
    async fn a_check_that_hangs_counts_as_lost() {
        struct Hanging;
        #[async_trait]
        impl LockConnection for Hanging {
            fn idle_timeout_refused(&self) -> Option<&str> {
                None
            }
            async fn try_lock(&mut self) -> Result<bool, crate::registry::Error> {
                Ok(true)
            }
            async fn is_alive(&mut self) -> bool {
                std::future::pending().await
            }
            async fn close(self: Box<Self>) {}
        }
        let (lost_tx, lost_rx) = tokio::sync::oneshot::channel();
        let _held = hold(Box::new(Hanging), move || {
            let _ = lost_tx.send(tokio::time::Instant::now());
        })
        .await
        .unwrap();
        let started = tokio::time::Instant::now();
        let lost_at = lost_rx.await.unwrap();
        assert_eq!(lost_at - started, KEEPALIVE + PING_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn releasing_closes_the_connection_and_stops_the_checks() {
        let (connection, closed) = scripted(&[true], &[]);
        let lost = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&lost);
        let held = hold(connection, move || flag.store(true, Ordering::SeqCst))
            .await
            .unwrap();
        held.release().await;
        assert!(
            closed.load(Ordering::SeqCst),
            "the connection is closed, freeing the lock"
        );
        tokio::time::sleep(KEEPALIVE * 10).await;
        assert!(
            !lost.load(Ordering::SeqCst),
            "no check runs after the release"
        );
    }

    /// 2 dippers on a real database: the 2nd gets the lock only once the 1st gives it up.
    #[tokio::test]
    async fn it_the_lock_goes_to_the_2nd_dipper_once_the_1st_lets_go() {
        let temp_db = pgtemp::PgTempDB::new();
        let pool = sqlx::PgPool::connect(&temp_db.connection_uri())
            .await
            .unwrap();
        let registry = dipper_pgregistry::PgRegistry::new(pool);
        let open = || async {
            let connection = registry
                .advisory_lock_connection(key(), "dipper-singleton-test", IDLE_TIMEOUT)
                .await
                .unwrap();
            Box::new(connection) as Box<dyn LockConnection>
        };
        let first = hold(open().await, || panic!("the 1st holder keeps its lock"))
            .await
            .unwrap();
        let second = tokio::spawn(hold(open().await, || {}));

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            !second.is_finished(),
            "the 2nd waits while the 1st holds the lock"
        );

        first.release().await;
        let second = tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .expect("the 2nd gets the lock soon after the 1st lets go")
            .unwrap()
            .unwrap();
        second.release().await;
    }

    /// The lock's connection ended by the database (as a failover or a terminated backend does)
    /// tells the holder at its next check.
    #[tokio::test]
    async fn it_a_connection_ended_by_the_database_loses_the_lock() {
        let temp_db = pgtemp::PgTempDB::new();
        let pool = sqlx::PgPool::connect(&temp_db.connection_uri())
            .await
            .unwrap();
        let registry = dipper_pgregistry::PgRegistry::new(pool.clone());
        let connection = registry
            .advisory_lock_connection(key(), "dipper-singleton-test", IDLE_TIMEOUT)
            .await
            .unwrap();
        let (lost_tx, lost_rx) = tokio::sync::oneshot::channel();
        let _held = hold(Box::new(connection), move || {
            let _ = lost_tx.send(());
        })
        .await
        .unwrap();

        sqlx::query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE application_name = 'dipper-singleton-test'",
        )
        .execute(&pool)
        .await
        .unwrap();

        tokio::time::timeout(KEEPALIVE + PING_TIMEOUT + Duration::from_secs(5), lost_rx)
            .await
            .expect("the holder notices within a check")
            .unwrap();
    }
}
