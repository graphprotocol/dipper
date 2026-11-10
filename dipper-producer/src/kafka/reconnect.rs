//! A broker client that reconnects from the bootstrap addresses after repeated failures. rskafka
//! only goes back to the bootstrap (seed) addresses when it knows no brokers, so a cluster that
//! replaces every broker under new names leaves a long-lived client dialling names that are gone.

use std::{
    fmt::Display,
    future::Future,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::sync::Notify;

/// Consecutive failures to reach the brokers after which the client is dropped and rebuilt.
pub const RECONNECT_AFTER_FAILURES: u32 = 3;

/// The longest wait between attempts to connect.
const MAX_CONNECT_BACKOFF_SECS: u64 = 30;

/// How long 1 attempt to connect may take. rskafka retries an unreachable bootstrap address with no
/// deadline, so an attempt without a limit could hang with no retry and no warning.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Failed connect attempts between repeated warnings, after the first.
const WARN_EVERY: u32 = 5;

/// A client connected in the background and rebuilt after [`RECONNECT_AFTER_FAILURES`] consecutive
/// failures to reach the brokers. Callers report each attempt with `report_success` or
/// `report_unreachable`; `current` returns `None` while the client is being (re)built.
pub struct Reconnecting<T> {
    shared: Arc<Shared<T>>,
}

struct Shared<T> {
    name: &'static str,
    client: Mutex<Option<Arc<T>>>,
    failures: AtomicU32,
    rebuild: Notify,
}

impl<T> Clone for Reconnecting<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Shared<T> {
    /// The guarded `Option` is always valid, so a panic elsewhere while holding the lock must not
    /// take the client down with it.
    fn lock(&self) -> MutexGuard<'_, Option<Arc<T>>> {
        self.client.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl<T: Send + Sync + 'static> Reconnecting<T> {
    /// Starts connecting in the background with `connect`, retrying with capped backoff until it
    /// succeeds, and connects again each time the client is dropped. `name` labels the log lines.
    pub fn spawn<F, Fut, E>(name: &'static str, connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        E: Display + Send,
    {
        let shared = Arc::new(Shared {
            name,
            client: Mutex::new(None),
            failures: AtomicU32::new(0),
            rebuild: Notify::new(),
        });
        let task = shared.clone();
        tokio::spawn(async move {
            loop {
                let client = connect_with_retry(name, &connect).await;
                task.failures.store(0, Ordering::Relaxed);
                *task.lock() = Some(Arc::new(client));
                tracing::info!(broker = name, "connected to the brokers");
                // `notify_one` stores a permit, so a rebuild asked for before this wait starts is
                // not missed.
                task.rebuild.notified().await;
            }
        });
        Self { shared }
    }

    /// The client, or `None` while it is being (re)built.
    pub fn current(&self) -> Option<Arc<T>> {
        self.shared.lock().clone()
    }

    /// Records that a request through the current client reached the brokers.
    pub fn report_success(&self) {
        self.shared.failures.store(0, Ordering::Relaxed);
    }

    /// Records that a request through `client` couldn't reach the brokers; after
    /// [`RECONNECT_AFTER_FAILURES`] in a row, drops it so the background task connects again.
    /// Ignores a client that was already replaced. Returns whether this call dropped the client.
    pub fn report_unreachable(&self, client: &Arc<T>) -> bool {
        let mut current = self.shared.lock();
        if !current.as_ref().is_some_and(|c| Arc::ptr_eq(c, client)) {
            return false;
        }
        let failures = self.shared.failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures < RECONNECT_AFTER_FAILURES {
            return false;
        }
        *current = None;
        drop(current);
        tracing::warn!(
            broker = self.shared.name,
            failures,
            "brokers unreachable; connecting again from the bootstrap addresses"
        );
        self.shared.rebuild.notify_one();
        true
    }
}

/// Calls `connect` until it succeeds, waiting 2, 4, 8, 16 and then 30 seconds between attempts.
/// Warns on the first failure and every [`WARN_EVERY`] after it, so an operator without
/// dashboards still sees that the client is down.
async fn connect_with_retry<T, F, Fut, E>(name: &'static str, connect: &F) -> T
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    let mut attempt: u32 = 0;
    loop {
        let error = match tokio::time::timeout(CONNECT_TIMEOUT, connect()).await {
            Ok(Ok(client)) => return client,
            Ok(Err(err)) => err.to_string(),
            Err(_) => format!("timed out after {}s", CONNECT_TIMEOUT.as_secs()),
        };
        attempt = attempt.saturating_add(1);
        if attempt == 1 || attempt.is_multiple_of(WARN_EVERY) {
            tracing::warn!(
                broker = name,
                attempt,
                error,
                "can't connect to the brokers; retrying"
            );
        }
        let backoff = MAX_CONNECT_BACKOFF_SECS.min(2u64.saturating_pow(attempt.min(5)));
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;

    use super::*;

    /// A stand-in client, numbered by the connect attempt that made it.
    #[derive(Debug, PartialEq)]
    struct Client(u32);

    /// A connector whose first `failing` attempts fail, counting every attempt.
    fn connector(
        failing: u32,
    ) -> (
        Arc<AtomicU32>,
        impl Fn() -> std::future::Ready<Result<Client, String>> + Send + Sync + 'static,
    ) {
        let attempts = Arc::new(AtomicU32::new(0));
        let counted = attempts.clone();
        let connect = move || {
            let n = counted.fetch_add(1, Ordering::Relaxed) + 1;
            std::future::ready(if n <= failing {
                Err(format!("attempt {n} failed"))
            } else {
                Ok(Client(n))
            })
        };
        (attempts, connect)
    }

    /// Waits (in paused test time) until a client other than `not` is handed out.
    async fn next_client(conn: &Reconnecting<Client>, not: Option<&Arc<Client>>) -> Arc<Client> {
        for _ in 0..1_000 {
            if let Some(c) = conn.current()
                && not.is_none_or(|n| !Arc::ptr_eq(n, &c))
            {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("no new client was handed out");
    }

    #[tokio::test(start_paused = true)]
    async fn keeps_trying_until_the_first_connect_succeeds() {
        let (attempts, connect) = connector(2);
        let conn = Reconnecting::spawn("test", connect);

        let client = next_client(&conn, None).await;

        assert_eq!(*client, Client(3));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_on_a_connect_attempt_that_never_finishes() {
        let attempts = Arc::new(AtomicU32::new(0));
        let counted = attempts.clone();
        let conn = Reconnecting::spawn("test", move || {
            let n = counted.fetch_add(1, Ordering::Relaxed) + 1;
            async move {
                if n == 1 {
                    std::future::pending::<()>().await;
                }
                Ok::<_, String>(Client(n))
            }
        });

        let client = next_client(&conn, None).await;

        assert_eq!(*client, Client(2));
    }

    #[tokio::test(start_paused = true)]
    async fn connects_again_after_repeated_failures_to_reach_the_brokers() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", connect);
        let first = next_client(&conn, None).await;

        for _ in 1..RECONNECT_AFTER_FAILURES {
            assert!(!conn.report_unreachable(&first));
        }
        assert_eq!(
            conn.current().as_ref(),
            Some(&first),
            "kept until the last allowed failure"
        );
        assert!(conn.report_unreachable(&first));
        assert!(
            conn.current().is_none(),
            "dropped once the failures reach the limit"
        );

        let second = next_client(&conn, Some(&first)).await;
        assert_eq!(*second, Client(2));
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_success_starts_the_failure_count_again() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", connect);
        let client = next_client(&conn, None).await;

        for _ in 1..RECONNECT_AFTER_FAILURES {
            conn.report_unreachable(&client);
        }
        conn.report_success();
        for _ in 1..RECONNECT_AFTER_FAILURES {
            assert!(!conn.report_unreachable(&client));
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(conn.current().as_ref(), Some(&client));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_on_a_replaced_client_are_ignored() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", connect);
        let first = next_client(&conn, None).await;
        for _ in 0..RECONNECT_AFTER_FAILURES {
            conn.report_unreachable(&first);
        }
        let second = next_client(&conn, Some(&first)).await;

        // A send still in flight on the first client reports its failure after the second took over.
        for _ in 0..RECONNECT_AFTER_FAILURES {
            assert!(!conn.report_unreachable(&first));
        }

        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(conn.current().as_ref(), Some(&second));
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }
}
