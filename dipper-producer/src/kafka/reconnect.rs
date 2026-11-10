//! A broker client that reconnects from the bootstrap addresses once sends keep failing. rskafka
//! only goes back to the bootstrap (seed) addresses when it knows no brokers, so a cluster that
//! replaces every broker under new names leaves a long-lived client dialling names that are gone.

use std::{
    fmt::Display,
    future::Future,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::{sync::Notify, time::Instant};

/// How long sends must keep failing to reach the brokers, with none getting through, before the
/// client is dropped and rebuilt. Measured in time rather than failures, so sends that time out
/// together in 1 brief stall don't count as an outage.
pub const RECONNECT_AFTER: Duration = Duration::from_secs(60);

/// The longest wait between attempts to connect.
const MAX_CONNECT_BACKOFF_SECS: u64 = 30;

/// How long 1 attempt to connect may take. rskafka retries an unreachable bootstrap address with no
/// deadline, so an attempt without a limit could hang with no retry and no warning.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Failed connect attempts between repeated warnings, after the first.
const WARN_EVERY: u32 = 5;

/// A client connected in the background and rebuilt once sends have failed to reach the brokers for
/// [`RECONNECT_AFTER`]. Callers report each attempt with `report_success` or `report_unreachable`;
/// `current` returns `None` while the client is being (re)built.
pub struct Reconnecting<T> {
    shared: Arc<Shared<T>>,
}

struct Shared<T> {
    name: &'static str,
    state: Mutex<State<T>>,
    signals: Arc<Signals>,
}

/// Wakes the background task, to connect again or to stop once every handle is gone, and wakes
/// callers waiting for a client once one is connected.
#[derive(Default)]
struct Signals {
    rebuild: Notify,
    closed: Notify,
    connected: Notify,
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        self.signals.closed.notify_one();
    }
}

/// The client and, while sends through it are failing, when the first of those failures was seen.
struct State<T> {
    client: Option<Arc<T>>,
    failing_since: Option<Instant>,
}

impl<T> Clone for Reconnecting<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Shared<T> {
    /// The guarded state is always valid, so a panic elsewhere while holding the lock must not take
    /// the client down with it.
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
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
        Self::start(name, None, connect)
    }

    /// Like [`spawn`](Self::spawn), starting from a client the caller already connected, so the
    /// caller decides what a failed first connect means; `connect` is used only for rebuilds.
    pub fn with_client<F, Fut, E>(name: &'static str, client: T, connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        E: Display + Send,
    {
        Self::start(name, Some(client), connect)
    }

    fn start<F, Fut, E>(name: &'static str, client: Option<T>, connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        E: Display + Send,
    {
        let mut connected = client.is_some();
        let signals = Arc::new(Signals::default());
        let shared = Arc::new(Shared {
            name,
            state: Mutex::new(State {
                client: client.map(Arc::new),
                failing_since: None,
            }),
            signals: signals.clone(),
        });
        let task = Arc::downgrade(&shared);
        tokio::spawn(async move {
            loop {
                if !connected {
                    let client = tokio::select! {
                        client = connect_with_retry(name, &connect) => client,
                        _ = signals.closed.notified() => return,
                    };
                    let Some(shared) = task.upgrade() else {
                        return;
                    };
                    *shared.lock() = State {
                        client: Some(Arc::new(client)),
                        failing_since: None,
                    };
                    drop(shared);
                    signals.connected.notify_waiters();
                    tracing::info!(broker = name, "connected to the brokers");
                }
                // `notify_one` stores a permit, so a rebuild or close asked for before this wait
                // starts is not missed.
                tokio::select! {
                    _ = signals.rebuild.notified() => connected = false,
                    _ = signals.closed.notified() => return,
                }
            }
        });
        Self { shared }
    }

    /// Waits for a client, returning at once when one is connected.
    pub async fn connected(&self) -> Arc<T> {
        loop {
            // Registered before the check, so a client swapped in between the two still wakes it.
            let notified = self.shared.signals.connected.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(client) = self.current() {
                return client;
            }
            notified.await;
        }
    }

    /// The client, or `None` while it is being (re)built.
    pub fn current(&self) -> Option<Arc<T>> {
        self.shared.lock().client.clone()
    }

    /// Records that a request through the current client reached the brokers.
    pub fn report_success(&self) {
        self.shared.lock().failing_since = None;
    }

    /// Records that a request through `client` couldn't reach the brokers; once sends have failed for
    /// [`RECONNECT_AFTER`] with none getting through, drops it so the background task connects again.
    /// Ignores a client that was already replaced. Returns whether this call dropped the client.
    pub fn report_unreachable(&self, client: &Arc<T>) -> bool {
        let mut state = self.shared.lock();
        if !state
            .client
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, client))
        {
            return false;
        }
        let now = Instant::now();
        let since = *state.failing_since.get_or_insert(now);
        if now - since < RECONNECT_AFTER {
            return false;
        }
        *state = State {
            client: None,
            failing_since: None,
        };
        drop(state);
        tracing::warn!(
            broker = self.shared.name,
            failing_for_secs = (now - since).as_secs(),
            "brokers unreachable; connecting again from the bootstrap addresses"
        );
        self.shared.signals.rebuild.notify_one();
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
    use std::sync::atomic::{AtomicU32, Ordering};

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
    async fn hands_out_a_client_connected_by_the_caller_without_connecting() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::with_client("test", Client(0), connect);

        assert_eq!(conn.current().as_deref(), Some(&Client(0)));
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert_eq!(attempts.load(Ordering::Relaxed), 0);

        let first = conn.current().unwrap();
        conn.report_unreachable(&first);
        tokio::time::sleep(RECONNECT_AFTER).await;
        assert!(conn.report_unreachable(&first));
        assert_eq!(*conn.connected().await, Client(1));
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_a_client_while_one_is_connected() {
        let (_, connect) = connector(2);
        let conn = Reconnecting::spawn("test", connect);

        let waited = tokio::time::timeout(Duration::from_secs(60), conn.connected()).await;

        assert_eq!(waited.as_deref(), Ok(&Client(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn stops_once_every_handle_is_dropped() {
        let (_, connect) = connector(0);
        let held = Arc::new(());
        let marker = held.clone();
        let conn = Reconnecting::spawn("test", move || {
            let _keep = &marker;
            connect()
        });
        next_client(&conn, None).await;

        drop(conn);
        tokio::time::sleep(Duration::from_secs(1)).await;

        // The connector, which holds the broker credentials, went with the background task.
        assert_eq!(Arc::strong_count(&held), 1);
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
    async fn connects_again_once_sends_keep_failing_to_reach_the_brokers() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", connect);
        let first = next_client(&conn, None).await;

        assert!(!conn.report_unreachable(&first));
        tokio::time::sleep(RECONNECT_AFTER - Duration::from_secs(1)).await;
        assert!(!conn.report_unreachable(&first));
        assert_eq!(
            conn.current().as_ref(),
            Some(&first),
            "kept until the failures have gone on long enough"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(conn.report_unreachable(&first));
        assert!(conn.current().is_none(), "dropped once they have");

        let second = next_client(&conn, Some(&first)).await;
        assert_eq!(*second, Client(2));
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn sends_failing_together_in_one_stall_keep_the_client() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", connect);
        let client = next_client(&conn, None).await;

        // Several sends waiting on the same slow broker time out at once.
        for _ in 0..5 {
            assert!(!conn.report_unreachable(&client));
        }

        assert_eq!(conn.current().as_ref(), Some(&client));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_success_starts_the_clock_again() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", connect);
        let client = next_client(&conn, None).await;

        conn.report_unreachable(&client);
        tokio::time::sleep(RECONNECT_AFTER - Duration::from_secs(1)).await;
        conn.report_success();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!conn.report_unreachable(&client));

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(conn.current().as_ref(), Some(&client));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_on_a_replaced_client_are_ignored() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", connect);
        let first = next_client(&conn, None).await;
        conn.report_unreachable(&first);
        tokio::time::sleep(RECONNECT_AFTER).await;
        assert!(conn.report_unreachable(&first));
        let second = next_client(&conn, Some(&first)).await;

        // A send still in flight on the first client reports its failure after the second took over.
        assert!(!conn.report_unreachable(&first));
        tokio::time::sleep(RECONNECT_AFTER).await;
        assert!(!conn.report_unreachable(&first));

        assert_eq!(conn.current().as_ref(), Some(&second));
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }
}
