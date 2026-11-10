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

/// Failed connect attempts between repeated warnings, after the first.
const WARN_EVERY: u32 = 5;

/// What a failed call through the client says about the client.
pub trait CallError {
    /// The call never reached the brokers, so a new client might succeed where this one can't.
    fn is_unreachable(&self) -> bool;

    /// The brokers answered, but this client can't serve the call however long it waits, such as a
    /// partition missing from what it learned when it connected, so only a new client can.
    fn needs_new_client(&self) -> bool {
        false
    }
}

/// A client connected in the background and rebuilt once calls have failed to reach the brokers for
/// [`RECONNECT_AFTER`]. Callers run calls through [`call`](Self::call), or report each outcome with
/// [`report`](Self::report); `current` returns `None` while the client is being (re)built.
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
    /// When the client was connected, so a client that can't serve a call is replaced at most
    /// once per [`RECONNECT_AFTER`] rather than in a loop.
    connected_at: Instant,
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
    /// Each attempt is abandoned after `connect_timeout`, since rskafka retries an unreachable
    /// bootstrap address with no deadline; give a connect with its own deadline a little longer.
    pub fn spawn<F, Fut, E>(name: &'static str, connect_timeout: Duration, connect: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        E: Display + Send,
    {
        Self::start(name, None, connect_timeout, connect)
    }

    /// Like [`spawn`](Self::spawn), starting from a client the caller already connected, so the
    /// caller decides what a failed first connect means; `connect` is used only for rebuilds.
    pub fn with_client<F, Fut, E>(
        name: &'static str,
        client: T,
        connect_timeout: Duration,
        connect: F,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        E: Display + Send,
    {
        Self::start(name, Some(client), connect_timeout, connect)
    }

    fn start<F, Fut, E>(
        name: &'static str,
        client: Option<T>,
        connect_timeout: Duration,
        connect: F,
    ) -> Self
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
                connected_at: Instant::now(),
            }),
            signals: signals.clone(),
        });
        let task = Arc::downgrade(&shared);
        tokio::spawn(async move {
            loop {
                if !connected {
                    let client = tokio::select! {
                        client = connect_with_retry(name, connect_timeout, &connect) => client,
                        _ = signals.closed.notified() => return,
                    };
                    let Some(shared) = task.upgrade() else {
                        return;
                    };
                    *shared.lock() = State {
                        client: Some(Arc::new(client)),
                        failing_since: None,
                        connected_at: Instant::now(),
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

    /// Runs `call` on the client, waiting while one is being connected, and reports the outcome.
    pub async fn call<R, E, F, Fut>(&self, call: F) -> Result<R, E>
    where
        E: CallError,
        F: FnOnce(Arc<T>) -> Fut,
        Fut: Future<Output = Result<R, E>>,
    {
        let client = self.connected().await;
        let result = call(Arc::clone(&client)).await;
        self.report(&client, &result);
        result
    }

    /// Records what a call through `client` showed: a success clears the outage, and a failure to
    /// reach the brokers counts toward a rebuild. A refusal from a broker counts as neither.
    pub fn report<R, E: CallError>(&self, client: &Arc<T>, result: &Result<R, E>) {
        match result {
            Ok(_) => self.report_success(client),
            Err(e) if e.is_unreachable() => {
                self.report_unreachable(client);
            }
            Err(e) if e.needs_new_client() => self.report_unservable(client),
            Err(_) => {}
        }
    }

    /// Records that a request through `client` reached the brokers, unless another client has
    /// replaced it since, so a late success on an old client can't hide an outage on the new one.
    fn report_success(&self, client: &Arc<T>) {
        let mut state = self.shared.lock();
        if state
            .client
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, client))
        {
            state.failing_since = None;
        }
    }

    /// Drops `client`, which the brokers answered but which can't serve a call, so the background
    /// task connects again, unless it was replaced since or connected less than
    /// [`RECONNECT_AFTER`] ago. Other calls succeeding through it don't save it.
    fn report_unservable(&self, client: &Arc<T>) {
        let mut state = self.shared.lock();
        let current = state
            .client
            .as_ref()
            .is_some_and(|c| Arc::ptr_eq(c, client));
        let age = state.connected_at.elapsed();
        if !current || age < RECONNECT_AFTER {
            return;
        }
        state.client = None;
        state.failing_since = None;
        drop(state);
        tracing::warn!(
            broker = self.shared.name,
            connected_for_secs = age.as_secs(),
            "the client can't serve a call the brokers answered; connecting again"
        );
        self.shared.signals.rebuild.notify_one();
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
        state.client = None;
        state.failing_since = None;
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
async fn connect_with_retry<T, F, Fut, E>(
    name: &'static str,
    connect_timeout: Duration,
    connect: &F,
) -> T
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Display,
{
    let mut attempt: u32 = 0;
    loop {
        let error = match tokio::time::timeout(connect_timeout, connect()).await {
            Ok(Ok(client)) => return client,
            Ok(Err(err)) => err.to_string(),
            Err(_) => format!("timed out after {}s", connect_timeout.as_secs()),
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

    const TIMEOUT: Duration = Duration::from_secs(30);

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

    /// A call's error, saying whether it reached the brokers.
    #[derive(Debug)]
    struct Failure {
        unreachable: bool,
    }

    impl CallError for Failure {
        fn is_unreachable(&self) -> bool {
            self.unreachable
        }
    }

    #[tokio::test(start_paused = true)]
    async fn calls_go_through_a_new_client_once_the_old_one_keeps_failing() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);
        // Client 1 can't reach the brokers; any later client can.
        let call = || {
            conn.call(|client: Arc<Client>| async move {
                if client.0 == 1 {
                    Err(Failure { unreachable: true })
                } else {
                    Ok(client.0)
                }
            })
        };

        assert!(call().await.is_err());
        tokio::time::sleep(RECONNECT_AFTER).await;
        assert!(call().await.is_err(), "this failure drops client 1");

        assert_eq!(call().await.unwrap(), 2);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_success_through_a_replaced_client_leaves_the_outage_running() {
        let (_, connect) = connector(0);
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);
        let first = next_client(&conn, None).await;
        conn.report_unreachable(&first);
        tokio::time::sleep(RECONNECT_AFTER).await;
        assert!(conn.report_unreachable(&first));
        let second = next_client(&conn, Some(&first)).await;

        conn.report_unreachable(&second);
        tokio::time::sleep(RECONNECT_AFTER).await;
        // A call still in flight on the first client finishes well.
        conn.report(&first, &Ok::<(), Failure>(()));

        assert!(
            conn.report_unreachable(&second),
            "the second client's outage still counts"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_call_the_brokers_answered_keeps_the_client() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);
        let refused =
            || conn.call(|_: Arc<Client>| async { Err::<(), _>(Failure { unreachable: false }) });

        assert!(refused().await.is_err());
        tokio::time::sleep(RECONNECT_AFTER).await;
        assert!(refused().await.is_err());

        assert_eq!(conn.current().as_deref(), Some(&Client(1)));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn keeps_trying_until_the_first_connect_succeeds() {
        let (attempts, connect) = connector(2);
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);

        let client = next_client(&conn, None).await;

        assert_eq!(*client, Client(3));
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn hands_out_a_client_connected_by_the_caller_without_connecting() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::with_client("test", Client(0), TIMEOUT, connect);

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
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);

        let waited = tokio::time::timeout(Duration::from_secs(60), conn.connected()).await;

        assert_eq!(waited.as_deref(), Ok(&Client(3)));
    }

    #[tokio::test(start_paused = true)]
    async fn stops_once_every_handle_is_dropped() {
        let (_, connect) = connector(0);
        let held = Arc::new(());
        let marker = held.clone();
        let conn = Reconnecting::spawn("test", TIMEOUT, move || {
            let _keep = &marker;
            connect()
        });
        next_client(&conn, None).await;

        drop(conn);
        tokio::time::sleep(Duration::from_secs(1)).await;

        // The connector, which holds the broker credentials, went with the background task.
        assert_eq!(Arc::strong_count(&held), 1);
    }

    /// An answer from the brokers that this client can't serve, such as a partition it never learned.
    #[derive(Debug)]
    struct Unservable;

    impl CallError for Unservable {
        fn is_unreachable(&self) -> bool {
            false
        }

        fn needs_new_client(&self) -> bool {
            true
        }
    }

    #[tokio::test(start_paused = true)]
    async fn replaces_a_client_that_cant_serve_a_call_at_most_once_a_minute() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::with_client("test", Client(0), TIMEOUT, connect);
        let first = conn.current().unwrap();
        let unservable = Err::<(), _>(Unservable);

        conn.report(&first, &Ok::<(), Unservable>(()));
        conn.report(&first, &unservable);
        assert!(conn.current().is_some(), "a client this new is kept");

        tokio::time::sleep(RECONNECT_AFTER).await;
        conn.report(&first, &unservable);
        assert!(
            conn.current().is_none(),
            "even while other calls through it succeed"
        );
        assert_eq!(*conn.connected().await, Client(1));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_a_rebuild_as_long_as_the_caller_allows() {
        let attempts = Arc::new(AtomicU32::new(0));
        let counted = attempts.clone();
        let conn =
            Reconnecting::with_client("test", Client(0), Duration::from_secs(60), move || {
                let n = counted.fetch_add(1, Ordering::Relaxed) + 1;
                async move {
                    tokio::time::sleep(Duration::from_secs(45)).await;
                    Ok::<_, String>(Client(n))
                }
            });
        let first = conn.current().unwrap();
        conn.report_unreachable(&first);
        tokio::time::sleep(RECONNECT_AFTER).await;
        assert!(conn.report_unreachable(&first));

        assert_eq!(
            *conn.connected().await,
            Client(1),
            "the first slow attempt succeeded"
        );
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_on_a_connect_attempt_that_never_finishes() {
        let attempts = Arc::new(AtomicU32::new(0));
        let counted = attempts.clone();
        let conn = Reconnecting::spawn("test", TIMEOUT, move || {
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
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);
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
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);
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
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);
        let client = next_client(&conn, None).await;

        conn.report_unreachable(&client);
        tokio::time::sleep(RECONNECT_AFTER - Duration::from_secs(1)).await;
        conn.report(&client, &Ok::<(), Failure>(()));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!conn.report_unreachable(&client));

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(conn.current().as_ref(), Some(&client));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn failures_on_a_replaced_client_are_ignored() {
        let (attempts, connect) = connector(0);
        let conn = Reconnecting::spawn("test", TIMEOUT, connect);
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
