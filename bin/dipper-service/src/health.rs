//! Worker liveness watermarks and a minimal HTTP health endpoint. Exit-based supervision catches
//! a worker that exits, not one that is wedged (alive but making no progress). Each worker loop
//! ticks its own watermark per iteration; the server reports 503 once any watermark goes stale.

use std::{
    net::SocketAddr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tokio::{net::TcpListener, sync::mpsc};

/// Default staleness threshold, twice the [`crate::worker::service::PROCESS_JOB_TIMEOUT`]
/// that bounds a single job, so a legitimately slow job never trips the probe.
pub const DEFAULT_HEALTH_THRESHOLD: Duration =
    Duration::from_secs(2 * crate::worker::service::PROCESS_JOB_TIMEOUT.as_secs());

/// Reference point for every watermark, fixed the first time liveness is touched. Watermarks are
/// seconds since this instant rather than wall-clock stamps, so an NTP step cannot make a healthy
/// worker look stale (or a wedged one look live) and hand the orchestrator a bogus verdict.
static PROCESS_START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Shared worker liveness: one progress watermark per worker loop, registered via
/// [`Liveness::register`] and ticked through the returned [`ProgressTicker`]. Health reflects the
/// oldest watermark, so one wedged loop trips the probe even while its siblings keep working.
#[derive(Clone)]
pub struct Liveness {
    // The std mutex is only locked to push a slot at registration and to read the slots on a
    // probe, never held across an await.
    slots: Arc<Mutex<Vec<Arc<AtomicI64>>>>,
}

/// A single worker loop's progress watermark. The loop calls
/// [`ProgressTicker::record_progress`] once per iteration.
pub struct ProgressTicker {
    slot: Arc<AtomicI64>,
}

impl ProgressTicker {
    /// Records that this loop just made progress.
    pub fn record_progress(&self) {
        self.slot.store(elapsed_secs(), Ordering::Relaxed);
    }
}

/// Seconds elapsed since [`PROCESS_START`]. Monotonic, so it never jumps with the system clock.
fn elapsed_secs() -> i64 {
    PROCESS_START.elapsed().as_secs() as i64
}

impl Liveness {
    /// Creates an empty tracker with no loops registered yet.
    pub fn new() -> Self {
        Self {
            slots: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Registers a worker loop and returns its ticker, seeded to now so a
    /// freshly spawned loop is considered live (startup grace).
    pub fn register(&self) -> ProgressTicker {
        let slot = Arc::new(AtomicI64::new(elapsed_secs()));
        self.slots
            .lock()
            .expect("liveness mutex poisoned")
            .push(slot.clone());
        ProgressTicker { slot }
    }

    /// Whether every registered loop made progress within `threshold` of `now_elapsed_secs`, where
    /// both are seconds since [`PROCESS_START`]. A single stale loop makes the whole worker
    /// unhealthy. Pure in its inputs so it is unit testable without waiting on real time.
    fn is_healthy_at(&self, now_elapsed_secs: i64, threshold: Duration) -> bool {
        let slots = self.slots.lock().expect("liveness mutex poisoned");
        // With no loops registered yet (startup) there is nothing stale to report.
        slots.iter().all(|slot| {
            let age = now_elapsed_secs.saturating_sub(slot.load(Ordering::Relaxed));
            age <= threshold.as_secs() as i64
        })
    }

    /// [`Liveness::is_healthy_at`] against the current elapsed time.
    pub fn is_healthy(&self, threshold: Duration) -> bool {
        self.is_healthy_at(elapsed_secs(), threshold)
    }
}

impl Default for Liveness {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to stop the health server.
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stops the health server.
    pub async fn stop(self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Binds the health server and returns a stop handle plus its run future. The listener is bound
/// eagerly so a bind failure surfaces at startup, and so callers and tests can read the address.
pub async fn new(
    addr: SocketAddr,
    liveness: Liveness,
    threshold: Duration,
) -> anyhow::Result<(
    Handle,
    SocketAddr,
    impl std::future::Future<Output = anyhow::Result<()>>,
)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (tx_stop, rx_stop) = mpsc::channel(1);
    let fut = serve(listener, liveness, threshold, rx_stop);
    Ok((Handle { tx_stop }, local_addr, fut))
}

/// State shared with the [`health`] handler.
#[derive(Clone)]
struct HealthState {
    liveness: Liveness,
    threshold: Duration,
}

/// `GET /health`: 200 while the worker watermark is fresh, 503 once it has gone
/// stale (the worker is wedged and the orchestrator should restart it).
async fn health(State(state): State<HealthState>) -> impl IntoResponse {
    if state.liveness.is_healthy(state.threshold) {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "worker stalled")
    }
}

/// Serves health requests until `stop_rx` fires. axum/hyper gives request parsing, per-connection
/// isolation and graceful shutdown, but no header-read or idle timeout: a stalled client holds its
/// connection for the process lifetime, which is acceptable on a cluster-internal port.
async fn serve(
    listener: TcpListener,
    liveness: Liveness,
    threshold: Duration,
    mut stop_rx: mpsc::Receiver<()>,
) -> anyhow::Result<()> {
    tracing::info!(addr = %listener.local_addr()?, "health server listening");
    let app = Router::new()
        .route("/health", get(health))
        .with_state(HealthState {
            liveness,
            threshold,
        });
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = stop_rx.recv().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::*;

    #[test]
    fn default_threshold_exceeds_the_job_timeout() {
        // A job timeout at or above the threshold means a job running to its bound looks wedged,
        // so k8s restarts a healthy pod. The threshold is derived from the timeout, so this
        // guards a future edit that sets it by hand.
        assert!(
            DEFAULT_HEALTH_THRESHOLD > crate::worker::service::PROCESS_JOB_TIMEOUT,
            "the health threshold must leave room for one full-length job"
        );
    }

    #[test]
    fn no_loops_registered_is_healthy() {
        // Before any loop registers (startup), there is nothing stale to report.
        let liveness = Liveness::new();
        let now = elapsed_secs() + 10_000;
        assert!(liveness.is_healthy_at(now, Duration::from_secs(600)));
    }

    #[test]
    fn fresh_watermark_is_healthy() {
        let liveness = Liveness::new();
        let _ticker = liveness.register();
        let now = elapsed_secs();
        assert!(liveness.is_healthy_at(now, Duration::from_secs(600)));
    }

    #[test]
    fn stale_watermark_is_unhealthy() {
        let liveness = Liveness::new();
        let _ticker = liveness.register();
        // Pretend "now" is well past the threshold since the watermark was set.
        let now = elapsed_secs() + 10_000;
        assert!(
            !liveness.is_healthy_at(now, Duration::from_secs(600)),
            "a watermark older than the threshold must report unhealthy"
        );
    }

    #[test]
    fn record_progress_refreshes_a_stale_watermark() {
        // Exercises the call the worker loop actually makes, rather than writing the slot by hand.
        let liveness = Liveness::new();
        let ticker = liveness.register();
        let threshold = Duration::from_secs(600);
        ticker
            .slot
            .store(elapsed_secs() - 10_000, Ordering::Relaxed);
        assert!(
            !liveness.is_healthy_at(elapsed_secs(), threshold),
            "a watermark 10000s old must report unhealthy before the tick"
        );

        ticker.record_progress();
        assert!(
            liveness.is_healthy_at(elapsed_secs(), threshold),
            "record_progress must advance the watermark back to now"
        );
    }

    #[test]
    fn one_stale_loop_among_fresh_is_unhealthy() {
        // The headline case: one loop wedges while the others keep ticking. The
        // stale loop must trip the probe even though its siblings are fresh.
        let liveness = Liveness::new();
        let fresh_a = liveness.register();
        let _stale = liveness.register();
        let fresh_b = liveness.register();

        // Move the two fresh loops' watermarks forward to `now`; the stale one
        // is left seeded at registration time.
        let now = elapsed_secs() + 10_000;
        fresh_a.slot.store(now, Ordering::Relaxed);
        fresh_b.slot.store(now, Ordering::Relaxed);

        assert!(
            !liveness.is_healthy_at(now, Duration::from_secs(600)),
            "a single stale loop must make the whole worker unhealthy"
        );
    }

    #[test]
    fn boundary_is_inclusive_healthy() {
        let liveness = Liveness::new();
        let _ticker = liveness.register();
        let base = elapsed_secs();
        // Exactly at the threshold is still healthy; one second past is not.
        assert!(liveness.is_healthy_at(base + 600, Duration::from_secs(600)));
        assert!(!liveness.is_healthy_at(base + 601, Duration::from_secs(600)));
    }

    /// Reads an HTTP response's status line from a fresh connection to the
    /// server.
    async fn probe(addr: SocketAddr) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        // `Connection: close` so the server closes the socket after responding
        // and `read_to_end` returns rather than blocking on HTTP/1.1 keep-alive.
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf);
        text.lines().next().unwrap_or_default().to_string()
    }

    #[tokio::test]
    async fn server_reports_503_when_stalled() {
        let liveness = Liveness::new();
        let ticker = liveness.register();
        // A zero threshold plus a backdated watermark makes the worker stale on
        // the spot, so the unhealthy branch is exercised without waiting.
        ticker.slot.store(elapsed_secs() - 1, Ordering::Relaxed);
        let (handle, addr, fut) = new("127.0.0.1:0".parse().unwrap(), liveness, Duration::ZERO)
            .await
            .unwrap();
        let server = tokio::spawn(fut);

        let status = probe(addr).await;
        assert!(
            status.contains("503"),
            "expected 503 when stalled, got: {status:?}"
        );

        handle.stop().await;
        let _ = server.await;
    }

    #[tokio::test]
    async fn server_reports_200_when_live() {
        let liveness = Liveness::new();
        let _ticker = liveness.register();
        let (handle, addr, fut) = new(
            "127.0.0.1:0".parse().unwrap(),
            liveness,
            Duration::from_secs(600),
        )
        .await
        .unwrap();
        let server = tokio::spawn(fut);

        let status = probe(addr).await;
        assert!(
            status.contains("200"),
            "expected 200 when live, got: {status:?}"
        );

        handle.stop().await;
        let _ = server.await;
    }

    /// A client that connects but never sends must not wedge the server: other probes are still
    /// answered promptly and graceful shutdown still completes. hyper isolates each connection;
    /// this guards against a future regression that drops that isolation.
    #[tokio::test]
    async fn stalled_client_does_not_block_other_probes() {
        let liveness = Liveness::new();
        let _ticker = liveness.register();
        let (handle, addr, fut) = new(
            "127.0.0.1:0".parse().unwrap(),
            liveness,
            Duration::from_secs(600),
        )
        .await
        .unwrap();
        let server = tokio::spawn(fut);

        // Open a connection and never write to it; its server-side read blocks.
        let _stalled = TcpStream::connect(addr).await.unwrap();

        // A well-behaved probe must still get a response without waiting on the
        // stalled client.
        let status = tokio::time::timeout(Duration::from_secs(2), probe(addr))
            .await
            .expect("a stalled client blocked an independent probe");
        assert!(status.contains("200"), "expected 200, got: {status:?}");

        // Shutdown must not wait on the stalled connection either.
        tokio::time::timeout(Duration::from_secs(2), handle.stop())
            .await
            .expect("a stalled client blocked shutdown");
        let _ = server.await;
    }
}
