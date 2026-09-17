use std::{
    future::Future,
    time::{Duration, Instant},
};

use dipper_core::state::FromState;
use dipper_iisa::CandidateSelection;
use time::OffsetDateTime;
use tokio::{sync::watch, task::JoinSet};

use super::{
    context::{Ctx, InnerCtx},
    handlers::{
        self, CancelRejectedAgreementOnChainCtx, ReassessIndexingRequestCtx,
        SUBMIT_OFFER_DROPPED_TX_RETRY_BASE, SUBMIT_OFFER_TRANSIENT_RETRY_BASE,
        SendIndexingAgreementProposalCtx, SubmitOfferCtx,
    },
    messages::Message,
    queue::{JobNotifications, Queue},
    result::{JobError, JobResult, calculate_backoff_delay, retries_within_window},
};
pub use super::{
    queue::JobPriority,
    service_queue::{WorkerQueue, WorkerQueueHandle},
};
use crate::{
    chain_client::ChainClient,
    indexer_rpc_client::IndexerClient,
    registry::{
        AgreementRegistry, IndexerDenylistRegistry, IndexingRequestRegistry,
        PendingCancellationRegistry,
    },
};

/// Default period to poll the queue for new jobs
const DEFAULT_QUEUE_POLL_PERIOD: Duration = Duration::from_secs(1);

/// Backstop timeout for processing a single job.
///
/// Every external call `process_job` makes is already individually bounded
/// (IISA HTTP, indexer RPC, chain RPC + receipt polling), so the legitimate
/// worst case is their sum, on the order of a couple of minutes. This timeout
/// sits comfortably above that and only fires if a dependency accepts the
/// connection but never responds, defeating the per-call timeouts. It also
/// bounds how long a chain submission may retry across the RPC providers
/// (see `derive_submit_deadline`): 4/5 of it must hold two walks of a
/// 3-provider ring at the default 10s timeout and 3 retries. Critically,
/// for the whole `process_job` call the job's `JobGuard` holds the row's
/// `Running` lock (and the pgmq transaction behind it). An unbounded hang
/// would therefore both wedge the worker loop and pin that row indefinitely.
/// On elapse the in-flight `process_job` future is cancelled (dropped), which
/// unblocks the loop, and the timeout is surfaced as a retryable error so the
/// `JobGuard` reschedules the row and releases its lock. Recovery is
/// idempotent (chain-as-source-of-truth), so re-running a job whose handler
/// was cancelled mid-flight is safe.
pub(crate) const PROCESS_JOB_TIMEOUT: Duration = Duration::from_secs(420);

/// Base backoff for a job rescheduled after hitting [`PROCESS_JOB_TIMEOUT`].
const JOB_TIMEOUT_RETRY_BASE_DELAY: Duration = Duration::from_secs(30);

/// Runs a `process_job` future under [`PROCESS_JOB_TIMEOUT`]. On elapse it
/// cancels the in-flight future and returns a retryable error so the worker
/// reschedules the job (via its `JobGuard`) rather than blocking indefinitely.
async fn run_job_with_timeout<F>(timeout: Duration, fut: F) -> JobResult<()>
where
    F: std::future::Future<Output = JobResult<()>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(res) => res,
        Err(_elapsed) => Err(JobError::Retryable(
            anyhow::anyhow!("job processing exceeded the {timeout:?} backstop timeout"),
            JOB_TIMEOUT_RETRY_BASE_DELAY,
        )),
    }
}

/// What the worker should do after waiting for the next trigger.
#[derive(Debug, PartialEq, Eq)]
enum Tick {
    /// A stop signal was received; the worker loop should exit.
    Stop,
    /// Attempt to pop and process the next job. `listener_failed` is true only
    /// when the listener broke during this very tick, which is what paces
    /// re-subscription (see `should_resubscribe`).
    Poll { listener_failed: bool },
}

/// Whether the loop should try to re-open the notification subscription now.
///
/// Only when there is no listener and it did not just break on this tick. A
/// `wait_for_notification` that fails fast, on a pool timeout say, returns
/// immediately, so re-subscribing on the same tick and landing on another that
/// fails just as fast would spin the loop at full CPU: fail, re-subscribe, pop,
/// fail, with no poll-period sleep anywhere. Waiting one tick means the listener
/// is already absent when the wait runs, so the wait falls through to the
/// poll-period sleep and paces the retry.
///
/// Attempts are further spaced out by `backoff`, so a database that cannot hold
/// the subscription is not asked once a second per loop indefinitely.
fn should_resubscribe(
    listener_present: bool,
    listener_failed_this_tick: bool,
    backoff: &ResubscribeBackoff,
    now: Instant,
) -> bool {
    !listener_present && !listener_failed_this_tick && backoff.is_due(now)
}

/// Shortest and longest wait between re-subscription attempts.
const RESUBSCRIBE_BASE_DELAY: Duration = Duration::from_secs(1);
const RESUBSCRIBE_MAX_DELAY: Duration = Duration::from_secs(30);

/// How long a subscription must survive before the next failure is treated as a
/// fresh problem rather than a continuation of the last one.
const RESUBSCRIBE_STABLE_PERIOD: Duration = RESUBSCRIBE_MAX_DELAY;

/// How many times the delay may double before it stops growing. Five doublings
/// take the 1 second base past the 30 second ceiling.
const RESUBSCRIBE_MAX_DOUBLINGS: u32 = 5;

/// Spaces out re-subscription attempts while the notification connection keeps
/// failing, so eight worker loops do not open eight connections a second
/// against a database that is already struggling.
#[derive(Debug, Default)]
struct ResubscribeBackoff {
    failures: u32,
    next_attempt: Option<Instant>,
    subscribed_at: Option<Instant>,
}

impl ResubscribeBackoff {
    /// Whether enough time has passed since the last failure to try again.
    fn is_due(&self, now: Instant) -> bool {
        self.next_attempt.is_none_or(|at| now >= at)
    }

    /// Records a subscription that failed, or one that broke after being
    /// established, and schedules the next attempt.
    fn record_failure(&mut self, now: Instant) {
        // A subscription that held for a good while was not part of this run of
        // failures, so its loss starts the delay again from the bottom.
        let was_stable = self
            .subscribed_at
            .is_some_and(|at| now.duration_since(at) >= RESUBSCRIBE_STABLE_PERIOD);
        if was_stable {
            self.failures = 0;
        }
        self.subscribed_at = None;
        self.failures = self.failures.saturating_add(1);
        self.next_attempt = Some(now + self.delay());
    }

    /// Records a subscription that was established, clearing the wait.
    fn record_success(&mut self, now: Instant) {
        self.next_attempt = None;
        self.subscribed_at = Some(now);
    }

    /// The wait after the failures seen so far: 1 second, then doubling to a
    /// ceiling of 30 seconds.
    fn delay(&self) -> Duration {
        let doublings = self
            .failures
            .saturating_sub(1)
            .min(RESUBSCRIBE_MAX_DOUBLINGS);
        (RESUBSCRIBE_BASE_DELAY * (1u32 << doublings)).min(RESUBSCRIBE_MAX_DELAY)
    }
}

/// How long to wait before repeating a warning about the same ongoing problem.
const WARN_COOLDOWN: Duration = Duration::from_secs(60);

/// Paces warnings about a notification connection that will not stay up, and
/// remembers whether an operator was told, so the recovery can be reported at
/// the same volume.
///
/// Counting failures does not work for this. A flapping connection that
/// delivers the occasional notification resets any "consecutive" count, so
/// every cycle looks like a first failure and warns, which is the noise this
/// exists to prevent. It also makes the interval depend on how quickly attempts
/// fail rather than on the clock. Time is the honest measure of "still broken".
#[derive(Debug, Default)]
struct WarnPacer {
    last_warned: Option<Instant>,
    announced: bool,
    failures_since_warning: u32,
    failures_since_success: u32,
}

impl WarnPacer {
    /// Counts a failure, returning how many happened since the last warning
    /// when it is time to warn again. The count tells a blip apart from a
    /// connection failing dozens of times a minute, which otherwise look alike.
    fn record_failure(&mut self, now: Instant) -> Option<u32> {
        self.failures_since_warning = self.failures_since_warning.saturating_add(1);
        self.failures_since_success = self.failures_since_success.saturating_add(1);

        let due = self
            .last_warned
            .is_none_or(|last| now.duration_since(last) >= WARN_COOLDOWN);
        if !due {
            return None;
        }
        self.last_warned = Some(now);
        self.announced = true;
        Some(std::mem::take(&mut self.failures_since_warning))
    }

    /// How many attempts failed before this success, when a warning is
    /// outstanding, clearing both so one recovery answers one warning. The
    /// cooldown is left alone so a flap cannot warn on every single cycle.
    fn take_announced(&mut self) -> Option<u32> {
        let failures = std::mem::take(&mut self.failures_since_success);
        std::mem::take(&mut self.announced).then_some(failures)
    }
}

/// Waits for the next trigger to attempt a queue poll: a stop signal, a
/// `LISTEN`/`NOTIFY` notification, or the poll-interval fallback.
///
/// On a listener error the listener is dropped (`*listener = None`) and the
/// worker degrades to poll-only operation. This is correct, not a degraded
/// state to be feared: `queue.pop()` is independent of `LISTEN`/`NOTIFY`, since
/// the notification only wakes the loop earlier than the poll interval as a
/// latency optimisation, so a listener fault never stops job processing and
/// never leaves the worker in an uncertain state. The caller re-subscribes on a
/// later poll.
///
/// Whichever branch does not win has its future dropped. In the steady state
/// that future is suspended in sqlx's `recv_unchecked`, which is written to be
/// cancel-safe: it leaves its read buffer untouched until a whole message has
/// arrived. What makes that sufficient is that the buffer lives on the
/// connection the listener owns rather than in the future, so a half-read
/// message stays buffered for the next call, and a message that did finish
/// arriving makes the branch ready, which returns from the select before the
/// poll timer below is even polled.
///
/// Notifications are still lost whenever the connection goes down, cancellation
/// or not, because PostgreSQL only holds them for sessions currently listening.
/// The 1 second poll bounds that to added latency, never a dropped job, which is
/// the property worth relying on here.
async fn await_next_tick<N: JobNotifications>(
    stop_rx: &mut watch::Receiver<bool>,
    listener: &mut Option<N>,
    poll_period: Duration,
    warn_pacer: &mut WarnPacer,
) -> Tick {
    match listener {
        Some(l) => {
            tokio::select! { biased;
                res = stop_rx.changed() => {
                    // Sender dropped (Err) or value flipped to true: shut down.
                    if res.is_err() || *stop_rx.borrow() {
                        Tick::Stop
                    } else {
                        Tick::Poll { listener_failed: false }
                    }
                }
                res = l.wait_for_notification() => {
                    match res {
                        Ok(()) => Tick::Poll { listener_failed: false },
                        Err(err) => {
                            match warn_pacer.record_failure(Instant::now()) {
                                Some(failures) => tracing::warn!(
                                    error=?err,
                                    failures_since_last_warning=failures,
                                    "job-available listener failed; degrading to poll-only until it can be re-established"
                                ),
                                None => tracing::debug!(
                                    error=?err,
                                    "job-available listener failed again; staying in poll-only mode"
                                ),
                            }
                            *listener = None;
                            Tick::Poll { listener_failed: true }
                        }
                    }
                }
                _ = tokio::time::sleep(poll_period) => Tick::Poll { listener_failed: false },
            }
        }
        None => {
            tokio::select! { biased;
                res = stop_rx.changed() => {
                    if res.is_err() || *stop_rx.borrow() {
                        Tick::Stop
                    } else {
                        Tick::Poll { listener_failed: false }
                    }
                }
                _ = tokio::time::sleep(poll_period) => Tick::Poll { listener_failed: false },
            }
        }
    }
}

/// One worker loop's job-available subscription, and everything that keeps it
/// in repair: the poll-interval fallback, degrading to poll-only on a fault,
/// and paced re-subscription afterwards.
struct Subscription<N> {
    /// `Some` while `LISTEN`/`NOTIFY` is healthy, `None` once it has degraded
    /// to poll-only operation (see `await_next_tick`).
    listener: Option<N>,
    warn_pacer: WarnPacer,
    backoff: ResubscribeBackoff,
}

impl<N: JobNotifications> Subscription<N> {
    /// Subscribes for the first time, starting in poll-only mode if that fails.
    /// Failing the loop instead would stop every sibling loop with it, and the
    /// notification is only a latency optimisation over the poll interval.
    async fn start<Q: Queue<Message, Listener = N>>(queue: &Q) -> Self {
        let mut this = Self {
            listener: None,
            warn_pacer: WarnPacer::default(),
            backoff: ResubscribeBackoff::default(),
        };
        match queue.subscribe().await {
            Ok(l) => {
                this.backoff.record_success(Instant::now());
                this.listener = Some(l);
            }
            Err(err) => {
                this.backoff.record_failure(Instant::now());
                this.warn_pacer.record_failure(Instant::now());
                tracing::warn!(
                    error=?err,
                    "could not subscribe to job-available notifications at startup; starting in poll-only mode"
                );
            }
        }
        this
    }

    /// Waits for the next trigger to poll the queue, repairing the subscription
    /// when the pacing rules allow. Returns `false` once the worker should stop.
    async fn next_tick<Q: Queue<Message, Listener = N>>(
        &mut self,
        queue: &Q,
        stop_rx: &mut watch::Receiver<bool>,
        poll_period: Duration,
    ) -> bool {
        let listener_failed = match await_next_tick(
            stop_rx,
            &mut self.listener,
            poll_period,
            &mut self.warn_pacer,
        )
        .await
        {
            Tick::Stop => return false,
            Tick::Poll { listener_failed } => listener_failed,
        };

        if listener_failed {
            self.backoff.record_failure(Instant::now());
        }
        if should_resubscribe(
            self.listener.is_some(),
            listener_failed,
            &self.backoff,
            Instant::now(),
        ) {
            self.resubscribe(queue).await;
        }
        true
    }

    /// Re-opens the subscription. A failure here keeps us in poll-only mode,
    /// which processes every job just as reliably, only later.
    async fn resubscribe<Q: Queue<Message, Listener = N>>(&mut self, queue: &Q) {
        match queue.subscribe().await {
            Ok(l) => {
                self.backoff.record_success(Instant::now());
                // Answer every warning exactly once, so a problem an operator
                // was told about is reported as over, while a flap nobody was
                // told about stays quiet.
                match self.warn_pacer.take_announced() {
                    Some(failed_attempts) => tracing::info!(
                        failed_attempts,
                        "re-subscribed to job-available notifications"
                    ),
                    None => tracing::debug!("re-subscribed to job-available notifications"),
                }
                self.listener = Some(l);
            }
            Err(err) => {
                self.backoff.record_failure(Instant::now());
                match self.warn_pacer.record_failure(Instant::now()) {
                    Some(failures) => tracing::warn!(
                        error=?err,
                        failures_since_last_warning=failures,
                        "job-available listener still unavailable; jobs are running on the poll interval alone"
                    ),
                    None => tracing::debug!(
                        error=?err,
                        "job-available listener re-subscription failed; staying in poll-only mode"
                    ),
                }
            }
        }
    }
}

/// Create a worker plus a future that runs `concurrency` (>=1) loops, each
/// draining the same queue. The queue's `FOR UPDATE SKIP LOCKED` pop hands
/// every loop a distinct job; the default of 1 is a single serial loop.
pub fn new<S, Q, R, C, I, T>(state: S) -> (Handle<Q>, impl Future<Output = anyhow::Result<()>>)
where
    Q: Queue<Message> + Clone + Send + Sync + 'static,
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + crate::network::service::chain_listener::ChainListenerStateRegistry
        + Clone
        + Send
        + Sync
        + 'static,
    C: IndexerClient + Clone + Send + Sync + 'static,
    I: CandidateSelection + Clone + Send + Sync + 'static,
    T: ChainClient + Clone + Send + Sync + 'static,
    S: Into<Ctx<Q, R, C, I, T>>,
{
    let Ctx {
        queue,
        signer,
        agreement_conf,
        rca_domain,
        pricing_table,
        registry,
        network,
        client,
        iisa,
        chain_client,
        networks_registry,
        additional_networks,
        entity_count_cache,
        chain_listener_notify,
        bypass_chain_clock_defenses,
        chain_listener_chain_id,
        liveness,
        reassess_lock,
        unresponsive_breaker,
        dips_accepting_cache,
        concurrency,
        subgraph_indexing_agreements_events_emitter,
    } = state.into();

    // A watch channel fans the stop signal out to every loop; a single mpsc
    // receiver could only wake one of them.
    let (stop_tx, stop_rx) = watch::channel(false);

    // An offer is worth resubmitting for as long as the indexer can still accept
    // it, so the budget comes from that window. Sized on the shorter of the two
    // backoffs, it cannot run out early whichever failure the job keeps hitting.
    let submit_offer_max_retries = retries_within_window(
        Duration::from_secs(agreement_conf.deadline_seconds()),
        SUBMIT_OFFER_DROPPED_TX_RETRY_BASE.min(SUBMIT_OFFER_TRANSIENT_RETRY_BASE),
    );

    let handle = Handle {
        stop_tx: stop_tx.clone(),
        worker_queue_handle: WorkerQueueHandle::new(queue.clone(), submit_offer_max_retries),
    };
    let fut = async move {
        // Built once, cloned per loop. Every field is Arc/Clone, so the clones
        // share the same registry, caches and reassess locks.
        let state = InnerCtx {
            signer,
            agreement_conf,
            rca_domain,
            pricing_table,
            registry,
            network,
            client,
            iisa,
            chain_client,
            networks_registry,
            additional_networks,
            entity_count_cache,
            chain_listener_notify,
            worker: WorkerQueueHandle::new(queue.clone(), submit_offer_max_retries),
            bypass_chain_clock_defenses,
            chain_listener_chain_id,
            reassess_lock,
            unresponsive_breaker,
            dips_accepting_cache,
            subgraph_indexing_agreements_events_emitter,
        };

        let mut set = JoinSet::new();
        for _ in 0..concurrency.max(1) {
            // Each loop gets its own watermark so the health endpoint can spot a
            // single wedged loop, not just the case where every loop stalls.
            set.spawn(run_loop(
                state.clone(),
                queue.clone(),
                stop_rx.clone(),
                liveness.register(),
            ));
        }
        // Drop the supervisor's own receiver so `Handle::stop`'s `closed()`
        // tracks only the loops.
        drop(stop_rx);

        let mut first_err: Option<anyhow::Error> = None;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::error!(error=?err, "Worker loop exited with error");
                    first_err.get_or_insert(err);
                }
                Err(err) => {
                    // A panicking loop must also fail the worker, not be swallowed
                    // into an Ok, otherwise a single loop death looks like success.
                    tracing::error!(error=?err, "Worker loop task panicked");
                    first_err.get_or_insert_with(|| anyhow::Error::new(err));
                }
            }
            // One loop ended (shutdown or death); stop the rest too so the worker
            // drains and resolves instead of running on at reduced capacity.
            let _ = stop_tx.send(true);
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    };

    (handle, fut)
}

/// One worker loop: drains jobs until the stop signal fires. Each loop owns its
/// own queue notification listener because the underlying `LISTEN`/`NOTIFY` is
/// per-connection and can't be shared across tasks.
async fn run_loop<Q, R, C, I, T>(
    state: InnerCtx<R, WorkerQueueHandle<Q>, C, I, T>,
    queue: Q,
    mut stop_rx: watch::Receiver<bool>,
    liveness: crate::health::ProgressTicker,
) -> anyhow::Result<()>
where
    Q: Queue<Message> + Clone + Send + Sync + 'static,
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + crate::network::service::chain_listener::ChainListenerStateRegistry
        + Clone
        + Send
        + Sync
        + 'static,
    C: IndexerClient + Clone + Send + Sync + 'static,
    I: CandidateSelection + Clone + Send + Sync + 'static,
    T: ChainClient + Clone + Send + Sync + 'static,
{
    let mut subscription = Subscription::start(&queue).await;
    loop {
        if !subscription
            .next_tick(&queue, &mut stop_rx, DEFAULT_QUEUE_POLL_PERIOD)
            .await
        {
            return Ok(());
        }

        // Tick the liveness watermark on every iteration (including idle polls
        // that find no job) so the health endpoint sees a worker that is still
        // making its way around the loop rather than one that has wedged.
        liveness.record_progress();

        // Process the job
        let job = match queue.pop().await {
            Ok(Some(job)) => job,
            Ok(None) => continue,
            Err(err) => {
                // An unexpected DB failure (the empty queue is Ok(None) above);
                // surface it so a DB outage doesn't leave every loop spinning
                // silently while the worker still looks healthy.
                tracing::warn!(error=?err, "Failed to get next job from queue");
                continue;
            }
        };

        let _span = tracing::debug_span!("process_job", job = %job.id());
        match run_job_with_timeout(PROCESS_JOB_TIMEOUT, process_job(&state, job.desc())).await {
            Ok(..) => {
                if let Err(err) = job.remove().await {
                    tracing::debug!(error=?err, "Failed to remove job from queue");
                }
            }
            Err(JobError::Retryable(err, base_delay)) => {
                let attempt = job.failed_attempts();
                let delay = calculate_backoff_delay(base_delay, attempt);

                tracing::debug!(
                    error=?err,
                    attempt=%attempt,
                    delay_secs=%delay.as_secs(),
                    "Rescheduling job after failure with backoff"
                );

                let scheduled_for = OffsetDateTime::now_utc() + delay;
                if let Err(err) = job.mark_as_failed_and_reschedule(scheduled_for).await {
                    tracing::error!(error=?err, "Failed to reschedule job");
                }
            }
            Err(JobError::Deferred(delay)) => {
                // Couldn't run now (another reassessment holds the global lock);
                // re-queue at a flat delay without counting a failed attempt.
                // Logged at info so sustained contention is visible per job id.
                let scheduled_for = OffsetDateTime::now_utc() + delay;
                tracing::info!(
                    job = %job.id(),
                    delay_secs = %delay.as_secs(),
                    "Deferring job; another reassessment holds the global lock, will retry"
                );
                if let Err(err) = job.reschedule(scheduled_for).await {
                    tracing::error!(error=?err, "Failed to reschedule deferred job");
                }
            }
            Err(JobError::Fatal(err)) => {
                tracing::debug!(error=?err, "Failed to process job");

                // Remove the job from the queue as it failed and
                // should not be retried
                if let Err(err) = job.remove().await {
                    tracing::error!(error=?err, "Failed to remove job from queue");
                }
            }
        }
    }
}

async fn process_job<S, W, R, C, I, T>(state: &S, message: &Message) -> JobResult<()>
where
    R: IndexingRequestRegistry
        + AgreementRegistry
        + IndexerDenylistRegistry
        + PendingCancellationRegistry
        + crate::network::service::chain_listener::ChainListenerStateRegistry
        + Sync,
    W: WorkerQueue,
    C: IndexerClient,
    I: CandidateSelection,
    T: ChainClient,
    ReassessIndexingRequestCtx<R, W, I, T>: FromState<S>,
    SendIndexingAgreementProposalCtx<R, W, C>: FromState<S>,
    CancelRejectedAgreementOnChainCtx<R, T>: FromState<S>,
    SubmitOfferCtx<R, T>: FromState<S>,
{
    /// Dispatch a message to the appropriate message handler, based on the message type.
    macro_rules! _dispatch {
        ($state:expr, $message:expr, {$($msg_pat:path => $handler_fn:path),* $(,)?}) => {
            match $message {
                $(
                    $msg_pat(msg) => $handler_fn(FromState::from_state($state), msg).await,
                )*
            }
        };
    }

    _dispatch!(state, message, {
        Message::ReassessIndexingRequest => handlers::reassess_indexing_request,
        Message::SendIndexingAgreementProposal => handlers::send_indexing_agreement_proposal,
        Message::CancelRejectedAgreementOnChain => handlers::cancel_rejected_agreement_on_chain,
        Message::SubmitOffer => handlers::submit_offer,
    })
}

/// The worker service handle
#[derive(Clone)]
pub struct Handle<Q> {
    /// Broadcasts the stop signal to every worker loop
    stop_tx: watch::Sender<bool>,

    /// A handle to the worker's queue
    worker_queue_handle: WorkerQueueHandle<Q>,
}

impl<Q> Handle<Q> {
    /// Get a handle to the worker's queue
    pub fn queue(&self) -> &WorkerQueueHandle<Q> {
        &self.worker_queue_handle
    }

    /// Stop the worker and wait for every loop to drain.
    pub async fn stop(self) {
        if self.stop_tx.is_closed() {
            return;
        }

        // One send wakes all loops; closed() resolves once they've all exited.
        let _ = self.stop_tx.send(true);
        self.stop_tx.closed().await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use tokio::sync::watch;

    use super::{
        DEFAULT_QUEUE_POLL_PERIOD, JobError, Message, RESUBSCRIBE_BASE_DELAY,
        RESUBSCRIBE_MAX_DELAY, RESUBSCRIBE_STABLE_PERIOD, ResubscribeBackoff, Subscription, Tick,
        WARN_COOLDOWN, WarnPacer, await_next_tick, run_job_with_timeout, should_resubscribe,
    };
    use crate::worker::queue::{JobGuard, JobId, JobNotifications, JobPriority, Queue};

    /// A listener stub whose `wait_for_notification` always errors. Models a
    /// dropped/broken `LISTEN`/`NOTIFY` connection.
    struct FailingNotifier;

    #[async_trait]
    impl JobNotifications for FailingNotifier {
        async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("simulated listener failure")
        }
    }

    /// A listener stub that records being consulted, then never notifies, so a
    /// test can tell whether the loop actually used the subscription it opened.
    struct RecordingNotifier {
        waits: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl JobNotifications for RecordingNotifier {
        async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
            self.waits.fetch_add(1, Ordering::SeqCst);
            future::pending().await
        }
    }

    /// A queue with no database behind it, whose `subscribe` fails a set number
    /// of times before it starts succeeding.
    struct MockQueue {
        subscribe_attempts: AtomicUsize,
        failures_before_success: usize,
        listener_waits: Arc<AtomicUsize>,
    }

    impl MockQueue {
        fn failing_first(failures_before_success: usize) -> Self {
            Self {
                subscribe_attempts: AtomicUsize::new(0),
                failures_before_success,
                listener_waits: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn subscribe_attempts(&self) -> usize {
            self.subscribe_attempts.load(Ordering::SeqCst)
        }

        fn listener_waits(&self) -> usize {
            self.listener_waits.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Queue<Message> for MockQueue {
        type Listener = RecordingNotifier;

        async fn push(&self, _msg: Message, _priority: JobPriority) -> anyhow::Result<JobId> {
            anyhow::bail!("these tests never push")
        }

        async fn push_with_max_retries(
            &self,
            _msg: Message,
            _priority: JobPriority,
            _max_retries: u32,
        ) -> anyhow::Result<JobId> {
            anyhow::bail!("these tests never push")
        }

        async fn pop(&self) -> anyhow::Result<Option<JobGuard<'_, Message>>> {
            Ok(None)
        }

        async fn subscribe(&self) -> anyhow::Result<Self::Listener> {
            let attempt = self.subscribe_attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < self.failures_before_success {
                anyhow::bail!("simulated subscribe failure");
            }
            Ok(RecordingNotifier {
                waits: self.listener_waits.clone(),
            })
        }
    }

    /// A subscription that cannot be opened at startup must leave the worker
    /// running in poll-only mode. Failing here would stop this loop, and one
    /// loop stopping stops every sibling loop with it.
    #[tokio::test(start_paused = true)]
    async fn a_failed_startup_subscribe_leaves_the_loop_running() {
        let queue = MockQueue::failing_first(usize::MAX);
        let (_tx, mut rx) = watch::channel(false);

        let mut subscription = Subscription::start(&queue).await;
        assert!(
            subscription.listener.is_none(),
            "a failed startup subscribe should degrade to poll-only"
        );

        for tick in 0..3 {
            assert!(
                subscription
                    .next_tick(&queue, &mut rx, Duration::from_secs(1))
                    .await,
                "the loop should still be running on tick {tick}"
            );
        }
    }

    /// A worker that degraded to poll-only must re-open the subscription on a
    /// later tick, and then actually use it, rather than polling forever.
    #[tokio::test(start_paused = true)]
    async fn a_degraded_subscription_is_re_opened_and_then_used() {
        let queue = MockQueue::failing_first(0);
        let (_tx, mut rx) = watch::channel(false);
        let mut subscription: Subscription<RecordingNotifier> = Subscription {
            listener: None,
            warn_pacer: WarnPacer::default(),
            backoff: ResubscribeBackoff::default(),
        };

        assert!(
            subscription
                .next_tick(&queue, &mut rx, Duration::from_secs(1))
                .await
        );
        assert_eq!(
            queue.subscribe_attempts(),
            1,
            "a tick with no listener should re-subscribe"
        );
        assert!(subscription.listener.is_some());

        assert!(
            subscription
                .next_tick(&queue, &mut rx, Duration::from_secs(1))
                .await
        );
        assert_eq!(
            queue.listener_waits(),
            1,
            "the re-opened subscription should be waited on, not ignored"
        );
    }

    /// A database that refuses every new connection must be asked less and less
    /// often. Without the backoff being told about a failed re-subscription, the
    /// loop stays due on every tick and hammers it once a second forever.
    #[tokio::test(start_paused = true)]
    async fn a_subscribe_that_keeps_failing_is_retried_less_and_less_often() {
        let queue = MockQueue::failing_first(usize::MAX);
        let (_tx, mut rx) = watch::channel(false);
        let mut subscription: Subscription<RecordingNotifier> = Subscription {
            listener: None,
            warn_pacer: WarnPacer::default(),
            backoff: ResubscribeBackoff::default(),
        };

        for tick in 0..5 {
            assert!(
                subscription
                    .next_tick(&queue, &mut rx, Duration::from_secs(1))
                    .await,
                "the loop should still be running on tick {tick}"
            );
        }
        assert_eq!(
            queue.subscribe_attempts(),
            1,
            "a failed attempt should hold off the next one, not retry every tick"
        );
        assert_eq!(
            subscription.backoff.delay(),
            RESUBSCRIBE_BASE_DELAY,
            "one failed attempt should schedule the shortest wait"
        );

        // Stand in for the base delay elapsing, which a paused tokio clock does
        // not do for the `std::time::Instant` the backoff reads.
        subscription.backoff.next_attempt = None;
        assert!(
            subscription
                .next_tick(&queue, &mut rx, Duration::from_secs(1))
                .await
        );
        assert_eq!(queue.subscribe_attempts(), 2);
        assert_eq!(
            subscription.backoff.delay(),
            RESUBSCRIBE_BASE_DELAY * 2,
            "a second failed attempt should double the wait"
        );
    }

    /// A listener stub that never notifies (its future stays pending).
    struct SilentNotifier;

    #[async_trait]
    impl JobNotifications for SilentNotifier {
        async fn wait_for_notification(&mut self) -> anyhow::Result<()> {
            future::pending().await
        }
    }

    /// A listener error must degrade to poll-only (drop the listener) and return
    /// `Poll`, never panic or stop. This is the regression test for the worker
    /// `panic!` that turned a recoverable listener fault into a silent total
    /// stall.
    #[tokio::test]
    async fn listener_error_degrades_to_poll_without_panicking() {
        // Hold the sender so the stop branch stays pending.
        let (_tx, mut rx) = watch::channel(false);
        let mut listener = Some(FailingNotifier);

        let tick = await_next_tick(
            &mut rx,
            &mut listener,
            std::time::Duration::from_secs(60),
            &mut WarnPacer::default(),
        )
        .await;

        assert_eq!(
            tick,
            Tick::Poll {
                listener_failed: true
            },
            "a listener error should still poll, and report the failure"
        );
        assert!(
            listener.is_none(),
            "a listener error should degrade to poll-only (listener dropped)"
        );
    }

    /// The pacing rule that keeps a listener which fails instantly from
    /// spinning the loop: re-subscribe only once a whole tick has passed since
    /// the failure, so the poll-period sleep sits between attempts.
    #[test]
    fn resubscribes_only_after_a_paced_tick() {
        let idle = ResubscribeBackoff::default();
        let now = Instant::now();

        assert!(
            !should_resubscribe(true, false, &idle, now),
            "a healthy listener needs no re-subscription"
        );
        assert!(
            !should_resubscribe(false, true, &idle, now),
            "the tick a listener fails on must not re-subscribe immediately"
        );
        assert!(
            should_resubscribe(false, false, &idle, now),
            "a listener that degraded on an earlier tick should be retried"
        );
    }

    /// The one-tick deferral still holds while a backoff is running, so the two
    /// rules cannot combine into an immediate retry.
    #[test]
    fn a_running_backoff_holds_off_re_subscription() {
        let mut backoff = ResubscribeBackoff::default();
        let start = Instant::now();
        backoff.record_failure(start);

        assert!(
            !should_resubscribe(false, false, &backoff, start),
            "an attempt that just failed must wait out its delay"
        );
        assert!(
            should_resubscribe(false, false, &backoff, start + RESUBSCRIBE_BASE_DELAY),
            "once the delay has passed the loop should try again"
        );
    }

    /// Consecutive failures double the wait up to the ceiling, so a database
    /// that cannot hold the subscription is asked less and less often.
    #[test]
    fn repeated_failures_back_off_up_to_the_ceiling() {
        let mut backoff = ResubscribeBackoff::default();
        let start = Instant::now();

        for expected in [1u64, 2, 4, 8, 16, 30, 30] {
            backoff.record_failure(start);
            assert_eq!(
                backoff.delay(),
                Duration::from_secs(expected),
                "unexpected delay after {} failures",
                backoff.failures
            );
        }
    }

    /// Job polling is never delayed by the backoff: the ceiling only spaces out
    /// subscription attempts, and the poll period stays where it was.
    #[test]
    fn the_backoff_never_slows_job_polling() {
        assert_eq!(DEFAULT_QUEUE_POLL_PERIOD, Duration::from_secs(1));
        assert!(RESUBSCRIBE_MAX_DELAY > DEFAULT_QUEUE_POLL_PERIOD);
    }

    /// A subscription that held for a good while is not part of the previous
    /// run of failures, so losing it starts the delay from the bottom again.
    #[test]
    fn a_long_lived_subscription_resets_the_backoff() {
        let mut backoff = ResubscribeBackoff::default();
        let start = Instant::now();

        backoff.record_failure(start);
        backoff.record_failure(start);
        assert_eq!(backoff.delay(), Duration::from_secs(2));

        backoff.record_success(start);
        backoff.record_failure(start + RESUBSCRIBE_STABLE_PERIOD);
        assert_eq!(
            backoff.delay(),
            RESUBSCRIBE_BASE_DELAY,
            "a subscription that proved itself should not inherit old failures"
        );
    }

    /// A subscription that drops straight back out keeps escalating, which is
    /// the flap the backoff exists to slow down.
    #[test]
    fn a_flapping_subscription_keeps_escalating() {
        let mut backoff = ResubscribeBackoff::default();
        let start = Instant::now();

        backoff.record_failure(start);
        backoff.record_success(start);
        backoff.record_failure(start + Duration::from_millis(10));

        assert_eq!(
            backoff.delay(),
            Duration::from_secs(2),
            "a subscription that dies immediately is the same problem continuing"
        );
    }

    /// An outage warns once, then stays quiet until the cooldown expires, so a
    /// fault that never recovers keeps resurfacing without one line per retry.
    #[test]
    fn a_continuing_outage_warns_on_a_cooldown() {
        let mut pacer = WarnPacer::default();
        let start = Instant::now();

        assert_eq!(
            pacer.record_failure(start),
            Some(1),
            "the first failure is what an operator needs to see"
        );
        assert_eq!(
            pacer.record_failure(start + Duration::from_secs(1)),
            None,
            "a continuing fault should not warn every cycle"
        );
        assert_eq!(
            pacer.record_failure(start + WARN_COOLDOWN - Duration::from_secs(1)),
            None
        );
        assert_eq!(
            pacer.record_failure(start + WARN_COOLDOWN),
            Some(3),
            "a fault that persists should resurface, counting the quiet cycles"
        );
    }

    /// The warning carries how many failures it stands for, so a steady flap
    /// reads as the dozens of cycles it is rather than one recurring blip.
    #[test]
    fn a_warning_reports_the_failures_it_covers() {
        let mut pacer = WarnPacer::default();
        let start = Instant::now();

        assert_eq!(pacer.record_failure(start), Some(1));
        for i in 1..50 {
            assert_eq!(pacer.record_failure(start + Duration::from_millis(i)), None);
        }

        assert_eq!(
            pacer.record_failure(start + WARN_COOLDOWN),
            Some(50),
            "the 49 quiet failures plus this one should all be accounted for"
        );
        assert_eq!(
            pacer.record_failure(start + WARN_COOLDOWN * 2),
            Some(1),
            "the count starts again from the last warning, not from the start"
        );
    }

    /// The recovery message says how many attempts failed first, so a listener
    /// that took 40 tries to come back does not read like a clean reconnect.
    #[test]
    fn a_recovery_reports_the_attempts_it_took() {
        let mut pacer = WarnPacer::default();
        let start = Instant::now();

        for i in 0..5 {
            pacer.record_failure(start + Duration::from_millis(i));
        }

        assert_eq!(
            pacer.take_announced(),
            Some(5),
            "every failure since the last success should be reported"
        );
    }

    /// A connection that recovers between every failure must not win back the
    /// right to warn each cycle, which is how counting failures went wrong.
    #[test]
    fn recovering_between_failures_does_not_reset_the_cooldown() {
        let mut pacer = WarnPacer::default();
        let start = Instant::now();

        assert!(pacer.record_failure(start).is_some());
        assert!(
            pacer.take_announced().is_some(),
            "the warning is outstanding"
        );

        assert_eq!(
            pacer.record_failure(start + Duration::from_secs(2)),
            None,
            "recovering in between must not make the next failure loud"
        );
    }

    /// Every warning gets exactly one resolution, and a flap nobody was told
    /// about stays quiet.
    #[test]
    fn only_an_announced_problem_reports_its_recovery() {
        let mut pacer = WarnPacer::default();
        let start = Instant::now();

        assert_eq!(
            pacer.take_announced(),
            None,
            "nothing was announced, so nothing to resolve"
        );

        pacer.record_failure(start);
        assert_eq!(
            pacer.take_announced(),
            Some(1),
            "the warning deserves an answer"
        );
        assert_eq!(
            pacer.take_announced(),
            None,
            "but only one, not one per re-subscription"
        );
    }

    /// A pending stop signal wins over a silent listener (biased select).
    #[tokio::test]
    async fn stop_signal_wins() {
        let (tx, mut rx) = watch::channel(false);
        tx.send(true).unwrap();
        let mut listener = Some(SilentNotifier);

        let tick = await_next_tick(
            &mut rx,
            &mut listener,
            std::time::Duration::from_secs(60),
            &mut WarnPacer::default(),
        )
        .await;

        assert_eq!(tick, Tick::Stop);
    }

    /// With no listener (already degraded), the poll-interval fallback drives
    /// the loop so job processing continues. Runs on a paused clock so the
    /// result comes from the interval elapsing, not from a real wait.
    #[tokio::test(start_paused = true)]
    async fn poll_only_mode_ticks_on_interval() {
        let (_tx, mut rx) = watch::channel(false);
        let mut listener: Option<SilentNotifier> = None;

        let tick = await_next_tick(
            &mut rx,
            &mut listener,
            std::time::Duration::from_secs(60),
            &mut WarnPacer::default(),
        )
        .await;

        assert_eq!(
            tick,
            Tick::Poll {
                listener_failed: false
            }
        );
    }

    /// A job that hangs past the timeout must be turned into a retryable error
    /// (so it reschedules and the open transaction rolls back), not awaited
    /// indefinitely. Bounded by a test-level timeout so a regression fails fast
    /// instead of hanging.
    #[tokio::test]
    async fn hung_job_times_out_as_retryable() {
        let hung = future::pending::<super::JobResult<()>>();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            run_job_with_timeout(std::time::Duration::from_millis(50), hung),
        )
        .await
        .expect("run_job_with_timeout did not bound the hung job");

        assert!(
            matches!(result, Err(JobError::Retryable(..))),
            "a timed-out job should be retryable, got {result:?}"
        );
    }

    /// A job that completes within the timeout passes its result through
    /// unchanged (including a Fatal classification).
    #[tokio::test]
    async fn fast_job_result_passes_through() {
        let ok = run_job_with_timeout(std::time::Duration::from_secs(60), async { Ok(()) }).await;
        assert!(ok.is_ok());

        let fatal = run_job_with_timeout(std::time::Duration::from_secs(60), async {
            Err(JobError::Fatal(anyhow::anyhow!("boom")))
        })
        .await;
        assert!(matches!(fatal, Err(JobError::Fatal(..))));
    }
}
