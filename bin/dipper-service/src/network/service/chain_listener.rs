//! On-chain agreement state reconciler
//!
//! This service polls a subgraph for changes to indexing agreements and
//! reconciles dipper's local view of each agreement against the on-chain
//! truth. Every poll returns agreements whose `lastStateChangeBlock` has
//! advanced since the last processed block; for each returned snapshot
//! dipper computes the transition from local status to remote state and
//! applies it.
//!
//! ## Transitions
//!
//! | Local status       | Remote state        | Action                                                                 |
//! |--------------------|---------------------|------------------------------------------------------------------------|
//! | Created            | Accepted            | mark AcceptedOnChain, run pending cancellations                         |
//! | Created            | CanceledBy*         | mark AcceptedOnChain, then canceled; drop its pending cancellations    |
//! | AcceptedOnChain    | Accepted            | no-op                                                                  |
//! | AcceptedOnChain    | CanceledBy*         | mark canceled (requester vs indexer from the CanceledBy* state)        |
//! | Expired            | Accepted            | mark AcceptedOnChain (recovery), run pending cancellations              |
//! | Expired            | CanceledBy*         | recover to AcceptedOnChain, then cancel; drop its pending cancellations |
//! | Rejected           | Accepted            | queue a chain check, which reopens its cancel if live (adversarial path) |
//! | CanceledByRequester | Accepted           | queue a chain check, which reopens its cancel if live                  |
//! | Canceled*          | *                   | no-op (terminal)                                                       |
//!
//! Recording an indexer's cancel also queues a reassessment of its request, which nothing else
//! would refill before the daily pass.
//!
//! Pending cancellations are the agreements a new one was offered to replace. They are run once
//! the new agreement is accepted, and dropped when the poll that shows it accepted also shows it
//! cancelled, so the request keeps the agreements it had.
//!
//! ## Data Source
//!
//! Snapshots come from the indexing-payments-subgraph's aggregated
//! `IndexingAgreement` entity (see [`AgreementStateSnapshot`]). The subgraph
//! exposes `lastStateChangeBlock` for cursor pagination and the aggregated
//! `state` (`CanceledByPayer` / `CanceledByServiceProvider`), which dipper
//! reads to tell payer-initiated cancels (requester) apart from
//! indexer-initiated ones. `canceledBy` is still carried as the canceler
//! address for observability.

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Context as _;
use dipper_core::ids::IndexingAgreementId;
// The concrete emitter is only constructed in tests (`disabled()`); the runtime
// paths depend on the `dyn` trait, so keep this import test-gated.
#[cfg(test)]
use dipper_producer::events::SubgraphIndexingAgreementsEventsEmitter;
use dipper_producer::{events::SubgraphIndexingAgreementEventsProducer, proto};
use thegraph_core::alloy::primitives::Address;
use tokio::{
    sync::{Notify, mpsc},
    time::MissedTickBehavior,
};

use super::chain_events::{AgreementState, AgreementStateSnapshot, ChainEventSource, Cursor};
use crate::{
    chain_client::ChainClient,
    config::ChainListenerConfig,
    registry::{
        AgreementRegistry, CancelKind, IndexingAgreement, IndexingAgreementStatus,
        PendingCancellationRegistry, ReconciliationItem, SetAsideChange, SetAsideChangeRegistry,
    },
    worker::service::{ChainAccept, JobPriority, WorkerQueue},
};

/// Idle interval used when no agreement is `Created` or `AcceptedOnChain`.
const SLOW_POLL_INTERVAL: Duration = Duration::from_secs(300);
/// How often the heartbeat info-line fires while idle. ~5 min at the fast
/// rate, every poll at the slow rate.
const HEARTBEAT_POLLS: u64 = 60;
/// After this many back-to-back fetch failures the listener escalates the
/// log line from `warn` to `error`, alerting once until a fetch succeeds.
const MAX_CONSECUTIVE_FAILURES: u32 = 10;

/// How to log a fetch failure, given how many have come back to back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchFailureLog {
    Warn,
    Alert,
    Error,
}

fn fetch_failure_log(consecutive_failures: u32) -> FetchFailureLog {
    match consecutive_failures.cmp(&MAX_CONSECUTIVE_FAILURES) {
        std::cmp::Ordering::Less => FetchFailureLog::Warn,
        std::cmp::Ordering::Equal => FetchFailureLog::Alert,
        std::cmp::Ordering::Greater => FetchFailureLog::Error,
    }
}
/// Base for the exponential failure-backoff sleep applied between ticks
/// while `consecutive_failures > 0`.
const FAILURE_BACKOFF_BASE: Duration = Duration::from_secs(5);
/// Subgraph-stall warn / error escalation: ticks-without-head-advance.
const STALL_WARN_THRESHOLD: u32 = 5;
const STALL_ERROR_THRESHOLD: u32 = 15;
/// Sanity bound on a single tick's drain — not an operating point;
/// steady-state drains are 0–1 pages.
const MAX_PAGES_PER_DRAIN: u32 = 1000;
/// How long the listener keeps re-reading a page whose changes fail to apply before setting them
/// aside and moving past it: long enough to ride out a database failover, short enough that a
/// change that can never apply doesn't hold up every later accept and cancel.
const MAX_CURSOR_HOLD: Duration = Duration::from_secs(600);
/// How often the retry of set-aside changes repeats its warning about the same agreements.
const SET_ASIDE_WARNING_INTERVAL: Duration = Duration::from_secs(600);

/// Where the cursor has been held, and since when, before a page whose changes failed to apply.
#[derive(Debug)]
struct CursorHold {
    cursor: Cursor,
    since: std::time::Instant,
}

/// Whether to set aside the changes of a page at `cursor` that failed to apply and move past it:
/// once the cursor has been held before it for [`MAX_CURSOR_HOLD`]. Until then the hold is noted,
/// so the page is read again. The hold lasts until the page is passed, so if setting the changes
/// aside fails, the next poll tries again.
fn hold_expired(hold: &mut Option<CursorHold>, cursor: &Cursor, now: std::time::Instant) -> bool {
    match hold {
        Some(held) if held.cursor == *cursor => {
            now.saturating_duration_since(held.since) >= MAX_CURSOR_HOLD
        }
        _ => {
            *hold = Some(CursorHold {
                cursor: cursor.clone(),
                since: now,
            });
            false
        }
    }
}

/// Most agreement ids a log line lists, so an alert for a whole page that failed stays readable.
const MAX_LOGGED_IDS: usize = 20;

/// Agreement ids in the 0x form operators search for, at most [`MAX_LOGGED_IDS`] of them.
fn listed_ids(ids: &[IndexingAgreementId]) -> String {
    let shown: Vec<String> = ids
        .iter()
        .take(MAX_LOGGED_IDS)
        .map(ToString::to_string)
        .collect();
    let more = ids.len().saturating_sub(MAX_LOGGED_IDS);
    if more == 0 {
        shown.join(", ")
    } else {
        format!("{} and {more} more", shown.join(", "))
    }
}

/// Whether to poll at the fast interval, given the check for any agreement that is offered or
/// accepted. A failed check counts as yes: polling slowly while offers are out would record their
/// acceptances, and the cancellations those trigger, up to [`SLOW_POLL_INTERVAL`] late.
fn wants_fast_polling(active_check: crate::registry::Result<bool>) -> bool {
    match active_check {
        Ok(any_active) => any_active,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "could not check for offered or accepted agreements; polling at the fast interval \
                 until the check works"
            );
            true
        }
    }
}

/// Cap on the cancellation-sweep batch so a backlog drains across polls
/// instead of blocking one tick.
const SWEEP_BATCH_SIZE: i64 = 1000;
/// How often the cancellation sweep runs. The sweep is purely
/// crash-recovery; the steady-state fan-out fires from finalize on a
/// fresh accept, so per-poll execution is wasted DB work.
const SWEEP_POLLS: u64 = 60;
/// Handle for controlling the chain listener service lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the chain listener service gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the chain listener service
pub struct Ctx<R, E, T, W> {
    /// Registry for querying and updating agreements
    pub registry: R,
    /// Chain event source (subgraph)
    pub event_source: E,
    /// Chain client used to cancel on-chain via the RecurringAgreementManager
    /// when a replacement agreement is confirmed accepted on-chain.
    pub chain_client: T,
    /// Indexing agreement config (manager address for cancel dispatch).
    pub agreement_conf: Arc<crate::config::IndexingAgreementConfig>,
    /// Service configuration
    pub config: ChainListenerConfig,
    /// Signalled by the worker when proposals are dispatched, waking the listener
    /// from the slow (300s) idle interval so it starts fast-polling immediately.
    pub chain_listener_notify: Arc<Notify>,
    /// Emits `accepted` and `terminated` lifecycle events as on-chain state is reconciled.
    pub subgraph_indexing_agreements_events_emitter:
        Arc<dyn SubgraphIndexingAgreementEventsProducer>,
    /// Queues the chain check of an agreement dipper ended that the subgraph shows accepted.
    pub worker_queue: W,
}

/// State persisted in the database
#[derive(Clone)]
pub struct ChainListenerState {
    pub _chain_id: u64,
    pub last_processed_block: u64,
    /// `id` of the last consumed entity at `last_processed_block`. `None`
    /// means the cursor sits at a block boundary. Stored alongside the
    /// block to support keyset pagination across same-block ties.
    pub last_processed_id: Option<dipper_core::ids::IndexingAgreementId>,
    /// Block timestamp (epoch seconds). Used by the expiration service.
    pub last_processed_block_timestamp: Option<u64>,
}

/// Create a new chain listener service
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service polls the subgraph for agreement state snapshots
/// and reconciles them against the local DB.
#[expect(
    clippy::too_many_lines,
    reason = "predates this lint; fix when next touched"
)]
pub fn new<R, E, T, W>(ctx: Ctx<R, E, T, W>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry
        + ChainListenerStateRegistry
        + PendingCancellationRegistry
        + SetAsideChangeRegistry
        + Send
        + Sync,
    E: ChainEventSource,
    T: ChainClient + Send + Sync,
    W: WorkerQueue + Send + Sync,
    R: crate::registry::IndexingRequestRegistry,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        event_source,
        chain_client,
        agreement_conf,
        config,
        chain_listener_notify,
        subgraph_indexing_agreements_events_emitter,
        worker_queue,
    } = ctx;

    let service = async move {
        tracing::info!(
            poll_interval_secs = config.poll_interval.as_secs(),
            endpoint = %config
                .subgraph_endpoint
                .as_ref()
                .map_or_else(|| "none".to_string(), crate::config::endpoint_name),
            "chain listener service started"
        );

        let chain_id = config.chain_id;
        let reorg_buffer_blocks = config.reorg_buffer_blocks;
        let chain_ts_drift_tolerance_secs = config.chain_ts_drift_tolerance_secs;
        let bypass_chain_clock_defenses = config.bypass_chain_clock_defenses;

        // Get initial state from DB or start at genesis. We track
        // `(block, id)` together so a same-block-tie that crashed mid-page
        // is resumed from the right entity rather than re-played from the
        // block boundary.
        let (mut cursor, mut last_persisted_timestamp) =
            match registry.get_chain_listener_state(chain_id).await {
                Ok(Some(state)) => {
                    tracing::info!(
                        last_processed_block = state.last_processed_block,
                        last_processed_id = ?state.last_processed_id.map(|id| id.to_string()),
                        "Resuming from last processed cursor"
                    );
                    (
                        Cursor {
                            block: state.last_processed_block,
                            id: state.last_processed_id,
                        },
                        state.last_processed_block_timestamp,
                    )
                }
                Ok(None) => {
                    tracing::info!("No previous state, starting from genesis");
                    (Cursor::genesis(), None)
                }
                Err(err) => {
                    tracing::error!(error = %err, "Failed to get chain listener state");
                    return Err(err.into());
                }
            };

        // Adaptive polling: fast (config.poll_interval) while any agreement is
        // Created or AcceptedOnChain, slow (`SLOW_POLL_INTERVAL`) when none is.
        let fast_interval = config.poll_interval;
        let slow_interval = SLOW_POLL_INTERVAL;
        let mut timer = tokio::time::interval(fast_interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut using_fast_interval = true;

        // Track consecutive failures for adaptive backoff
        let mut consecutive_failures: u32 = 0;
        let mut cursor_hold: Option<CursorHold> = None;

        // Observability: heartbeat and stall detection
        let mut polls_since_last_event: u64 = 0;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        // Counts up to SWEEP_POLLS, then trips the cancellation sweep.
        // Starts at SWEEP_POLLS so the first poll runs the sweep,
        // recovering any pre-startup orphans.
        let mut polls_since_sweep: u64 = SWEEP_POLLS;
        // Pause the event sweeps after a Kafka send failure, backing off from one
        // poll interval up to the idle interval, so a hung broker cannot stall the
        // poll loop on every iteration.
        let mut broker_cooldown = BrokerCooldown::new(fast_interval, slow_interval);
        let mut live_checks = LiveChecks::new(worker_queue);
        let mut set_aside_warning = SetAsideWarning::default();

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
                _ = chain_listener_notify.notified() => {
                    // Worker dispatched a proposal -- switch to fast polling immediately
                    if !using_fast_interval {
                        timer = tokio::time::interval(fast_interval);
                        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        timer.tick().await;
                        using_fast_interval = true;
                        tracing::info!(
                            interval_secs = fast_interval.as_secs(),
                            "chain listener woken by proposal dispatch, switching to fast polling"
                        );
                    }
                },
            }

            // Adaptive interval: check if any agreement is Created or AcceptedOnChain
            let want_fast = wants_fast_polling(registry.exists_active_agreements().await);
            if want_fast != using_fast_interval {
                let new_interval = if want_fast {
                    fast_interval
                } else {
                    slow_interval
                };
                timer = tokio::time::interval(new_interval);
                timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
                timer.tick().await; // consume the immediate first tick
                using_fast_interval = want_fast;
                tracing::info!(
                    interval_secs = new_interval.as_secs(),
                    has_pending_agreements = want_fast,
                    "Chain listener polling interval changed"
                );
            }

            // Apply adaptive backoff on consecutive failures
            if consecutive_failures > 0 {
                let backoff = FAILURE_BACKOFF_BASE
                    .saturating_mul(2u32.saturating_pow(consecutive_failures.min(6)));
                tracing::debug!(
                    consecutive_failures,
                    backoff_secs = backoff.as_secs(),
                    "Applying failure backoff"
                );
                tokio::time::sleep(backoff).await;
            }

            if retry_set_aside_changes(
                &registry,
                &event_source,
                &chain_client,
                &mut live_checks,
                &agreement_conf,
                &mut set_aside_warning,
                &mut rx_stop,
            )
            .await
            {
                tracing::debug!("chain listener stopping mid-cycle");
                return Ok(());
            }

            let outcome = match drain_once(
                &mut cursor,
                &mut cursor_hold,
                &mut last_persisted_timestamp,
                &mut last_subgraph_head,
                &mut stall_count,
                &mut consecutive_failures,
                chain_id,
                reorg_buffer_blocks,
                chain_ts_drift_tolerance_secs,
                bypass_chain_clock_defenses,
                &registry,
                &chain_client,
                &mut live_checks,
                &event_source,
                &mut rx_stop,
                &agreement_conf,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(()) => continue,
            };

            if outcome.stopped {
                tracing::debug!("chain listener stopping mid-cycle");
                return Ok(());
            }

            if outcome.processed > 0 || outcome.errors > 0 {
                tracing::info!(
                    pages_per_tick = outcome.pages,
                    drain_duration_ms = outcome.duration_ms,
                    subgraph_lag_seconds = outcome.subgraph_lag_seconds,
                    processed = outcome.processed,
                    errors = outcome.errors,
                    "Reconciled agreement snapshots (drain)"
                );
                polls_since_last_event = 0;
            } else if outcome.latest_block <= cursor.block {
                polls_since_last_event += 1;
                if polls_since_last_event.is_multiple_of(HEARTBEAT_POLLS) || !using_fast_interval {
                    tracing::info!(
                        pages_per_tick = outcome.pages,
                        drain_duration_ms = outcome.duration_ms,
                        subgraph_lag_seconds = outcome.subgraph_lag_seconds,
                        last_processed_block = cursor.block,
                        subgraph_head = outcome.latest_block,
                        polls_idle = polls_since_last_event,
                        fast_mode = using_fast_interval,
                        "chain listener heartbeat"
                    );
                }
            }

            // Announce accepted / terminated / expired every poll, but pause the
            // sweeps during a broker outage: each sweep makes one blocking send
            // that can hang, so unchecked they would stall polling every iteration.
            let now = Instant::now();
            if broker_cooldown.is_paused(now) {
                tracing::debug!(
                    backoff_secs = broker_cooldown.backoff().as_secs(),
                    "event broker in cooldown; skipping event sweeps this poll"
                );
            } else {
                let health = run_event_sweeps(
                    &registry,
                    &agreement_conf,
                    subgraph_indexing_agreements_events_emitter.as_ref(),
                )
                .await;
                match broker_cooldown.record(health, now) {
                    CooldownTransition::Entered => tracing::warn!(
                        backoff_secs = broker_cooldown.backoff().as_secs(),
                        "event broker send failed; pausing event sweeps until backoff elapses"
                    ),
                    CooldownTransition::Extended => tracing::warn!(
                        backoff_secs = broker_cooldown.backoff().as_secs(),
                        "event broker still unreachable; extending event sweep pause"
                    ),
                    CooldownTransition::Recovered => {
                        tracing::info!("event broker recovered; resuming event sweeps")
                    }
                    CooldownTransition::Unchanged => {}
                }
            }

            // The cancellation crash-recovery sweeps stay on the slower cadence:
            // the steady-state fan-out fires from finalize on a fresh accept, so
            // running these every poll is wasted DB work.
            polls_since_sweep += 1;
            if polls_since_sweep >= SWEEP_POLLS {
                sweep_executable_pending_cancellations(&registry, &chain_client, &agreement_conf)
                    .await;
                sweep_orphan_canceled_agreements(&registry, &chain_client, &agreement_conf).await;
                polls_since_sweep = 0;
            }
        }

        tracing::debug!("chain listener service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

struct DrainOutcome {
    pages: u32,
    processed: u64,
    errors: u64,
    latest_block: u64,
    duration_ms: u64,
    subgraph_lag_seconds: i64,
    /// True when `rx_stop` fired mid-drain; the outer loop returns
    /// gracefully rather than starting another tick.
    stopped: bool,
}

/// Run one drain cycle: page through new agreement snapshots from the
/// subgraph, apply reconciliation transitions, persist cursor + timestamp
/// progress per-page, and emit observability fields. Mutates the long-lived
/// state in place; the returned [`DrainOutcome`] carries only stats. `Err(())`
/// signals the subgraph fetch itself failed so the outer loop skips the
/// heartbeat block.
#[allow(clippy::too_many_arguments)]
#[expect(
    clippy::cast_possible_wrap,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    reason = "predates this lint; fix when next touched"
)]
async fn drain_once<R, E, T, W>(
    cursor: &mut Cursor,
    cursor_hold: &mut Option<CursorHold>,
    last_persisted_timestamp: &mut Option<u64>,
    last_subgraph_head: &mut u64,
    stall_count: &mut u32,
    consecutive_failures: &mut u32,
    chain_id: u64,
    reorg_buffer_blocks: u32,
    chain_ts_drift_tolerance_secs: u64,
    bypass_chain_clock_defenses: bool,
    registry: &R,
    chain_client: &T,
    live_checks: &mut LiveChecks<W>,
    event_source: &E,
    rx_stop: &mut mpsc::Receiver<()>,
    config: &crate::config::IndexingAgreementConfig,
) -> Result<DrainOutcome, ()>
where
    R: AgreementRegistry
        + ChainListenerStateRegistry
        + PendingCancellationRegistry
        + SetAsideChangeRegistry
        + Send
        + Sync,
    E: ChainEventSource,
    T: ChainClient + Send + Sync,
    W: WorkerQueue + Sync,
    R: crate::registry::IndexingRequestRegistry,
{
    // Read-side cursor backs off by `reorg_buffer_blocks` so a
    // reorg that moves a state change across the boundary is still
    // re-read. The committed cursor only moves forward; idempotent
    // reconcile makes the duplicate work a no-op. The keyset `id`
    // is intentionally dropped on backoff (the resume must re-read
    // every row in the buffer window, not just the tail of the
    // tied block at `cursor.block`).
    let mut effective_cursor = if cursor.block > 0
        && reorg_buffer_blocks > 0
        && cursor.block > u64::from(reorg_buffer_blocks)
    {
        Cursor::at_block(cursor.block.saturating_sub(u64::from(reorg_buffer_blocks)))
    } else {
        cursor.clone()
    };
    live_checks.forget_before(effective_cursor.block);

    let drain_start = std::time::Instant::now();
    let mut pages_per_tick: u32 = 0;
    let mut pinned_block: Option<u64> = None;
    let mut drain_processed: u64 = 0;
    let mut drain_errors: u64 = 0;
    let mut drain_latest_block: u64;
    let mut drain_latest_timestamp: Option<u64>;
    let mut drain_stopped = false;

    'drain: loop {
        let result = match event_source
            .get_changed_agreements(&effective_cursor, pinned_block)
            .await
        {
            Ok(result) => {
                if *consecutive_failures > 0 {
                    tracing::info!(
                        recovered_after = *consecutive_failures,
                        "chain listener recovered from consecutive fetch failures"
                    );
                }
                *consecutive_failures = 0;
                result
            }
            Err(err) => {
                *consecutive_failures = consecutive_failures.saturating_add(1);
                match fetch_failure_log(*consecutive_failures) {
                    FetchFailureLog::Alert => tracing::error!(
                        event = "chain_listener_fetch_failing",
                        error = %err,
                        consecutive_failures = *consecutive_failures,
                        pages_drained = pages_per_tick,
                        "Chain listener keeps failing to fetch agreement changes from the \
                         subgraph; on-chain accepts and cancels go unrecorded until it recovers"
                    ),
                    FetchFailureLog::Error => tracing::error!(
                        error = %err,
                        consecutive_failures = *consecutive_failures,
                        pages_drained = pages_per_tick,
                        "Too many consecutive failures fetching changed agreements"
                    ),
                    FetchFailureLog::Warn => tracing::warn!(
                        error = %err,
                        consecutive_failures = *consecutive_failures,
                        pages_drained = pages_per_tick,
                        "Failed to fetch changed agreements from subgraph"
                    ),
                }
                return Err(());
            }
        };

        // Pin to page 1's snapshot so later pages in this drain
        // see the same chain state.
        if pinned_block.is_none() {
            pinned_block = Some(result.latest_block);
        }

        drain_latest_block = result.latest_block;
        drain_latest_timestamp = result.latest_block_timestamp;
        let new_cursor = result.cursor.clone();
        let new_timestamp = result.latest_block_timestamp;
        let count = result.snapshots.len();

        // Page 1 is the only meaningful sample for stall detection;
        // pinned pages return the same `latest_block` by construction.
        if pages_per_tick == 0 {
            if result.latest_block == *last_subgraph_head && result.latest_block > 0 {
                *stall_count += 1;
                if *stall_count == STALL_ERROR_THRESHOLD {
                    tracing::error!(
                        subgraph_head = result.latest_block,
                        stall_polls = *stall_count,
                        "Subgraph appears stalled — on-chain events may be delayed"
                    );
                } else if *stall_count == STALL_WARN_THRESHOLD {
                    tracing::warn!(
                        subgraph_head = result.latest_block,
                        stall_polls = *stall_count,
                        "Subgraph has not advanced, may be paused or behind"
                    );
                }
            } else if *stall_count > 0 && result.latest_block > *last_subgraph_head {
                tracing::info!(
                    previous_head = *last_subgraph_head,
                    new_head = result.latest_block,
                    stall_polls = *stall_count,
                    "Subgraph recovered from stall"
                );
                *stall_count = 0;
            }
            *last_subgraph_head = result.latest_block;
        }

        tracing::debug!(
            from_block = effective_cursor.block + 1,
            to_block = result.latest_block,
            snapshot_count = count,
            page = pages_per_tick + 1,
            "Reconciling agreement snapshots (page)"
        );

        // Single-shot batch lookup for every agreement referenced in this
        // page. Snapshots whose id is not in the local registry (e.g.
        // another payer's) drop out as `None` below.
        let snapshot_ids: Vec<IndexingAgreementId> =
            result.snapshots.iter().map(|s| s.agreement_id).collect();
        // Each change's block, kept for setting aside those that fail to apply.
        let page_changes: Vec<SetAsideChange> = result
            .snapshots
            .iter()
            .map(|s| SetAsideChange {
                agreement_id: s.agreement_id,
                block: s.last_state_change_block,
            })
            .collect();
        // Agreements whose change this page failed to apply. The cursor stays before the page
        // until none fail, or they're set aside, as nothing else would look at them again.
        let mut unapplied: Vec<IndexingAgreementId> = Vec::new();
        let mut agreements_by_id =
            match registry.get_indexing_agreements_by_ids(&snapshot_ids).await {
                Ok(m) => m,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        page = pages_per_tick + 1,
                        snapshots = snapshot_ids.len(),
                        "Failed to batch-fetch agreements for page; counting snapshots as errors"
                    );
                    drain_errors += snapshot_ids.len() as u64;
                    unapplied.extend(&snapshot_ids);
                    std::collections::HashMap::new()
                }
            };

        let mut prepared: Vec<PreparedReconciliation> = Vec::with_capacity(count);
        for snapshot in result.snapshots {
            if rx_stop.try_recv().is_ok() {
                drain_stopped = true;
                break 'drain;
            }

            let agreement = agreements_by_id.remove(&snapshot.agreement_id);
            match prepare_reconciliation(&snapshot, agreement, registry, live_checks).await {
                Ok(Some(prep)) => prepared.push(prep),
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        error = format!("{err:#}"),
                        agreement_id = %snapshot.agreement_id,
                        "Failed to prepare reconciliation for snapshot"
                    );
                    drain_errors += 1;
                    unapplied.push(snapshot.agreement_id);
                }
            }
        }

        // On batch error the tx rolls back; the cursor is held so the
        // next tick re-reads these rows. The successful path
        // pre-fills every input id in `outcomes` (with `default()`
        // for rows whose CAS guard didn't match), so a missing id
        // is an unambiguous signal that the whole batch failed.
        let items: Vec<ReconciliationItem> = prepared.iter().map(|p| p.item.clone()).collect();
        let outcomes = if items.is_empty() {
            std::collections::HashMap::new()
        } else {
            match registry.apply_reconciliation_batch(&items).await {
                Ok(o) => o,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        page = pages_per_tick + 1,
                        items = items.len(),
                        "Batched apply_reconciliation failed; counting page items as errors"
                    );
                    drain_errors += items.len() as u64;
                    unapplied.extend(items.iter().map(|item| item.agreement_id));
                    std::collections::HashMap::new()
                }
            }
        };

        let mut queued_reassessments = QueuedReassessments::default();
        for prep in prepared {
            // Items absent from outcomes are batch-failures, already
            // counted as errors above — finalize would double-count.
            let Some(outcome) = outcomes.get(&prep.agreement.id).copied() else {
                continue;
            };
            match finalize_reconciliation(
                &prep,
                outcome,
                registry,
                chain_client,
                config,
                &live_checks.queue,
                &mut queued_reassessments,
            )
            .await
            {
                Ok(()) => drain_processed += 1,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        agreement_id = %prep.agreement.id,
                        "Failed to finalize reconciliation outcome"
                    );
                    drain_errors += 1;
                }
            }
        }

        pages_per_tick += 1;

        // Persisted timestamp never decreases so a rolled-back subgraph
        // can't drag expiration's clock backwards.
        let now_instant = std::time::Instant::now();
        let ratchet_timestamp = match (new_timestamp, *last_persisted_timestamp) {
            (Some(new_ts), Some(cached_ts)) if new_ts < cached_ts => {
                tracing::warn!(
                    new_timestamp_secs = new_ts,
                    persisted_timestamp_secs = cached_ts,
                    "Subgraph reported timestamp below persisted value; ignoring (ratchet up only)"
                );
                Some(cached_ts)
            }
            (Some(new_ts), cached_ts) => Some(apply_chain_ts_drift_cap(
                new_ts,
                cached_ts.unwrap_or(0),
                dipper_core::time::now_secs(),
                chain_ts_drift_tolerance_secs,
                bypass_chain_clock_defenses,
            )),
            (None, _) => *last_persisted_timestamp,
        };

        // Persist per-page so a crash mid-drain replays at most
        // one page. Skip the write when neither cursor nor
        // ratcheted timestamp moved.
        let already_held = cursor_hold
            .as_ref()
            .is_some_and(|hold| hold.cursor == *cursor);
        let hold_over = !unapplied.is_empty() && hold_expired(cursor_hold, cursor, now_instant);
        // Moving past the page drops its changes from the read, so those that failed are recorded
        // first, to be tried again every poll. Until that works, the cursor stays held.
        let skip =
            hold_over && set_aside_unapplied(registry, &page_changes, &unapplied, cursor).await;
        if unapplied.is_empty() || skip {
            *cursor_hold = None;
        }
        let advance_cursor = (unapplied.is_empty() || skip) && *cursor < new_cursor;
        if skip {
            tracing::error!(
                event = "chain_listener_changes_skipped",
                cursor_block = cursor.block,
                agreement_count = unapplied.len(),
                agreement_ids = %listed_ids(&unapplied),
                "Set aside on-chain changes to agreements that kept failing to apply, moving past \
                 them; they're tried again every poll until they apply"
            );
        } else if !unapplied.is_empty() && already_held {
            // The hold started with the tagged ERROR below; a WARN on each poll it goes on, which
            // can be every few seconds for 10 minutes, with no tag so it isn't alerted on again.
            tracing::warn!(
                cursor_block = cursor.block,
                agreement_count = unapplied.len(),
                agreement_ids = %listed_ids(&unapplied),
                "Still failing to apply on-chain changes to agreements; re-reading them next poll"
            );
        } else if !unapplied.is_empty() {
            tracing::error!(
                event = "chain_listener_cursor_held",
                cursor_block = cursor.block,
                agreement_count = unapplied.len(),
                agreement_ids = %listed_ids(&unapplied),
                "Failed to apply on-chain changes to agreements; re-reading them next poll"
            );
        }
        let timestamp_changed = ratchet_timestamp != *last_persisted_timestamp;
        if advance_cursor || timestamp_changed {
            let cursor_to_persist = if advance_cursor {
                &new_cursor
            } else {
                &*cursor
            };
            match registry
                .update_chain_listener_state(chain_id, cursor_to_persist, ratchet_timestamp)
                .await
            {
                Ok(()) => {
                    *last_persisted_timestamp = ratchet_timestamp;
                }
                Err(err) => {
                    tracing::error!(error = %err, "Failed to update chain listener state");
                }
            }
        }
        if advance_cursor {
            *cursor = new_cursor.clone();
            effective_cursor = new_cursor;
        } else if count == 0 {
            // Cursor didn't advance and the page was empty —
            // nothing to do this tick.
            break 'drain;
        } else {
            // Held-back cursor (parse failure, or a change that failed to
            // apply); reconcile already ran on what we did read, retry next tick.
            break 'drain;
        }

        // Drained?
        if count < super::chain_events::SUBGRAPH_PAGE_SIZE {
            break 'drain;
        }

        if pages_per_tick >= MAX_PAGES_PER_DRAIN {
            tracing::warn!(
                pages_per_tick,
                max_pages = MAX_PAGES_PER_DRAIN,
                "Drain hit per-tick page ceiling; deferring remaining snapshots to next tick"
            );
            break 'drain;
        }
    }

    let duration_ms: u64 = drain_start
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);

    // Positive = subgraph trailing wall clock; growing = falling
    // behind. Sourced from `_meta.block.timestamp`.
    let subgraph_lag_seconds: i64 = drain_latest_timestamp
        .map(|ts| (dipper_core::time::now_secs() as i64).saturating_sub(ts as i64))
        .unwrap_or(0);

    Ok(DrainOutcome {
        pages: pages_per_tick,
        processed: drain_processed,
        errors: drain_errors,
        latest_block: drain_latest_block,
        duration_ms,
        subgraph_lag_seconds,
        stopped: drain_stopped,
    })
}

/// Record the changes on a page that failed to apply, at the block of each, so the cursor can
/// move past the page and they're still tried again. False, with the reason logged, if recording
/// failed.
async fn set_aside_unapplied<R: SetAsideChangeRegistry + Sync>(
    registry: &R,
    page_changes: &[SetAsideChange],
    unapplied: &[IndexingAgreementId],
    cursor: &Cursor,
) -> bool {
    let unapplied_ids: std::collections::HashSet<&IndexingAgreementId> = unapplied.iter().collect();
    let changes: Vec<SetAsideChange> = page_changes
        .iter()
        .filter(|change| unapplied_ids.contains(&change.agreement_id))
        .copied()
        .collect();
    match registry.record_set_aside_changes(&changes).await {
        Ok(()) => true,
        Err(err) => {
            tracing::error!(
                error = %err,
                cursor_block = cursor.block,
                agreement_count = unapplied.len(),
                agreement_ids = %listed_ids(unapplied),
                "Failed to set aside on-chain changes to agreements that keep failing to apply; \
                 holding the cursor before them so they aren't dropped"
            );
            false
        }
    }
}

/// The set-aside changes the last warning named as still failing, and when, so the warning
/// repeats only when they change or every [`SET_ASIDE_WARNING_INTERVAL`].
#[derive(Debug, Default)]
struct SetAsideWarning {
    failing: Vec<IndexingAgreementId>,
    warned_at: Option<Instant>,
}

impl SetAsideWarning {
    /// Whether to warn at `now` that `failing`, sorted, still fail to apply. None failing ends
    /// the run, so the next failure warns at once.
    fn due(&mut self, failing: &[IndexingAgreementId], now: Instant) -> bool {
        if failing.is_empty() {
            *self = Self::default();
            return false;
        }
        let repeat = self.failing == failing
            && self
                .warned_at
                .is_some_and(|at| now.saturating_duration_since(at) < SET_ASIDE_WARNING_INTERVAL);
        if repeat {
            return false;
        }
        self.failing = failing.to_vec();
        self.warned_at = Some(now);
        true
    }
}

/// Try again the on-chain changes set aside after they kept failing to apply, reading each
/// agreement's current state from the subgraph and applying it 1 agreement at a time, so 1 that
/// fails can't hold back the rest. An agreement leaves the record once its change applied or
/// there was nothing to apply. Returns true if the listener was told to stop part way.
async fn retry_set_aside_changes<R, E, T, W>(
    registry: &R,
    event_source: &E,
    chain_client: &T,
    live_checks: &mut LiveChecks<W>,
    config: &crate::config::IndexingAgreementConfig,
    warning: &mut SetAsideWarning,
    rx_stop: &mut mpsc::Receiver<()>,
) -> bool
where
    R: AgreementRegistry
        + PendingCancellationRegistry
        + SetAsideChangeRegistry
        + crate::registry::IndexingRequestRegistry
        + Sync,
    E: ChainEventSource,
    T: ChainClient + Sync,
    W: WorkerQueue + Sync,
{
    let set_aside = match registry.get_set_aside_changes().await {
        Ok(set_aside) => set_aside,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to read the on-chain changes set aside to try again; trying next poll"
            );
            return false;
        }
    };
    if set_aside.is_empty() {
        *warning = SetAsideWarning::default();
        return false;
    }

    let mut finished = Vec::new();
    let mut failing = Vec::new();
    let mut stopped = false;
    match read_set_aside(registry, event_source, &set_aside).await {
        Err(err) => {
            let reason = format!("{err:#}");
            failing.extend(set_aside.iter().map(|c| (c.agreement_id, reason.clone())));
        }
        Ok((read, mut agreements)) => {
            for change in &set_aside {
                if rx_stop.try_recv().is_ok() {
                    stopped = true;
                    break;
                }
                let agreement = agreements.remove(&change.agreement_id);
                match apply_set_aside_change(
                    change,
                    read.state_of(change),
                    agreement,
                    registry,
                    chain_client,
                    live_checks,
                    config,
                )
                .await
                {
                    Ok(()) => finished.push(change.agreement_id),
                    Err(err) => failing.push((change.agreement_id, format!("{err:#}"))),
                }
            }
        }
    }

    forget_finished_set_aside(registry, &finished).await;
    if !stopped {
        warn_still_failing_set_aside(warning, &failing);
    }
    stopped
}

/// What a read of the subgraph showed for the set-aside agreements, and the block it was read at.
struct SetAsideRead {
    snapshots: std::collections::HashMap<IndexingAgreementId, AgreementStateSnapshot>,
    unreadable: std::collections::HashSet<IndexingAgreementId>,
    block: u64,
}

/// What the subgraph showed for 1 set-aside agreement.
enum SetAsideState<'a> {
    /// The subgraph hadn't yet indexed the block of the change, so it can't show it.
    Behind { indexed: u64 },
    /// The agreement's state came back in a form that couldn't be read.
    Unreadable,
    /// The subgraph, indexed past the change, has no state for the agreement.
    Gone,
    /// The agreement's state as the subgraph, indexed past the change, has it.
    Readable(&'a AgreementStateSnapshot),
}

impl SetAsideRead {
    fn state_of(&self, change: &SetAsideChange) -> SetAsideState<'_> {
        if self.block < change.block {
            SetAsideState::Behind {
                indexed: self.block,
            }
        } else if let Some(snapshot) = self.snapshots.get(&change.agreement_id) {
            SetAsideState::Readable(snapshot)
        } else if self.unreadable.contains(&change.agreement_id) {
            SetAsideState::Unreadable
        } else {
            SetAsideState::Gone
        }
    }
}

/// Read each set-aside agreement's current state from the subgraph and its row from the
/// registry, by agreement. Either failing leaves every change to try again.
async fn read_set_aside<R, E>(
    registry: &R,
    event_source: &E,
    set_aside: &[SetAsideChange],
) -> anyhow::Result<(
    SetAsideRead,
    std::collections::HashMap<IndexingAgreementId, IndexingAgreement>,
)>
where
    R: AgreementRegistry + Sync,
    E: ChainEventSource,
{
    let ids: Vec<IndexingAgreementId> = set_aside.iter().map(|c| c.agreement_id).collect();
    let found = event_source
        .get_agreement_snapshots(&ids)
        .await
        .context("failed to read their current state from the subgraph")?;
    let agreements = registry
        .get_indexing_agreements_by_ids(&ids)
        .await
        .context("failed to read the agreements from the database")?;
    let read = SetAsideRead {
        snapshots: found
            .snapshots
            .into_iter()
            .map(|snapshot| (snapshot.agreement_id, snapshot))
            .collect(),
        unreadable: found.unreadable.into_iter().collect(),
        block: found.block,
    };
    Ok((read, agreements))
}

/// Apply the current state of 1 agreement whose change was set aside, once the subgraph has
/// indexed past the change: what it shows then is the chain as it is, even if a reorg or a change
/// of the payer address took the change away. Ok once the state applied, there was nothing to
/// apply, or the subgraph has no state for the agreement.
async fn apply_set_aside_change<R, T, W>(
    change: &SetAsideChange,
    state: SetAsideState<'_>,
    agreement: Option<IndexingAgreement>,
    registry: &R,
    chain_client: &T,
    live_checks: &mut LiveChecks<W>,
    config: &crate::config::IndexingAgreementConfig,
) -> anyhow::Result<()>
where
    R: AgreementRegistry
        + PendingCancellationRegistry
        + crate::registry::IndexingRequestRegistry
        + Sync,
    T: ChainClient + Sync,
    W: WorkerQueue + Sync,
{
    let snapshot = match state {
        SetAsideState::Behind { indexed } => anyhow::bail!(
            "the subgraph has indexed up to block {indexed}, before the change at {}",
            change.block
        ),
        SetAsideState::Unreadable => {
            anyhow::bail!(
                "the subgraph returned the agreement's state in a form that can't be read"
            )
        }
        SetAsideState::Gone => {
            warn_set_aside_change_gone(change, None);
            return Ok(());
        }
        SetAsideState::Readable(snapshot) => snapshot,
    };
    apply_snapshot(
        snapshot,
        agreement,
        registry,
        chain_client,
        live_checks,
        config,
    )
    .await?;
    // Warned only once the state applied, as the change then leaves the record.
    if snapshot.last_state_change_block < change.block {
        warn_set_aside_change_gone(change, Some(snapshot.last_state_change_block));
    }
    Ok(())
}

/// Apply 1 agreement's state with the same steps as a page, on its own.
async fn apply_snapshot<R, T, W>(
    snapshot: &AgreementStateSnapshot,
    agreement: Option<IndexingAgreement>,
    registry: &R,
    chain_client: &T,
    live_checks: &mut LiveChecks<W>,
    config: &crate::config::IndexingAgreementConfig,
) -> anyhow::Result<()>
where
    R: AgreementRegistry
        + PendingCancellationRegistry
        + crate::registry::IndexingRequestRegistry
        + Sync,
    T: ChainClient + Sync,
    W: WorkerQueue + Sync,
{
    let Some(prep) = prepare_reconciliation(snapshot, agreement, registry, live_checks).await?
    else {
        return Ok(());
    };
    let outcomes = registry
        .apply_reconciliation_batch(std::slice::from_ref(&prep.item))
        .await
        .context("failed to apply the change")?;
    let outcome = outcomes
        .get(&prep.agreement.id)
        .copied()
        .context("applying the change returned no outcome for the agreement")?;
    // The change is applied, as when a page's finalize fails; the pending cancellation sweep
    // finishes what this leaves.
    // A fresh listing for each change: the retry applies them 1 at a time, so a reassessment
    // listed as waiting for one could start before the next is applied.
    if let Err(err) = finalize_reconciliation(
        &prep,
        outcome,
        registry,
        chain_client,
        config,
        &live_checks.queue,
        &mut QueuedReassessments::default(),
    )
    .await
    {
        tracing::warn!(
            error = %err,
            agreement_id = %prep.agreement.id,
            "Failed to finalize reconciliation outcome"
        );
    }
    Ok(())
}

/// Warn that a set-aside change is no longer on the chain as the subgraph, indexed past it, has
/// it, so it's no longer tried: the agreement's latest change there is older, or there is none.
fn warn_set_aside_change_gone(change: &SetAsideChange, latest_change_block: Option<u64>) {
    tracing::warn!(
        event = "chain_listener_set_aside_change_gone",
        agreement_id = %change.agreement_id,
        change_block = change.block,
        latest_change_block = ?latest_change_block,
        "On-chain change set aside at this block is no longer on the chain as the subgraph has it, \
         as after a reorg or a change of payer address; recorded the agreement as the subgraph has \
         it now, if at all, and no longer trying the change"
    );
}

/// Remove from the record the set-aside changes that applied or had nothing left to apply.
async fn forget_finished_set_aside<R: SetAsideChangeRegistry + Sync>(
    registry: &R,
    finished: &[IndexingAgreementId],
) {
    if finished.is_empty() {
        return;
    }
    match registry.delete_set_aside_changes(finished).await {
        Ok(()) => tracing::info!(
            agreement_count = finished.len(),
            agreement_ids = %listed_ids(finished),
            "Applied on-chain changes set aside earlier, or found nothing left to apply"
        ),
        Err(err) => tracing::warn!(
            error = %err,
            agreement_count = finished.len(),
            agreement_ids = %listed_ids(finished),
            "Finished with on-chain changes set aside earlier but failed to remove them from the \
             record; trying them again next poll, which applies nothing new"
        ),
    }
}

/// Warn that set-aside changes still fail to apply, only when the agreements failing differ from
/// the last warning's or every [`SET_ASIDE_WARNING_INTERVAL`], as this runs every poll.
fn warn_still_failing_set_aside(
    warning: &mut SetAsideWarning,
    failing: &[(IndexingAgreementId, String)],
) {
    for (agreement_id, reason) in failing {
        tracing::debug!(
            agreement_id = %agreement_id,
            error = reason,
            "Set-aside on-chain change still fails to apply"
        );
    }
    let mut ids: Vec<IndexingAgreementId> = failing.iter().map(|(id, _)| *id).collect();
    ids.sort();
    if !warning.due(&ids, Instant::now()) {
        return;
    }
    tracing::warn!(
        agreement_count = ids.len(),
        agreement_ids = %listed_ids(&ids),
        error = failing.first().map(|(_, reason)| reason.as_str()).unwrap_or_default(),
        "On-chain changes set aside after failing to apply still fail; trying them again every poll"
    );
}

/// Decision computed for one snapshot, plus the resolved agreement and
/// indexer address that the post-apply log/fan-out paths need.
struct PreparedReconciliation {
    item: ReconciliationItem,
    agreement: IndexingAgreement,
    /// Address that appears in the cancel log line. The accept/cancel audit is
    /// carried on `item.audit` and persisted with the transition; the `accepted`
    /// and `terminated` events are emitted later by their sweeps.
    indexer: Address,
}

/// Hold expiration's clock to `now_secs + chain_ts_drift_tolerance_secs`, unless `bypass` is set
/// for a local network that jumps chain time ahead. Bounding by the wall clock rather than the
/// last persisted value lets a clock left behind by downtime catch up in 1 poll.
fn apply_chain_ts_drift_cap(
    new_ts: u64,
    cached_ts: u64,
    now_secs: u64,
    chain_ts_drift_tolerance_secs: u64,
    bypass: bool,
) -> u64 {
    if bypass {
        return new_ts;
    }
    let max_allowed = now_secs.saturating_add(chain_ts_drift_tolerance_secs);
    if new_ts > max_allowed {
        tracing::warn!(
            event = "chain_ts_drift_capped",
            new_timestamp_secs = new_ts,
            persisted_timestamp_secs = cached_ts,
            max_allowed_secs = max_allowed,
            "Chain timestamp is ahead of the wall clock; capping"
        );
        max_allowed.max(cached_ts)
    } else {
        new_ts
    }
}

/// Compute the apply decision for one snapshot and run any side effects
/// that don't belong inside the apply transaction. Returns `None` when
/// no DB write is needed. Caller pre-fetches the agreement (in batch for
/// the chain_listener loop, single-row for tests) and passes `None`
/// when the snapshot is for an agreement we don't track locally.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn prepare_reconciliation<R, W>(
    snapshot: &AgreementStateSnapshot,
    agreement: Option<IndexingAgreement>,
    registry: &R,
    live_checks: &mut LiveChecks<W>,
) -> anyhow::Result<Option<PreparedReconciliation>>
where
    R: AgreementRegistry + Sync,
    W: WorkerQueue + Sync,
{
    tracing::debug!(
        agreement_id = %snapshot.agreement_id,
        indexer = %snapshot.indexer,
        state = ?snapshot.state,
        canceled_by = %snapshot.canceled_by,
        last_state_change_block = snapshot.last_state_change_block,
        "Preparing reconciliation against snapshot"
    );

    let agreement = match agreement {
        Some(a) => a,
        None => {
            tracing::debug!(
                agreement_id = %snapshot.agreement_id,
                "Agreement not found (may be from another payer)"
            );
            return Ok(None);
        }
    };

    // Defensive guard against a state proposal-first dispatch makes
    // impossible: dipper gates `offer()` submission on local status, and
    // the contract requires an offer before allowing on-chain accept, so
    // a Rejected agreement should never reach Accepted. The `warn!` below
    // is therefore an alarm — investigate the offer-submission path
    // (race with gRPC rejection or stale read), not the indexer.
    // Skipped when remote is already canceled because the cancel tx
    // would revert against canceled contract state.
    if ended_but_accepted(snapshot, &agreement) {
        if agreement.status == IndexingAgreementStatus::Rejected {
            tracing::warn!(
                agreement_id = %agreement.id,
                indexer = %snapshot.indexer,
                "Rejected agreement accepted on-chain, checking whether to cancel it"
            );
        }
        live_checks.queue(snapshot).await?;
    }
    record_accept_of_cancelling(snapshot, &agreement, registry).await?;

    // Both transitions are applied atomically downstream so the
    // Accept-then-Cancel-in-one-snapshot path can't leak an intermediate
    // AcceptedOnChain to concurrent readers.
    // A withdrawn offer reads as cancelled with no accept time: it never went live.
    let withdrawn_offer = snapshot.state.is_canceled() && snapshot.accepted_at == 0;
    let apply_accept = matches!(
        agreement.status,
        IndexingAgreementStatus::Created | IndexingAgreementStatus::Expired,
    ) && snapshot.state.reached_accepted()
        && !withdrawn_offer;

    let already_terminal_cancel = matches!(
        agreement.status,
        IndexingAgreementStatus::CanceledByRequester
            | IndexingAgreementStatus::CanceledByIndexer
            | IndexingAgreementStatus::AbandonedByIndexer,
    );
    // Classify off the on-chain state, which carries the contract's own
    // "canceled by" flag. The canceler address is the payer, not dipper's
    // signer, so comparing it against the signer misreads payer cancels.
    let cancel_kind = if snapshot.state.is_canceled() && !already_terminal_cancel {
        match snapshot.state {
            AgreementState::CanceledByPayer => Some(CancelKind::ByRequester),
            AgreementState::CanceledByServiceProvider => Some(CancelKind::ByIndexer),
            AgreementState::NotAccepted | AgreementState::Accepted => None,
        }
    } else {
        None
    };

    if apply_accept || cancel_kind.is_some() {
        Ok(Some(PreparedReconciliation {
            item: ReconciliationItem {
                agreement_id: agreement.id,
                apply_accept,
                cancel: cancel_kind,
                // Capture the observed on-chain payload for the transition(s)
                // this item applies, so it is persisted with the status change
                // and a later emission sweep can rebuild the event from the row.
                audit: crate::registry::ReconciliationAudit {
                    accepted_at: apply_accept.then_some(snapshot.accepted_at),
                    accepted_tx: apply_accept.then(|| snapshot.accepted_tx.clone()),
                    canceled_at: cancel_kind.map(|_| snapshot.canceled_at),
                    canceled_by: cancel_kind.map(|_| snapshot.canceled_by.to_string()),
                    canceled_tx: cancel_kind.map(|_| snapshot.canceled_tx.clone()),
                },
            },
            agreement,
            indexer: snapshot.indexer,
        }))
    } else {
        if already_terminal_cancel && snapshot.state.is_canceled() {
            tracing::debug!(
                agreement_id = %agreement.id,
                status = %agreement.status,
                "Agreement already canceled, recording any accept dipper missed"
            );
            record_accept_and_cancel_from_chain(snapshot, &agreement, registry).await?;
        }
        Ok(None)
    }
}

/// Record the accept and end of an agreement dipper already ended that went live on-chain, in 1
/// write, as the terminated sweep waits only for the accept. What is on record wins, unless its end
/// predates the accept or has no transaction. The end is its last change, so a failure is returned.
async fn record_accept_and_cancel_from_chain<R: AgreementRegistry + Sync>(
    snapshot: &AgreementStateSnapshot,
    agreement: &IndexingAgreement,
    registry: &R,
) -> anyhow::Result<()> {
    if snapshot.accepted_at == 0 {
        return Ok(());
    }
    registry
        .record_accept_and_cancel_audit(
            &agreement.id,
            snapshot.accepted_at,
            &snapshot.accepted_tx,
            snapshot.canceled_at,
            &snapshot.canceled_by.to_string(),
            Some(&snapshot.canceled_tx),
        )
        .await
        .context("failed to record the on-chain accept and cancel of a cancelled agreement")
}

/// Whether dipper rejected or cancelled an agreement the subgraph shows accepted and not ended,
/// such as an offer that landed after dipper's cancel. Nothing else would end it, as
/// reconciliation ignores an accept on an ended row, so a job checks the chain for it.
fn ended_but_accepted(snapshot: &AgreementStateSnapshot, agreement: &IndexingAgreement) -> bool {
    matches!(
        agreement.status,
        IndexingAgreementStatus::Rejected | IndexingAgreementStatus::CanceledByRequester
    ) && snapshot.state.reached_accepted()
        && !snapshot.state.is_canceled()
}

/// Queues the chain check of an agreement dipper ended that the subgraph shows accepted, once
/// per change, as the reorg buffer and a held cursor read a change again. The job runs off the
/// drain, so a slow chain read can't hold up the rest of the page.
struct LiveChecks<W> {
    queue: W,
    /// The changes checks were queued for, by agreement and block.
    queued: std::collections::HashSet<(IndexingAgreementId, u64)>,
}

impl<W: WorkerQueue + Sync> LiveChecks<W> {
    fn new(queue: W) -> Self {
        Self {
            queue,
            queued: std::collections::HashSet::new(),
        }
    }

    /// Queue the check for this change, unless it is already queued.
    async fn queue(&mut self, snapshot: &AgreementStateSnapshot) -> anyhow::Result<()> {
        let change = (snapshot.agreement_id, snapshot.last_state_change_block);
        if self.queued.contains(&change) {
            tracing::debug!(
                agreement_id = %snapshot.agreement_id,
                "On-chain check already queued for this change"
            );
            return Ok(());
        }
        let accept = (snapshot.accepted_at != 0).then(|| ChainAccept {
            at: snapshot.accepted_at,
            tx: snapshot.accepted_tx.clone(),
        });
        self.queue
            .cancel_rejected_agreement_on_chain(
                snapshot.agreement_id,
                accept,
                JobPriority::Background,
            )
            .await
            .context("failed to queue the on-chain check of an agreement dipper ended")?;
        self.queued.insert(change);
        Ok(())
    }

    /// Forget the changes before `block`, which the drain no longer reads.
    fn forget_before(&mut self, block: u64) {
        self.queued.retain(|&(_, changed_at)| changed_at >= block);
    }
}

/// An agreement dipper is cancelling stays `Cancelling` when the chain shows it accepted, so its
/// accept is recorded here, to be announced with its end once the cancel lands. A withdrawn offer
/// reads as cancelled with no accept time.
async fn record_accept_of_cancelling<R: AgreementRegistry + Sync>(
    snapshot: &AgreementStateSnapshot,
    agreement: &IndexingAgreement,
    registry: &R,
) -> anyhow::Result<()> {
    if agreement.status != IndexingAgreementStatus::Cancelling
        || !snapshot.state.reached_accepted()
        || snapshot.accepted_at == 0
    {
        return Ok(());
    }
    registry
        .record_accepted_audit(&agreement.id, snapshot.accepted_at, &snapshot.accepted_tx)
        .await
        .context("failed to record the on-chain accept of an agreement being cancelled")
}

/// Log the transition that landed, fan out the linked pending cancellations on a fresh accept
/// (or drop them when the same change also cancelled the agreement), and queue a reassessment of
/// the request on an indexer's cancel. Every path that applies a change, from a page or from the
/// retry of set-aside changes, finishes here, so none can miss a step.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn finalize_reconciliation<R, T, W>(
    prep: &PreparedReconciliation,
    outcome: crate::registry::ReconciliationOutcome,
    registry: &R,
    chain_client: &T,
    config: &crate::config::IndexingAgreementConfig,
    worker_queue: &W,
    queued_reassessments: &mut QueuedReassessments,
) -> anyhow::Result<()>
where
    R: AgreementRegistry
        + PendingCancellationRegistry
        + crate::registry::IndexingRequestRegistry
        + Sync,
    T: ChainClient,
    W: WorkerQueue + Sync,
{
    if outcome.did_accept {
        let (old_status, reason) = match prep.agreement.status {
            IndexingAgreementStatus::Expired => ("EXPIRED", "recovered_expired_on_chain"),
            _ => ("CREATED", "accepted_on_chain"),
        };
        tracing::info!(
            agreement_id = %prep.agreement.id,
            indexing_request_id = %prep.agreement.indexing_request_id,
            old_status,
            new_status = "ACCEPTED_ON_CHAIN",
            reason,
            "agreement state transition"
        );

        // The `accepted` event is NOT emitted here. `apply_reconciliation`
        // persisted the accept audit (accepted_at/tx) in the same transaction as
        // this transition; `sweep_pending_accepted_events` is the single
        // authoritative emitter and will announce it durably.
    }

    if outcome.did_cancel {
        match prep.item.cancel {
            Some(CancelKind::ByRequester) => tracing::info!(
                agreement_id = %prep.agreement.id,
                "Agreement marked as ended by dipper (on-chain confirmation)"
            ),
            Some(CancelKind::ByIndexer) => tracing::info!(
                agreement_id = %prep.agreement.id,
                indexer = %prep.indexer,
                "Agreement marked as CanceledByIndexer"
            ),
            None => {}
        }

        // The `terminated` event is NOT emitted here. `apply_reconciliation`
        // persisted the cancel audit (canceled_at/by/tx) in the same transaction
        // as this transition; `sweep_pending_terminated_events` is the single
        // authoritative emitter and will announce it durably.
    }

    // Only on a fresh accept, as repeating it on a no-op would redo work a prior poll already
    // did. An accept that lands together with its cancel replaces nothing, so its pending
    // cancellations are dropped, not run.
    let fanned_out = if outcome.did_accept && outcome.did_cancel {
        keep_agreements_of_an_ended_replacement(&prep.agreement.id, registry).await
    } else if outcome.did_accept {
        execute_pending_cancellations(&prep.agreement.id, registry, chain_client, config).await
    } else {
        Ok(())
    };
    // After the fan-out, so the reassessment finds the agreements it replaced already marked, and
    // whether or not the fan-out worked, as the cancel is on record either way.
    reassess_after_indexer_cancel(prep, outcome, registry, worker_queue, queued_reassessments)
        .await;
    fanned_out
}

/// Queue a reassessment of the request whose indexer has just been recorded cancelling its
/// agreement on-chain, which leaves the request a slot short. Nothing else fills it before the
/// daily pass, as the hourly top-up and the sweep of freed requests only take requests a
/// reassessment has marked short. A request with a reassessment already queued or running gets
/// no second one. A failure is logged, not returned: the cancel is on record either way, and
/// the daily pass reassesses the request.
async fn reassess_after_indexer_cancel<R, W>(
    prep: &PreparedReconciliation,
    outcome: crate::registry::ReconciliationOutcome,
    registry: &R,
    worker_queue: &W,
    queued: &mut QueuedReassessments,
) where
    R: crate::registry::IndexingRequestRegistry + Sync,
    W: WorkerQueue + Sync,
{
    if !outcome.did_cancel || !matches!(prep.item.cancel, Some(CancelKind::ByIndexer)) {
        return;
    }
    let agreement = &prep.agreement;
    let Some(request) = open_request_of(agreement, registry).await else {
        return;
    };
    if queued.contains(worker_queue, &request).await {
        tracing::debug!(
            agreement_id = %agreement.id,
            indexing_request_id = %request.id,
            "Reassessment already queued for the request of an agreement its indexer cancelled"
        );
        return;
    }
    if queue_reassessment_of(&request, agreement, worker_queue).await {
        queued.add(request.id);
    }
}

/// Queue a reassessment of `request`, which lost `agreement` to its indexer's cancel; whether it
/// was queued.
async fn queue_reassessment_of<W: WorkerQueue + Sync>(
    request: &crate::registry::IndexingRequest,
    agreement: &IndexingAgreement,
    worker_queue: &W,
) -> bool {
    let queued = worker_queue
        .reassess_indexing_request(
            request.id,
            request.deployment_id,
            request.deployment_chain_id,
            request.num_candidates,
            // Background: replacing an indexer yields to interactive work.
            JobPriority::Background,
        )
        .await;
    match queued {
        Ok(_) => {
            tracing::info!(
                agreement_id = %agreement.id,
                indexing_request_id = %request.id,
                "Queued reassessment for the request of an agreement its indexer cancelled"
            );
            true
        }
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                indexing_request_id = %request.id,
                error = format!("{err:#}"),
                "Failed to queue reassessment for the request of an agreement its indexer \
                 cancelled; the daily pass reassesses it"
            );
            false
        }
    }
}

/// The requests with a reassessment queued or running, listed from the job queue the first time
/// a page of changes asks, as listing reads every unfinished job. A page's changes are all applied
/// before any is finalized, so a reassessment listed as waiting will see every one of them.
#[derive(Debug, Default)]
struct QueuedReassessments {
    listed: Option<std::collections::HashSet<dipper_core::ids::IndexingRequestId>>,
}

impl QueuedReassessments {
    /// Whether a reassessment is queued or running for `request`. When the queue can't be listed
    /// none counts as queued for the rest of the page, as a duplicate only repeats a
    /// reassessment.
    async fn contains<W: WorkerQueue + Sync>(
        &mut self,
        worker_queue: &W,
        request: &crate::registry::IndexingRequest,
    ) -> bool {
        if self.listed.is_none() {
            let listed = match worker_queue.queued_reassessments().await {
                Ok(listed) => listed,
                Err(err) => {
                    tracing::warn!(
                        indexing_request_id = %request.id,
                        error = format!("{err:#}"),
                        "Can't list queued reassessments; queueing without that check"
                    );
                    std::collections::HashSet::new()
                }
            };
            self.listed = Some(listed);
        }
        self.listed
            .as_ref()
            .is_some_and(|listed| listed.contains(&request.id))
    }

    /// Count the reassessment just queued for `request_id` as queued for the rest of the page.
    fn add(&mut self, request_id: dipper_core::ids::IndexingRequestId) {
        self.listed
            .get_or_insert_with(Default::default)
            .insert(request_id);
    }
}

/// The request `agreement` serves while it is open. `None`, logged, when it is cancelled, gone
/// or can't be read; an unreadable one is left to the daily pass.
async fn open_request_of<R>(
    agreement: &IndexingAgreement,
    registry: &R,
) -> Option<crate::registry::IndexingRequest>
where
    R: crate::registry::IndexingRequestRegistry + Sync,
{
    match registry
        .get_indexing_request_by_id(&agreement.indexing_request_id)
        .await
    {
        Ok(Some(request)) if request.status == crate::registry::IndexingRequestStatus::Open => {
            Some(request)
        }
        Ok(Some(request)) => {
            tracing::debug!(
                agreement_id = %agreement.id,
                indexing_request_id = %request.id,
                request_status = %request.status,
                "Not reassessing a cancelled request after its indexer cancelled an agreement"
            );
            None
        }
        Ok(None) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                indexing_request_id = %agreement.indexing_request_id,
                "Indexing request not found for an agreement its indexer cancelled"
            );
            None
        }
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                indexing_request_id = %agreement.indexing_request_id,
                error = %err,
                "Failed to read the request of an agreement its indexer cancelled; the daily pass \
                 reassesses it"
            );
            None
        }
    }
}

/// Reconcile a single agreement snapshot against dipper's local DB.
///
/// Compares the snapshot's remote state to the local `IndexingAgreementStatus`
/// and applies whatever transitions the diff implies, returning the chain checks it queued.
/// See the module-level transition table for the full mapping.
#[cfg(test)]
async fn reconcile_agreement<R, T>(
    snapshot: &AgreementStateSnapshot,
    registry: &R,
    chain_client: &T,
    config: &crate::config::IndexingAgreementConfig,
) -> anyhow::Result<Vec<tests::QueuedCheck>>
where
    R: AgreementRegistry
        + PendingCancellationRegistry
        + crate::registry::IndexingRequestRegistry
        + Sync,
    T: ChainClient,
{
    let mut live_checks = LiveChecks::new(tests::MockWorkerQueue::default());
    let agreement = registry
        .get_indexing_agreement_by_id(&snapshot.agreement_id)
        .await?;
    let prep = prepare_reconciliation(snapshot, agreement, registry, &mut live_checks).await?;
    let queued = live_checks.queue.queued();
    let Some(prep) = prep else {
        return Ok(queued);
    };

    let outcome = registry
        .apply_reconciliation(&prep.agreement.id, prep.item.apply_accept, prep.item.cancel)
        .await?;

    finalize_reconciliation(
        &prep,
        outcome,
        registry,
        chain_client,
        config,
        &live_checks.queue,
        &mut QueuedReassessments::default(),
    )
    .await?;
    Ok(queued)
}

/// Drop the pending cancellations of an agreement that one poll showed accepted and already
/// cancelled, without running them. The agreements it was to replace stay as they are, since
/// cancelling them now would leave the request with neither. Nothing else would clear the rows:
/// the sweep only takes those of an agreement that is still accepted.
async fn keep_agreements_of_an_ended_replacement<R>(
    agreement_id: &IndexingAgreementId,
    registry: &R,
) -> anyhow::Result<()>
where
    R: PendingCancellationRegistry + Sync,
{
    let kept = registry
        .get_pending_cancellations_by_new_agreement(*agreement_id)
        .await?;
    if kept.is_empty() {
        return Ok(());
    }
    registry
        .delete_pending_cancellations_by_new_agreement(*agreement_id)
        .await?;
    for cancellation in &kept {
        tracing::info!(
            new_agreement_id = %agreement_id,
            old_agreement_id = %cancellation.old_agreement_id,
            "Replacement was accepted and cancelled before dipper saw it; keeping the agreement \
             it was to replace"
        );
    }
    Ok(())
}

/// Execute pending cancellations linked to a newly-accepted agreement.
///
/// Called from the Created -> AcceptedOnChain and Expired -> AcceptedOnChain
/// transitions. Each replaced agreement is marked `Cancelling` and its on-chain cancel
/// sent once; the cancel retry finishes any that don't end at once.
/// A pending row is deleted once its agreement is marked; a failed mark keeps it for retry.
async fn execute_pending_cancellations<R, T>(
    agreement_id: &IndexingAgreementId,
    registry: &R,
    chain_client: &T,
    config: &crate::config::IndexingAgreementConfig,
) -> anyhow::Result<()>
where
    R: AgreementRegistry + PendingCancellationRegistry + Sync,
    T: ChainClient,
{
    let pending = registry
        .get_pending_cancellations_by_new_agreement(*agreement_id)
        .await?;

    let mut transient_failures: u32 = 0;
    for cancellation in &pending {
        let old_agreement_id = cancellation.old_agreement_id;
        if start_replaced_cancel(
            agreement_id,
            &old_agreement_id,
            registry,
            chain_client,
            config,
        )
        .await?
        {
            registry
                .delete_pending_cancellation(*agreement_id, old_agreement_id)
                .await?;
        } else {
            transient_failures += 1;
        }
    }

    if transient_failures > 0 {
        anyhow::bail!(
            "{transient_failures} pending cancellation(s) failed for agreement {}; \
             records retained for retry",
            agreement_id,
        );
    }

    Ok(())
}

/// Start cancelling an agreement its accepted replacement supersedes. Returns whether its
/// pending cancellation is done with: marked, already ended, or gone.
async fn start_replaced_cancel<R, T>(
    new_agreement_id: &IndexingAgreementId,
    old_agreement_id: &IndexingAgreementId,
    registry: &R,
    chain_client: &T,
    config: &crate::config::IndexingAgreementConfig,
) -> anyhow::Result<bool>
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let Some(old_agreement) = registry
        .get_indexing_agreement_by_id(old_agreement_id)
        .await?
    else {
        tracing::warn!(
            %old_agreement_id,
            "Pending cancellation references non-existent agreement, cleaning up"
        );
        return Ok(true);
    };
    let reason = crate::cancel_dispatch::CancelReason::NotWanted;
    // An expired one is read first, so one that really expired keeps its status.
    let started = if old_agreement.status == IndexingAgreementStatus::Expired {
        match expired_but_live(chain_client, &old_agreement).await {
            Some(true) => {
                crate::cancel_dispatch::start_cancel_seen_live(
                    registry,
                    chain_client,
                    &old_agreement,
                    reason,
                    config,
                )
                .await
            }
            Some(false) => return Ok(true),
            None => return Ok(false),
        }
    } else {
        crate::cancel_dispatch::start_cancel(registry, chain_client, &old_agreement, reason, config)
            .await
    };
    Ok(note_replaced_cancel(
        new_agreement_id,
        &old_agreement,
        started,
    ))
}

/// Whether an agreement marked `Expired` is live on-chain after all, accepted unseen by a
/// lagging listener; `None`, logged, when the chain can't be read. One that really expired
/// stays `Expired`.
async fn expired_but_live<T: ChainClient>(
    chain_client: &T,
    agreement: &IndexingAgreement,
) -> Option<bool> {
    match chain_client
        .agreement_on_chain(agreement.id.as_bytes())
        .await
    {
        Ok(on_chain) => Some(on_chain.is_live()),
        Err(err) => {
            tracing::warn!(
                old_agreement_id = %agreement.id,
                error = %err,
                "Failed to read whether a replaced expired agreement is live, retaining pending row"
            );
            None
        }
    }
}

/// Log how cancelling a replaced agreement started; false when it couldn't be marked.
fn note_replaced_cancel(
    new_agreement_id: &IndexingAgreementId,
    old_agreement: &IndexingAgreement,
    started: crate::registry::Result<crate::cancel_dispatch::CancelStarted>,
) -> bool {
    let old_agreement_id = old_agreement.id;
    match started {
        Ok(started) => tracing::info!(
            %new_agreement_id,
            %old_agreement_id,
            old_status = %old_agreement.status,
            ?started,
            reason = "replacement_accepted",
            "Cancelling replaced agreement"
        ),
        Err(crate::registry::Error::NoRecordsUpdated) => tracing::debug!(
            %old_agreement_id,
            "Replaced agreement already ended or being cancelled"
        ),
        Err(err) => {
            tracing::error!(
                %old_agreement_id,
                error = %err,
                "Failed to mark replaced agreement cancelling, retaining pending row"
            );
            return false;
        }
    }
    true
}

/// Start cancelling agreements orphaned by a failed shrink-to-zero: still `AcceptedOnChain`
/// although their request is `Canceled`, because reassessment couldn't mark them. Each starts
/// like any other cancel, so the cancel retry finishes it with the same limit.
async fn sweep_orphan_canceled_agreements<R, T>(
    registry: &R,
    chain_client: &T,
    config: &crate::config::IndexingAgreementConfig,
) where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let orphans = match registry
        .get_agreements_pending_chain_cancel(SWEEP_BATCH_SIZE)
        .await
    {
        Ok(orphans) => orphans,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to query orphan canceled agreements for sweep"
            );
            return;
        }
    };

    for agreement in orphans {
        let started = crate::cancel_dispatch::start_cancel(
            registry,
            chain_client,
            &agreement,
            crate::cancel_dispatch::CancelReason::NotWanted,
            config,
        )
        .await;
        log_orphan_cancel(&agreement, started);
    }
}

fn log_orphan_cancel(
    agreement: &IndexingAgreement,
    started: crate::registry::Result<crate::cancel_dispatch::CancelStarted>,
) {
    match started {
        Ok(started) => tracing::info!(
            agreement_id = %agreement.id,
            ?started,
            reason = "request_canceled",
            "Cancelling orphan agreement"
        ),
        Err(crate::registry::Error::NoRecordsUpdated) => tracing::debug!(
            agreement_id = %agreement.id,
            "Orphan agreement already ended or being cancelled"
        ),
        Err(err) => tracing::warn!(
            error = %err,
            agreement_id = %agreement.id,
            "Failed to mark orphan agreement cancelling; will retry next sweep"
        ),
    }
}

/// Health of a durable event sweep, reported to the poll loop so it can pause
/// the sweeps during a Kafka outage instead of blocking on every poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepHealth {
    /// At least one event was sent and the broker accepted it.
    Sent,
    /// A send failed; the broker is likely unreachable.
    Failed,
    /// Nothing was pending, so the broker's reachability is unknown.
    Idle,
}

impl SweepHealth {
    /// Combine two outcomes: a failure dominates, then a send, then idle.
    fn merge(self, other: SweepHealth) -> SweepHealth {
        match (self, other) {
            (SweepHealth::Failed, _) | (_, SweepHealth::Failed) => SweepHealth::Failed,
            (SweepHealth::Sent, _) | (_, SweepHealth::Sent) => SweepHealth::Sent,
            _ => SweepHealth::Idle,
        }
    }
}

/// The cooldown change worth logging after folding in a sweep round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CooldownTransition {
    /// Broker went from healthy to paused.
    Entered,
    /// A retry failed again, so the backoff grew.
    Extended,
    /// First successful send after a pause.
    Recovered,
    /// Nothing worth logging.
    Unchanged,
}

/// Backoff clock that pauses the durable event sweeps after a Kafka send
/// failure. Each sweep makes one blocking send that can hang for the produce
/// timeout, so an outage would otherwise stall the chain poll loop every poll.
struct BrokerCooldown {
    /// Initial backoff and the value reset to on recovery: one poll interval.
    base: Duration,
    /// Backoff ceiling, so a down broker is retried at least this often.
    ceiling: Duration,
    /// When the current pause began, or `None` while the broker is healthy.
    down_since: Option<Instant>,
    /// Current backoff length.
    backoff: Duration,
}

impl BrokerCooldown {
    fn new(base: Duration, ceiling: Duration) -> Self {
        Self {
            base,
            ceiling,
            down_since: None,
            backoff: base,
        }
    }

    /// Whether the event sweeps should be skipped at `now`.
    fn is_paused(&self, now: Instant) -> bool {
        matches!(self.down_since, Some(since) if now.duration_since(since) < self.backoff)
    }

    fn backoff(&self) -> Duration {
        self.backoff
    }

    /// Fold a sweep round's health into the clock, returning what to log. A send
    /// clears the pause and resets the backoff; a failure enters or extends it
    /// (doubling up to the ceiling); an idle round leaves the state untouched.
    fn record(&mut self, health: SweepHealth, now: Instant) -> CooldownTransition {
        match health {
            SweepHealth::Sent => {
                let recovered = self.down_since.take().is_some();
                self.backoff = self.base;
                if recovered {
                    CooldownTransition::Recovered
                } else {
                    CooldownTransition::Unchanged
                }
            }
            SweepHealth::Failed if self.down_since.is_some() => {
                self.backoff = self.backoff.saturating_mul(2).min(self.ceiling);
                self.down_since = Some(now);
                CooldownTransition::Extended
            }
            SweepHealth::Failed => {
                self.backoff = self.base;
                self.down_since = Some(now);
                CooldownTransition::Entered
            }
            SweepHealth::Idle => CooldownTransition::Unchanged,
        }
    }
}

/// Run the three durable event sweeps in order, stopping early if one reports a
/// broker send failure so the rest are not attempted this poll. Accepted before
/// terminated: one snapshot can accept then cancel, and `accepted` must lead.
async fn run_event_sweeps<R>(
    registry: &R,
    config: &crate::config::IndexingAgreementConfig,
    events_emitter: &dyn SubgraphIndexingAgreementEventsProducer,
) -> SweepHealth
where
    R: AgreementRegistry + Sync,
{
    let accepted = sweep_pending_accepted_events(registry, events_emitter).await;
    if accepted == SweepHealth::Failed {
        return accepted;
    }
    let health =
        accepted.merge(sweep_pending_terminated_events(registry, config, events_emitter).await);
    if health == SweepHealth::Failed {
        return health;
    }
    health.merge(sweep_pending_expired_events(registry, events_emitter).await)
}

/// Emit `terminated` events durably for agreements that have reached a terminal
/// cancel state but not yet been announced.
///
/// This is the single authoritative emitter for `terminated`: every cancel path
/// (indexer-observed on-chain, or dipper-initiated) marks the row terminal and
/// leaves `terminated_event_emitted_at` NULL; this sweep re-derives the event
/// from the row, sends it synchronously, and stamps the marker only after the
/// broker confirms. If the send fails or the process crashes first, the marker
/// stays NULL and the next sweep retries -- self-healing, at-least-once.
///
/// Eligibility (`get_agreements_pending_terminated_emission`) requires
/// `accepted_at IS NOT NULL`, so a never-accepted agreement that was only ever
/// cancelled locally never produces a `terminated` -- there was nothing live
/// on-chain to terminate.
#[expect(
    clippy::cast_sign_loss,
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn sweep_pending_terminated_events<R>(
    registry: &R,
    config: &crate::config::IndexingAgreementConfig,
    events_emitter: &dyn SubgraphIndexingAgreementEventsProducer,
) -> SweepHealth
where
    R: AgreementRegistry + Sync,
{
    let pending = match registry
        .get_agreements_pending_terminated_emission(SWEEP_BATCH_SIZE)
        .await
    {
        Ok(pending) => pending,
        Err(err) => {
            tracing::warn!(error = %err, "failed to query agreements pending terminated emission");
            return SweepHealth::Idle;
        }
    };

    if pending.is_empty() {
        return SweepHealth::Idle;
    }

    tracing::debug!(count = pending.len(), "emitting pending terminated events");

    for p in pending {
        // Count runs after the row is already terminal, so it excludes this one.
        let remaining =
            crate::registry::remaining_accepted_indexing_agreements(registry, &p.deployment_id)
                .await;

        // Fall back to the row's terminal-transition time and the manager address
        // when the on-chain cancel audit was never recorded.
        let terminated_at = p
            .canceled_at
            .unwrap_or_else(|| p.updated_at.unix_timestamp().max(0) as u64);
        let terminated_by = p
            .canceled_by
            .clone()
            .unwrap_or_else(|| config.recurring_agreement_manager().to_string());

        let event = proto::SubgraphIndexingAgreementTerminated {
            agreement_id: p.agreement_id.to_string(),
            indexer: p.indexer_id.to_string(),
            terminated_at,
            terminated_by,
            terminated_tx: p.canceled_tx.clone().unwrap_or_default(),
            remaining_accepted_indexing_agreements: remaining,
        };

        match events_emitter
            .emit_subgraph_indexing_agreement_terminated(p.deployment_id, p.protocol_network, event)
            .await
        {
            Ok(()) => {
                if let Err(err) = registry
                    .mark_terminated_event_emitted(&p.agreement_id)
                    .await
                {
                    // Sent but not stamped: the next sweep re-sends (deduped by
                    // consumers on agreement_id and event type). Better than dropping.
                    tracing::warn!(
                        agreement_id = %p.agreement_id,
                        error = %err,
                        "emitted terminated event but failed to stamp marker; may re-emit"
                    );
                }
            }
            Err(err) => {
                // A send failure almost always means the broker is unreachable,
                // so the rest of this batch would fail too. Stop now and retry the
                // whole batch next sweep -- avoids a burst of failed sends and log
                // spam during an outage. Nothing is lost (markers stay unset).
                tracing::warn!(
                    agreement_id = %p.agreement_id,
                    error = %err,
                    "failed to emit terminated event; pausing batch, will retry next sweep"
                );
                return SweepHealth::Failed;
            }
        }
    }
    SweepHealth::Sent
}

/// Emit `accepted` events durably for agreements that reached accepted on-chain
/// but haven't been announced. Mirror of [`sweep_pending_terminated_events`]:
/// re-derive from the row, send synchronously, stamp the marker only on a
/// confirmed send.
///
/// Eligibility (`get_agreements_pending_accepted_emission`) requires
/// `accepted_at IS NOT NULL` (only rows whose accept was recorded) but is
/// NOT gated on current status: an agreement accepted and then cancelled in a
/// single snapshot is already terminal yet must still emit its `accepted` (which
/// is why this sweep runs before the terminated sweep).
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn sweep_pending_accepted_events<R>(
    registry: &R,
    events_emitter: &dyn SubgraphIndexingAgreementEventsProducer,
) -> SweepHealth
where
    R: AgreementRegistry + Sync,
{
    let pending = match registry
        .get_agreements_pending_accepted_emission(SWEEP_BATCH_SIZE)
        .await
    {
        Ok(pending) => pending,
        Err(err) => {
            tracing::warn!(error = %err, "failed to query agreements pending accepted emission");
            return SweepHealth::Idle;
        }
    };

    if pending.is_empty() {
        return SweepHealth::Idle;
    }

    tracing::debug!(count = pending.len(), "emitting pending accepted events");

    for p in pending {
        let event = proto::SubgraphIndexingAgreementAccepted {
            agreement_id: p.agreement_id.to_string(),
            indexer: p.indexer_id.to_string(),
            accepted_at: p.accepted_at,
            accepted_tx: p.accepted_tx.clone(),
            ends_at: p.ends_at,
            payer: p.payer.to_string(),
        };

        match events_emitter
            .emit_subgraph_indexing_agreement_accepted(p.deployment_id, p.protocol_network, event)
            .await
        {
            Ok(()) => {
                if let Err(err) = registry.mark_accepted_event_emitted(&p.agreement_id).await {
                    tracing::warn!(
                        agreement_id = %p.agreement_id,
                        error = %err,
                        "emitted accepted event but failed to stamp marker; may re-emit"
                    );
                }
            }
            Err(err) => {
                // Broker likely unreachable; the rest of the batch would fail too.
                // Stop and retry next sweep (markers stay unset -> nothing lost).
                tracing::warn!(
                    agreement_id = %p.agreement_id,
                    error = %err,
                    "failed to emit accepted event; pausing batch, will retry next sweep"
                );
                return SweepHealth::Failed;
            }
        }
    }
    SweepHealth::Sent
}

/// Emit `request.expired` events durably for agreements marked `Expired` but not
/// yet announced. Mirror of the accepted/terminated sweeps.
///
/// Running here (after the drain, which recovers `Expired -> AcceptedOnChain` on
/// a late on-chain accept) means a row that was prematurely marked `Expired` is
/// no longer selected once recovered -- so a premature `expired` never
/// contradicts a subsequent `accepted`.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn sweep_pending_expired_events<R>(
    registry: &R,
    events_emitter: &dyn SubgraphIndexingAgreementEventsProducer,
) -> SweepHealth
where
    R: AgreementRegistry + Sync,
{
    let pending = match registry
        .get_agreements_pending_expired_emission(SWEEP_BATCH_SIZE)
        .await
    {
        Ok(pending) => pending,
        Err(err) => {
            tracing::warn!(error = %err, "failed to query agreements pending expired emission");
            return SweepHealth::Idle;
        }
    };

    if pending.is_empty() {
        return SweepHealth::Idle;
    }

    tracing::debug!(count = pending.len(), "emitting pending expired events");

    for p in pending {
        let event = proto::SubgraphIndexingAgreementRequestExpired {
            agreement_id: p.agreement_id.to_string(),
            indexer: p.indexer_id.to_string(),
            request_proposed_at: p.request_proposed_at,
            request_expired_at: p.request_expired_at,
        };

        match events_emitter
            .emit_subgraph_indexing_agreement_request_expired(
                p.deployment_id,
                p.protocol_network,
                event,
            )
            .await
        {
            Ok(()) => {
                if let Err(err) = registry.mark_expired_event_emitted(&p.agreement_id).await {
                    tracing::warn!(
                        agreement_id = %p.agreement_id,
                        error = %err,
                        "emitted expired event but failed to stamp marker; may re-emit"
                    );
                }
            }
            Err(err) => {
                // Broker likely unreachable; the rest of the batch would fail too.
                // Stop and retry next sweep (markers stay unset -> nothing lost).
                tracing::warn!(
                    agreement_id = %p.agreement_id,
                    error = %err,
                    "failed to emit expired event; pausing batch, will retry next sweep"
                );
                return SweepHealth::Failed;
            }
        }
    }
    SweepHealth::Sent
}

/// Recover stranded pending cancellations from a partial-progress crash.
///
/// `execute_pending_cancellations` is invoked from `reconcile_agreement`
/// only on a fresh `Created`/`Expired` -> `AcceptedOnChain` transition.
/// If the process crashes after the local row is committed as
/// `AcceptedOnChain` but before the cancellation fan-out completes, the
/// pending_cancellation rows are left behind and the cursor advances
/// past the snapshot that would have re-triggered them. This sweep runs
/// once per chain_listener poll and re-feeds any such IDs through
/// `execute_pending_cancellations`, which is idempotent on already-canceled
/// old agreements and on already-deleted pending rows.
///
/// Per-orphan failures are logged and swallowed so one stuck cancellation
/// cannot block the rest of the sweep.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn sweep_executable_pending_cancellations<R, T>(
    registry: &R,
    chain_client: &T,
    config: &crate::config::IndexingAgreementConfig,
) where
    R: AgreementRegistry + PendingCancellationRegistry + Sync,
    T: ChainClient,
{
    let targets = match registry
        .list_executable_pending_cancellations(SWEEP_BATCH_SIZE)
        .await
    {
        Ok(targets) => targets,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to list executable pending cancellations for sweep"
            );
            return;
        }
    };

    if targets.is_empty() {
        return;
    }

    tracing::debug!(
        count = targets.len(),
        "Sweeping pending cancellations on AcceptedOnChain agreements"
    );

    for new_agreement_id in targets {
        if let Err(err) =
            execute_pending_cancellations(&new_agreement_id, registry, chain_client, config).await
        {
            tracing::warn!(
                error = %err,
                new_agreement_id = %new_agreement_id,
                "Sweep failed to execute pending cancellations; will retry next poll"
            );
        }
    }
}

/// Trait for chain listener state persistence
#[async_trait::async_trait]
pub trait ChainListenerStateRegistry {
    /// Get the current chain listener state for a chain
    async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> Result<Option<ChainListenerState>, crate::registry::Error>;

    /// Update the chain listener state
    async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        cursor: &Cursor,
        last_processed_block_timestamp: Option<u64>,
    ) -> Result<(), crate::registry::Error>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};
    use time::OffsetDateTime;

    use super::{super::chain_events::AgreementState, *};
    use crate::registry::{
        AgreementFeeRate, Indexer, IndexingAgreement, IndexingAgreementTerms as Terms,
        IndexingAgreementTermsMetadata as TermsMetadata, Result as RegistryResult,
    };

    fn test_terms() -> Terms {
        Terms {
            payer: Address::ZERO,
            service_provider: Address::ZERO,
            data_service: Address::ZERO,
            deadline: 0,
            ends_at: 0,
            max_initial_tokens: thegraph_core::alloy::primitives::U256::ZERO,
            max_ongoing_tokens_per_second: thegraph_core::alloy::primitives::U256::ZERO,
            min_seconds_per_collection: 0,
            max_seconds_per_collection: 0,
            conditions: 0,
            metadata: TermsMetadata {
                tokens_per_second: thegraph_core::alloy::primitives::U256::ZERO,
                tokens_per_entity_per_second: thegraph_core::alloy::primitives::U256::ZERO,
                subgraph_deployment_id: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                    .parse()
                    .unwrap(),
                protocol_network: ChainId::from(42161u64),
                chain_id: ChainId::from(1u64),
                proposed_at: 0,
            },
        }
    }

    fn test_agreement_conf() -> std::sync::Arc<crate::config::IndexingAgreementConfig> {
        std::sync::Arc::new(crate::config::IndexingAgreementConfig::for_tests())
    }

    // Deterministic audit values used by `make_snapshot` so emit tests can assert
    // the enriched `accepted`/`terminated` tx + timestamp fields.
    const TEST_ACCEPTED_AT: u64 = 1_700_000_001;
    const TEST_ACCEPTED_TX: &str =
        "0x1111111111111111111111111111111111111111111111111111111111111111";
    const TEST_CANCELED_AT: u64 = 1_700_000_002;
    const TEST_CANCELED_TX: &str =
        "0x2222222222222222222222222222222222222222222222222222222222222222";

    fn make_snapshot(
        agreement_id: IndexingAgreementId,
        state: AgreementState,
        canceled_by: Address,
    ) -> AgreementStateSnapshot {
        AgreementStateSnapshot {
            agreement_id,
            indexer: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            state,
            canceled_by,
            last_state_change_block: 100,
            accepted_at: TEST_ACCEPTED_AT,
            accepted_tx: TEST_ACCEPTED_TX.to_string(),
            canceled_at: TEST_CANCELED_AT,
            canceled_tx: TEST_CANCELED_TX.to_string(),
        }
    }

    #[test]
    fn fetch_failures_alert_once_per_run_of_failures() {
        let mut expected = vec![FetchFailureLog::Warn; 9];
        expected.push(FetchFailureLog::Alert);
        expected.extend([FetchFailureLog::Error; 5]);

        let logged: Vec<_> = (1..=15).map(fetch_failure_log).collect();

        assert_eq!(logged, expected);
    }

    #[test]
    fn test_handle_clone() {
        let (tx, _rx) = mpsc::channel(1);
        let handle = Handle { tx_stop: tx };
        let _cloned = handle.clone();
    }

    // Mock registry for testing
    #[derive(Clone, Default)]
    struct MockRegistry {
        state: Arc<Mutex<MockRegistryState>>,
    }

    #[derive(Default)]
    struct MockRegistryState {
        agreements: std::collections::HashMap<IndexingAgreementId, IndexingAgreement>,
        /// When true, the check for offered or accepted agreements fails.
        fail_active_check: bool,
        marked_accepted_on_chain: Vec<IndexingAgreementId>,
        marked_canceled_by_requester: Vec<IndexingAgreementId>,
        marked_cancelling: Vec<IndexingAgreementId>,
        reopened: Vec<IndexingAgreementId>,
        marked_canceled_by_indexer: Vec<IndexingAgreementId>,
        /// Ids passed to `record_cancel_audit` -- the signal a cancel path drives
        /// the terminated event (the sweep emits from this audit).
        recorded_cancel_audit: Vec<IndexingAgreementId>,
        /// Every audit write in order, as ("cancel" | "accept" | "accept and cancel", id).
        audit_writes: Vec<(&'static str, IndexingAgreementId)>,
        /// When true, writes that record a cancel fail.
        fail_cancel_audit: bool,
        /// When true, writes that record only an accept fail.
        fail_accept_audit: bool,
        pending_cancellations: std::collections::HashMap<
            IndexingAgreementId,
            Vec<crate::registry::PendingCancellation>,
        >,
        deleted_pending_cancellations: Vec<(IndexingAgreementId, IndexingAgreementId)>,
        fail_cancel_for: std::collections::HashSet<IndexingAgreementId>,
        canceled_request_ids: std::collections::HashSet<IndexingRequestId>,
        chain_listener_state_updates: Vec<(Cursor, Option<u64>)>,
        initial_last_processed_block: u64,
        fail_batch: bool,
        pending_accepted: Vec<crate::registry::PendingAcceptedEvent>,
        /// Changes recorded as set aside, in the order first recorded.
        set_aside: Vec<crate::registry::SetAsideChange>,
        /// When true, recording set-aside changes fails.
        fail_set_aside: bool,
        /// A batch apply holding any of these agreements fails.
        fail_apply_for: std::collections::HashSet<IndexingAgreementId>,
        /// When true, reading the set-aside changes fails.
        fail_set_aside_read: bool,
        /// When true, reading agreements fails.
        fail_agreement_reads: bool,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self::default()
        }

        fn add_agreement(&self, id: IndexingAgreementId, status: IndexingAgreementStatus) {
            let terms = test_terms();
            let agreement = IndexingAgreement {
                id,
                nonce_uuid: uuid::Uuid::now_v7(),
                status,
                indexer: Indexer {
                    id: "0x1234567890123456789012345678901234567890"
                        .parse()
                        .unwrap(),
                    url: "http://indexer.test".parse().unwrap(),
                },
                indexing_request_id: IndexingRequestId::new(),
                terms,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
                last_block_height: None,
                last_progress_at: None,
                rejection_reason: None,
                // The manager cancel path requires a 32-byte stored terms hash.
                terms_version_hash: Some(vec![0u8; 32]),
            };
            self.state.lock().unwrap().agreements.insert(id, agreement);
        }

        fn add_pending_cancellation(
            &self,
            new_agreement_id: IndexingAgreementId,
            old_agreement_id: IndexingAgreementId,
        ) {
            let pc = crate::registry::PendingCancellation { old_agreement_id };
            self.state
                .lock()
                .unwrap()
                .pending_cancellations
                .entry(new_agreement_id)
                .or_default()
                .push(pc);
        }

        fn fail_cancel_for(&self, id: IndexingAgreementId) {
            self.state.lock().unwrap().fail_cancel_for.insert(id);
        }

        fn mark_request_canceled(&self, id: IndexingRequestId) {
            self.state.lock().unwrap().canceled_request_ids.insert(id);
        }

        /// Set when the offer could last be accepted, as seconds from now.
        fn set_agreement_deadline_from_now(&self, agreement_id: IndexingAgreementId, secs: i64) {
            if let Some(a) = self.state.lock().unwrap().agreements.get_mut(&agreement_id) {
                let deadline = OffsetDateTime::now_utc().unix_timestamp() + secs;
                a.terms.deadline = u64::try_from(deadline).unwrap();
            }
        }

        fn set_agreement_request_id(
            &self,
            agreement_id: IndexingAgreementId,
            request_id: IndexingRequestId,
        ) {
            if let Some(a) = self.state.lock().unwrap().agreements.get_mut(&agreement_id) {
                a.indexing_request_id = request_id;
            }
        }

        fn was_marked_accepted_on_chain(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_accepted_on_chain
                .contains(id)
        }

        fn was_marked_cancelling(&self, id: &IndexingAgreementId) -> bool {
            self.state.lock().unwrap().marked_cancelling.contains(id)
        }

        fn was_reopened(&self, id: &IndexingAgreementId) -> bool {
            self.state.lock().unwrap().reopened.contains(id)
        }

        fn was_marked_canceled_by_requester(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_requester
                .contains(id)
        }

        fn was_marked_canceled_by_indexer(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .marked_canceled_by_indexer
                .contains(id)
        }

        fn audit_writes(&self) -> Vec<(&'static str, IndexingAgreementId)> {
            self.state.lock().unwrap().audit_writes.clone()
        }

        fn was_cancel_audit_recorded(&self, id: &IndexingAgreementId) -> bool {
            self.state
                .lock()
                .unwrap()
                .recorded_cancel_audit
                .contains(id)
        }

        fn was_pending_cancellation_deleted(
            &self,
            new_id: &IndexingAgreementId,
            old_id: &IndexingAgreementId,
        ) -> bool {
            self.state
                .lock()
                .unwrap()
                .deleted_pending_cancellations
                .contains(&(*new_id, *old_id))
        }

        fn set_initial_last_processed_block(&self, block: u64) {
            self.state.lock().unwrap().initial_last_processed_block = block;
        }

        fn chain_listener_state_updates(&self) -> Vec<(Cursor, Option<u64>)> {
            self.state
                .lock()
                .unwrap()
                .chain_listener_state_updates
                .clone()
        }

        fn set_fail_batch(&self, fail: bool) {
            self.state.lock().unwrap().fail_batch = fail;
        }

        fn seed_pending_accepted(&self, event: crate::registry::PendingAcceptedEvent) {
            self.state.lock().unwrap().pending_accepted.push(event);
        }

        fn set_aside(&self) -> Vec<crate::registry::SetAsideChange> {
            self.state.lock().unwrap().set_aside.clone()
        }

        fn set_fail_set_aside(&self, fail: bool) {
            self.state.lock().unwrap().fail_set_aside = fail;
        }

        fn fail_apply_for(&self, id: IndexingAgreementId) {
            self.state.lock().unwrap().fail_apply_for.insert(id);
        }

        fn set_fail_set_aside_read(&self, fail: bool) {
            self.state.lock().unwrap().fail_set_aside_read = fail;
        }

        fn set_fail_agreement_reads(&self, fail: bool) {
            self.state.lock().unwrap().fail_agreement_reads = fail;
        }
    }

    #[async_trait::async_trait]
    impl crate::registry::StubAgreementRegistry for MockRegistry {
        async fn get_indexing_agreement_by_id(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<Option<IndexingAgreement>> {
            let state = self.state.lock().unwrap();
            if state.fail_agreement_reads {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "forced agreement read failure (test)".into(),
                    )),
                ));
            }
            Ok(state.agreements.get(id).cloned())
        }

        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_indexing_agreements_by_indexer_id(
            &self,
            _indexer_id: &IndexerId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_pending_agreement_indexers_by_deployment(
            &self,
            _indexer_ids: &[IndexerId],
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(std::collections::HashMap::new())
        }

        async fn get_declined_indexers_by_deployment(
            &self,
            _default_lookback_days: i32,
            _price_lookback_days: i32,
            _transient_lookback_minutes: i32,
            _uncertain_lookback_days: i32,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(std::collections::HashMap::new())
        }
        async fn get_unresponsive_indexers(
            &self,
            _lookback_days: i32,
            _chain_id: thegraph_core::alloy::primitives::ChainId,
        ) -> RegistryResult<Vec<IndexerId>> {
            Ok(vec![])
        }

        async fn get_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn get_active_indexing_agreements_by_indexing_request_id(
            &self,
            _request_id: &IndexingRequestId,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn count_accepted_agreements_by_deployment(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<i64> {
            Ok(0)
        }

        async fn register_new_indexing_agreement(
            &self,
            _params: crate::registry::NewAgreementParams,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn register_agreement_with_pending_cancellation(
            &self,
            _params: crate::registry::NewAgreementParams,
            _old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<IndexingAgreementId> {
            Ok(IndexingAgreementId::from_bytes(rand::random()))
        }

        async fn mark_indexing_agreement_as_unresponsive(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn update_offer_tx_hash(
            &self,
            _id: &IndexingAgreementId,
            _tx_hash: &[u8; 32],
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_cancel_for.contains(id) {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "simulated transient failure".into(),
                    )),
                ));
            }
            state.marked_canceled_by_requester.push(*id);
            Ok(())
        }

        async fn mark_indexing_agreement_as_cancelling(
            &self,
            id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_cancel_for.contains(id) {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "simulated transient failure".into(),
                    )),
                ));
            }
            state.marked_cancelling.push(*id);
            Ok(())
        }

        async fn reopen_indexing_agreement_cancel(
            &self,
            id: &IndexingAgreementId,
            _seen_live: bool,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            let Some(agreement) = state.agreements.get_mut(id).filter(|a| {
                matches!(
                    a.status,
                    IndexingAgreementStatus::CanceledByRequester
                        | IndexingAgreementStatus::Rejected
                )
            }) else {
                return Err(crate::registry::Error::NoRecordsUpdated);
            };
            agreement.status = IndexingAgreementStatus::Cancelling;
            state.reopened.push(*id);
            Ok(())
        }

        async fn record_cancel_audit(
            &self,
            agreement_id: &IndexingAgreementId,
            _canceled_at: u64,
            _canceled_by: &str,
            _canceled_tx: Option<&str>,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_cancel_audit {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
            state.recorded_cancel_audit.push(*agreement_id);
            state.audit_writes.push(("cancel", *agreement_id));
            Ok(())
        }

        async fn record_accept_and_cancel_audit(
            &self,
            agreement_id: &IndexingAgreementId,
            _accepted_at: u64,
            _accepted_tx: &str,
            _canceled_at: u64,
            _canceled_by: &str,
            _canceled_tx: Option<&str>,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_cancel_audit {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
            state
                .audit_writes
                .push(("accept and cancel", *agreement_id));
            Ok(())
        }

        async fn record_accepted_audit(
            &self,
            agreement_id: &IndexingAgreementId,
            _accepted_at: u64,
            _accepted_tx: &str,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_accept_audit {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "simulated transient failure".into(),
                    )),
                ));
            }
            state.audit_writes.push(("accept", *agreement_id));
            Ok(())
        }

        async fn apply_reconciliation(
            &self,
            id: &IndexingAgreementId,
            apply_accept: bool,
            cancel: Option<crate::registry::CancelKind>,
        ) -> RegistryResult<crate::registry::ReconciliationOutcome> {
            let mut state = self.state.lock().unwrap();

            if let Some(kind_check) = cancel {
                let _ = kind_check;
                if state.fail_cancel_for.contains(id) {
                    return Err(crate::registry::Error::BackendError(
                        dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                            "simulated transient failure".into(),
                        )),
                    ));
                }
            }

            let original_status = state.agreements.get(id).map(|a| a.status);

            let did_accept = apply_accept
                && matches!(
                    original_status,
                    Some(IndexingAgreementStatus::Created | IndexingAgreementStatus::Expired),
                );

            // The cancel UPDATE in the real tx sees the post-accept status
            // because the accept's UPDATE landed first inside the same
            // transaction.
            let effective_status_for_cancel = if did_accept {
                Some(IndexingAgreementStatus::AcceptedOnChain)
            } else {
                original_status
            };

            let did_cancel = match cancel {
                Some(crate::registry::CancelKind::ByRequester) => matches!(
                    effective_status_for_cancel,
                    Some(
                        IndexingAgreementStatus::Created
                            | IndexingAgreementStatus::AcceptedOnChain
                            | IndexingAgreementStatus::Rejected
                            | IndexingAgreementStatus::Cancelling,
                    ),
                ),
                Some(crate::registry::CancelKind::ByIndexer) => matches!(
                    effective_status_for_cancel,
                    Some(
                        IndexingAgreementStatus::AcceptedOnChain
                            | IndexingAgreementStatus::Cancelling,
                    ),
                ),
                None => false,
            };

            // Roll back only when an accept landed in this tx but the
            // paired cancel matched no row. When both writes matched no
            // row, commit the empty tx and return Ok(no-op).
            if did_accept && !did_cancel && cancel.is_some() {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }

            if did_accept {
                state.marked_accepted_on_chain.push(*id);
            }
            if did_cancel {
                match cancel.expect("did_cancel implies cancel is Some") {
                    crate::registry::CancelKind::ByRequester => {
                        state.marked_canceled_by_requester.push(*id);
                    }
                    crate::registry::CancelKind::ByIndexer => {
                        state.marked_canceled_by_indexer.push(*id);
                    }
                }
            }

            Ok(crate::registry::ReconciliationOutcome {
                did_accept,
                did_cancel,
            })
        }

        async fn apply_reconciliation_batch(
            &self,
            items: &[crate::registry::ReconciliationItem],
        ) -> RegistryResult<
            std::collections::HashMap<IndexingAgreementId, crate::registry::ReconciliationOutcome>,
        > {
            let fails = {
                let state = self.state.lock().unwrap();
                state.fail_batch
                    || items
                        .iter()
                        .any(|item| state.fail_apply_for.contains(&item.agreement_id))
            };
            if fails {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "forced batch failure (test)".into(),
                    )),
                ));
            }
            let mut outcomes = std::collections::HashMap::with_capacity(items.len());
            for item in items {
                let outcome = crate::registry::StubAgreementRegistry::apply_reconciliation(
                    self,
                    &item.agreement_id,
                    item.apply_accept,
                    item.cancel,
                )
                .await?;
                outcomes.insert(item.agreement_id, outcome);
            }
            Ok(outcomes)
        }

        async fn get_expired_created_agreements(
            &self,
            _batch_size: i64,
            _chain_timestamp: u64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        async fn mark_indexing_agreement_as_expired(
            &self,
            _id: &IndexingAgreementId,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn mark_indexing_agreement_as_rejected(
            &self,
            _id: &IndexingAgreementId,
            _rejection_reason: Option<&str>,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn get_accepted_on_chain_agreements(
            &self,
            _batch_size: i64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            Ok(vec![])
        }

        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "predates this lint; fix when next touched"
        )]
        async fn get_agreements_pending_chain_cancel(
            &self,
            batch_size: i64,
        ) -> RegistryResult<Vec<IndexingAgreement>> {
            let state = self.state.lock().unwrap();
            let mut out: Vec<IndexingAgreement> = state
                .agreements
                .values()
                .filter(|a| a.status == IndexingAgreementStatus::AcceptedOnChain)
                .filter(|a| state.canceled_request_ids.contains(&a.indexing_request_id))
                .cloned()
                .collect();
            out.sort_by_key(|a| a.updated_at);
            if batch_size > 0 {
                out.truncate(batch_size as usize);
            }
            Ok(out)
        }

        async fn update_agreement_sync_progress(
            &self,
            _id: &IndexingAgreementId,
            _block_height: u64,
            _progress_at: time::OffsetDateTime,
        ) -> RegistryResult<()> {
            Ok(())
        }

        async fn count_active_agreements_by_deployment(
            &self,
        ) -> RegistryResult<std::collections::HashMap<DeploymentId, usize>> {
            let mut counts: std::collections::HashMap<DeploymentId, usize> =
                std::collections::HashMap::new();
            for agreement in self.state.lock().unwrap().agreements.values() {
                if matches!(
                    agreement.status,
                    IndexingAgreementStatus::Created | IndexingAgreementStatus::AcceptedOnChain,
                ) {
                    *counts
                        .entry(agreement.terms.metadata.subgraph_deployment_id)
                        .or_insert(0) += 1;
                }
            }
            Ok(counts)
        }

        async fn exists_active_agreements(&self) -> RegistryResult<bool> {
            if self.state.lock().unwrap().fail_active_check {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
            let counts =
                crate::registry::StubAgreementRegistry::count_active_agreements_by_deployment(self)
                    .await?;
            Ok(!counts.is_empty())
        }

        async fn count_created_agreements_by_indexer(
            &self,
        ) -> RegistryResult<(
            std::collections::HashMap<thegraph_core::IndexerId, u64>,
            u64,
        )> {
            let mut per_indexer: std::collections::HashMap<thegraph_core::IndexerId, u64> =
                std::collections::HashMap::new();
            let mut global = 0u64;
            for agreement in self.state.lock().unwrap().agreements.values() {
                if matches!(agreement.status, IndexingAgreementStatus::Created) {
                    *per_indexer.entry(agreement.indexer.id).or_insert(0) += 1;
                    global += 1;
                }
            }
            Ok((per_indexer, global))
        }

        async fn get_agreement_fee_rates(&self) -> RegistryResult<Vec<AgreementFeeRate>> {
            Ok(vec![])
        }

        async fn get_agreements_pending_accepted_emission(
            &self,
            _limit: i64,
        ) -> RegistryResult<Vec<crate::registry::PendingAcceptedEvent>> {
            Ok(self.state.lock().unwrap().pending_accepted.clone())
        }
    }

    #[async_trait::async_trait]
    impl PendingCancellationRegistry for MockRegistry {
        async fn get_pending_cancellations_by_new_agreement(
            &self,
            new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<Vec<crate::registry::PendingCancellation>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .pending_cancellations
                .get(&new_agreement_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn delete_pending_cancellations_by_new_agreement(
            &self,
            new_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            self.state
                .lock()
                .unwrap()
                .pending_cancellations
                .remove(&new_agreement_id);
            Ok(())
        }

        async fn delete_pending_cancellation(
            &self,
            new_agreement_id: IndexingAgreementId,
            old_agreement_id: IndexingAgreementId,
        ) -> RegistryResult<()> {
            let mut state = self.state.lock().unwrap();
            if let Some(pcs) = state.pending_cancellations.get_mut(&new_agreement_id) {
                pcs.retain(|pc| pc.old_agreement_id != old_agreement_id);
            }
            state
                .deleted_pending_cancellations
                .push((new_agreement_id, old_agreement_id));
            Ok(())
        }

        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "predates this lint; fix when next touched"
        )]
        async fn list_executable_pending_cancellations(
            &self,
            limit: i64,
        ) -> RegistryResult<Vec<IndexingAgreementId>> {
            let state = self.state.lock().unwrap();
            let mut ids: Vec<IndexingAgreementId> = state
                .pending_cancellations
                .iter()
                .filter(|(_, pcs)| !pcs.is_empty())
                .filter_map(|(new_id, _)| {
                    state
                        .agreements
                        .get(new_id)
                        .filter(|a| matches!(a.status, IndexingAgreementStatus::AcceptedOnChain))
                        .map(|_| *new_id)
                })
                .collect();
            ids.sort();
            ids.truncate(limit.max(0) as usize);
            Ok(ids)
        }
    }

    /// Minimal `ChainClient` mock for chain_listener tests. Records every on-chain cancel
    /// attempt. Every agreement reads as not live, so nothing is sent, unless
    /// `live_until_cancelled` is set.
    #[derive(Clone, Default)]
    struct MockChainClient {
        cancels: Arc<Mutex<Vec<[u8; 16]>>>,
        /// When set, each cancel records whether its agreement was already marked.
        registry: Option<MockRegistry>,
        marked_at_cancel: Arc<Mutex<Vec<bool>>>,
        fail_cancels: bool,
        /// When set, every agreement reads as live until a cancel is sent for it.
        live_until_cancelled: bool,
        /// How many times an agreement was read.
        reads: Arc<std::sync::atomic::AtomicUsize>,
    }

    /// A chain on which every agreement is live until a cancel is sent for it.
    fn live_chain() -> MockChainClient {
        MockChainClient {
            live_until_cancelled: true,
            ..MockChainClient::default()
        }
    }

    impl MockChainClient {
        fn was_on_chain_cancel_attempted(&self, id: &IndexingAgreementId) -> bool {
            self.cancels.lock().unwrap().contains(id.as_bytes())
        }
    }

    #[async_trait::async_trait]
    impl crate::chain_client::ChainClient for MockChainClient {
        async fn latest_block_timestamp(
            &self,
        ) -> Result<u64, crate::chain_client::ChainClientError> {
            // Err by default so a test must mock this explicitly to take the
            // live-chain-head path instead of silently reading timestamp 0.
            Err(crate::chain_client::ChainClientError::RpcError(
                anyhow::anyhow!("latest_block_timestamp not mocked"),
            ))
        }

        async fn offer_via_manager(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<
            Option<thegraph_core::alloy::primitives::B256>,
            crate::chain_client::ChainClientError,
        > {
            Ok(None)
        }

        async fn cancel_via_manager(
            &self,
            _collector: thegraph_core::alloy::primitives::Address,
            agreement_id: &[u8; 16],
            _version_hash: thegraph_core::alloy::primitives::B256,
            _options: u16,
        ) -> Result<
            Option<thegraph_core::alloy::primitives::B256>,
            crate::chain_client::ChainClientError,
        > {
            if self.fail_cancels {
                return Err(crate::chain_client::ChainClientError::RpcError(
                    anyhow::anyhow!("rpc down"),
                ));
            }
            // Record manager-routed cancels through the same recorder so the
            // existing assertions hold.
            self.cancels.lock().unwrap().push(*agreement_id);
            if let Some(registry) = &self.registry {
                let id = IndexingAgreementId::from_bytes(*agreement_id);
                let was_marked = registry.was_marked_cancelling(&id);
                self.marked_at_cancel.lock().unwrap().push(was_marked);
            }
            // A manager-routed cancel has no "already canceled" result: the
            // contract silently no-ops a stale cancel and the tx still succeeds,
            // so the real cancel_via_manager never returns Ok(None).
            Ok(Some(thegraph_core::alloy::primitives::B256::ZERO))
        }

        async fn reconcile_provider(
            &self,
            _collector: thegraph_core::alloy::primitives::Address,
            _provider: thegraph_core::alloy::primitives::Address,
        ) -> Result<
            Option<thegraph_core::alloy::primitives::B256>,
            crate::chain_client::ChainClientError,
        > {
            Ok(None)
        }
        async fn reconcile_agreement(
            &self,
            _collector: thegraph_core::alloy::primitives::Address,
            _agreement_id: &[u8; 16],
        ) -> Result<
            Option<thegraph_core::alloy::primitives::B256>,
            crate::chain_client::ChainClientError,
        > {
            Ok(None)
        }

        async fn agreement_on_chain(
            &self,
            agreement_id: &[u8; 16],
        ) -> Result<crate::chain_client::AgreementOnChain, crate::chain_client::ChainClientError>
        {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Cancel dispatch always reads back after a mined cancel; reporting
            // not-active here means "cancel confirmed", which these tests expect.
            Ok(crate::chain_client::AgreementOnChain::live_if(
                self.live_until_cancelled && !self.cancels.lock().unwrap().contains(agreement_id),
            ))
        }
    }

    /// A chain check the listener queued: the agreement, and the accept it carried.
    pub(super) type QueuedCheck = (IndexingAgreementId, Option<ChainAccept>);

    /// Records the chain checks queued; queuing fails while `fail` is set.
    #[derive(Clone, Default)]
    pub(super) struct MockWorkerQueue {
        checks: Arc<Mutex<Vec<QueuedCheck>>>,
        fail: Arc<std::sync::atomic::AtomicBool>,
        reassessments: Arc<Mutex<Vec<QueuedReassessment>>>,
        /// Requests listed as already having a reassessment queued or running.
        reassessing: Arc<Mutex<std::collections::HashSet<IndexingRequestId>>>,
        fail_reassessments: Arc<std::sync::atomic::AtomicBool>,
        /// How many times the queued reassessments were listed.
        listings: Arc<std::sync::atomic::AtomicUsize>,
    }

    /// A reassessment queued: the request, and the target it was queued with.
    type QueuedReassessment = (IndexingRequestId, usize);

    impl MockWorkerQueue {
        pub(super) fn queued(&self) -> Vec<QueuedCheck> {
            self.checks.lock().unwrap().clone()
        }

        fn reassessments(&self) -> Vec<QueuedReassessment> {
            self.reassessments.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl WorkerQueue for MockWorkerQueue {
        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: url::Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: JobPriority,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            unimplemented!()
        }

        async fn reassess_indexing_request(
            &self,
            indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            num_candidates: usize,
            _priority: JobPriority,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            if self
                .fail_reassessments
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                anyhow::bail!("queue unavailable");
            }
            self.reassessments
                .lock()
                .unwrap()
                .push((indexing_request_id, num_candidates));
            // Listed from now on, as the real queue lists the jobs waiting in it.
            self.reassessing.lock().unwrap().insert(indexing_request_id);
            Ok(dipper_pgmq::JobId::default())
        }

        async fn queued_reassessments(
            &self,
        ) -> anyhow::Result<std::collections::HashSet<IndexingRequestId>> {
            self.listings
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(self.reassessing.lock().unwrap().clone())
        }

        async fn cancel_rejected_agreement_on_chain(
            &self,
            agreement_id: IndexingAgreementId,
            accept: Option<ChainAccept>,
            _priority: JobPriority,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("queue unavailable");
            }
            self.checks.lock().unwrap().push((agreement_id, accept));
            Ok(dipper_pgmq::JobId::default())
        }

        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: url::Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: JobPriority,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            unimplemented!()
        }
    }

    fn snapshot_accept() -> Option<ChainAccept> {
        Some(ChainAccept {
            at: TEST_ACCEPTED_AT,
            tx: TEST_ACCEPTED_TX.to_string(),
        })
    }

    /// The target of every open request the mock registry hands out.
    const REQUEST_TARGET: usize = 3;

    /// Answers for the requests the mock's agreements name: open with [`REQUEST_TARGET`]
    /// indexers, or cancelled once marked so. A request no agreement names doesn't exist.
    #[async_trait::async_trait]
    impl crate::registry::IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
        ) -> RegistryResult<crate::registry::SetTargetOutcome> {
            unimplemented!()
        }

        async fn get_all_indexing_requests(
            &self,
        ) -> RegistryResult<Vec<crate::registry::IndexingRequest>> {
            unimplemented!()
        }

        async fn get_indexing_request_by_id(
            &self,
            id: &IndexingRequestId,
        ) -> RegistryResult<Option<crate::registry::IndexingRequest>> {
            let state = self.state.lock().unwrap();
            let Some(agreement) = state
                .agreements
                .values()
                .find(|agreement| agreement.indexing_request_id == *id)
            else {
                return Ok(None);
            };
            let status = if state.canceled_request_ids.contains(id) {
                crate::registry::IndexingRequestStatus::Canceled
            } else {
                crate::registry::IndexingRequestStatus::Open
            };
            Ok(Some(crate::registry::IndexingRequest {
                id: *id,
                created_at: OffsetDateTime::now_utc(),
                updated_at: OffsetDateTime::now_utc(),
                status,
                requested_by: Address::ZERO,
                deployment_id: agreement.terms.metadata.subgraph_deployment_id,
                deployment_chain_id: agreement.terms.metadata.chain_id,
                num_candidates: REQUEST_TARGET,
            }))
        }

        async fn get_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> RegistryResult<Vec<crate::registry::IndexingRequest>> {
            unimplemented!()
        }

        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age_seconds: i64,
            _batch_size: i64,
        ) -> RegistryResult<Vec<crate::registry::IndexingRequest>> {
            unimplemented!()
        }

        async fn get_short_requests_on_deployments_with_ended_agreements(
            &self,
            _since_seconds: i64,
            _batch_size: i64,
        ) -> RegistryResult<Vec<crate::registry::IndexingRequest>> {
            unimplemented!()
        }
    }

    /// Drain 1 page holding `snapshot` into `registry`, queueing on `queue`.
    async fn drain_page_of(
        snapshot: AgreementStateSnapshot,
        registry: &MockRegistry,
        queue: MockWorkerQueue,
    ) -> DrainOutcome {
        drain_page_holding(vec![snapshot], registry, queue).await
    }

    /// Drain 1 page holding `snapshots` into `registry`, queueing on `queue`.
    async fn drain_page_holding(
        snapshots: Vec<AgreementStateSnapshot>,
        registry: &MockRegistry,
        queue: MockWorkerQueue,
    ) -> DrainOutcome {
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        let last_block = snapshots
            .iter()
            .map(|snapshot| snapshot.last_state_change_block)
            .max()
            .unwrap_or_default();
        event_source.set_latest_block(last_block + 10);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        event_source.add_snapshots(snapshots);
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        drain_once(
            &mut Cursor::genesis(),
            &mut None,
            &mut None,
            &mut 0,
            &mut 0,
            &mut 0,
            1337,
            0,
            10,
            false,
            registry,
            &MockChainClient::default(),
            &mut LiveChecks::new(queue),
            &event_source,
            &mut rx_stop,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("the subgraph fetch succeeds")
    }

    /// An accepted agreement whose indexer cancelled it on-chain leaves its request a slot short,
    /// and only a reassessment fills it before the daily pass: the hourly top-up and the sweep
    /// of freed requests both pass over a request no reassessment has marked short.
    #[tokio::test]
    async fn reconciling_an_indexer_cancel_queues_a_reassessment_of_its_request() {
        let registry = MockRegistry::new();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        let request_id = IndexingRequestId::new();
        registry.set_agreement_request_id(agreement_id, request_id);
        let queue = MockWorkerQueue::default();

        let outcome = drain_page_of(
            make_snapshot(
                agreement_id,
                AgreementState::CanceledByServiceProvider,
                Address::ZERO,
            ),
            &registry,
            queue.clone(),
        )
        .await;

        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert_eq!(queue.reassessments(), [(request_id, REQUEST_TARGET)]);
        assert_eq!((outcome.processed, outcome.errors), (1, 0));
    }

    /// A request with a reassessment already queued or running gets no second one: that 1 reads
    /// the agreements as they stand when it runs.
    #[tokio::test]
    async fn an_indexer_cancel_queues_no_reassessment_while_one_is_queued() {
        let registry = MockRegistry::new();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        let request_id = IndexingRequestId::new();
        registry.set_agreement_request_id(agreement_id, request_id);
        let queue = MockWorkerQueue::default();
        queue.reassessing.lock().unwrap().insert(request_id);

        drain_page_of(
            make_snapshot(
                agreement_id,
                AgreementState::CanceledByServiceProvider,
                Address::ZERO,
            ),
            &registry,
            queue.clone(),
        )
        .await;

        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert_eq!(queue.reassessments(), []);
    }

    /// The cancel is on record whether or not its reassessment could be queued, so a queue
    /// that is down leaves the change applied and says so, rather than failing the page.
    #[tokio::test]
    async fn an_indexer_cancel_stays_recorded_when_its_reassessment_cannot_be_queued() {
        let registry = MockRegistry::new();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        let queue = MockWorkerQueue::default();
        queue
            .fail_reassessments
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let (outcome, logs) = {
            let mut outcome = None;
            let logs = crate::test_support::logged(async {
                outcome = Some(
                    drain_page_of(
                        make_snapshot(
                            agreement_id,
                            AgreementState::CanceledByServiceProvider,
                            Address::ZERO,
                        ),
                        &registry,
                        queue.clone(),
                    )
                    .await,
                );
            })
            .await;
            (outcome.expect("the drain ran"), logs)
        };

        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert_eq!((outcome.processed, outcome.errors), (1, 0));
        let line = logs
            .lines()
            .find(|line| line.contains("queue unavailable"))
            .unwrap_or_else(|| panic!("the failed queueing is logged:\n{logs}"));
        assert!(line.contains("WARN"), "{line}");
    }

    /// Listing the queued reassessments reads every unfinished job, so a page of indexer cancels
    /// lists them once, not once per cancel.
    #[tokio::test]
    async fn a_page_of_indexer_cancels_lists_the_queued_reassessments_once() {
        let registry = MockRegistry::new();
        let queue = MockWorkerQueue::default();
        let mut snapshots = Vec::new();
        let mut requests = Vec::new();
        for _ in 0..3 {
            let agreement_id = IndexingAgreementId::from_bytes(rand::random());
            registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
            let request_id = IndexingRequestId::new();
            registry.set_agreement_request_id(agreement_id, request_id);
            requests.push((request_id, REQUEST_TARGET));
            snapshots.push(make_snapshot(
                agreement_id,
                AgreementState::CanceledByServiceProvider,
                Address::ZERO,
            ));
        }

        drain_page_holding(snapshots, &registry, queue.clone()).await;

        let mut queued = queue.reassessments();
        queued.sort();
        requests.sort();
        assert_eq!(queued, requests);
        assert_eq!(queue.listings.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// 2 indexer cancels on 1 request in a page queue 1 reassessment, as the first one queued
    /// counts as queued for the second.
    #[tokio::test]
    async fn indexer_cancels_on_1_request_in_a_page_queue_1_reassessment() {
        let registry = MockRegistry::new();
        let queue = MockWorkerQueue::default();
        let request_id = IndexingRequestId::new();
        let mut snapshots = Vec::new();
        for _ in 0..2 {
            let agreement_id = IndexingAgreementId::from_bytes(rand::random());
            registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
            registry.set_agreement_request_id(agreement_id, request_id);
            snapshots.push(make_snapshot(
                agreement_id,
                AgreementState::CanceledByServiceProvider,
                Address::ZERO,
            ));
        }

        drain_page_holding(snapshots, &registry, queue.clone()).await;

        assert_eq!(queue.reassessments(), [(request_id, REQUEST_TARGET)]);
    }

    /// A cancel by dipper itself needs no reassessment from here: the paths that end an agreement
    /// for dipper queue their own.
    #[tokio::test]
    async fn a_cancel_by_dipper_queues_no_reassessment() {
        let registry = MockRegistry::new();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        let queue = MockWorkerQueue::default();

        drain_page_of(
            make_snapshot(agreement_id, AgreementState::CanceledByPayer, Address::ZERO),
            &registry,
            queue.clone(),
        )
        .await;

        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
        assert_eq!(queue.reassessments(), []);
    }

    // -- reconcile_agreement tests --

    #[tokio::test]
    async fn test_reconcile_transitions_created_to_accepted() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!registry.was_reopened(&agreement_id));
    }

    #[tokio::test]
    async fn reconcile_records_the_accept_of_an_agreement_being_cancelled() {
        // It stays cancelling, and the recorded accept lets its end be announced.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Cancelling);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("reconcile ok");

        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert_eq!(registry.audit_writes(), vec![("accept", agreement_id)]);
        assert!(!registry.was_reopened(&agreement_id));
    }

    #[tokio::test]
    async fn reconcile_records_no_accept_for_a_withdrawn_offer() {
        // The subgraph reports a withdrawn offer as cancelled by the payer with no accept
        // time; recording it would announce an agreement that was never live.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Cancelling);
        let mut snapshot =
            make_snapshot(agreement_id, AgreementState::CanceledByPayer, Address::ZERO);
        snapshot.accepted_at = 0;

        reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("reconcile ok");

        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(
            registry
                .audit_writes()
                .iter()
                .all(|(kind, _)| *kind != "accept")
        );
    }

    #[tokio::test]
    async fn reconcile_does_not_accept_an_offer_withdrawn_before_anyone_accepted_it() {
        // Announcing it would send accepted and terminated events for an agreement that
        // was never live; an expired one stays expired.
        for status in [
            IndexingAgreementStatus::Created,
            IndexingAgreementStatus::Expired,
        ] {
            let registry = MockRegistry::new();
            let chain_client = MockChainClient::default();
            let agreement_id = IndexingAgreementId::from_bytes(rand::random());
            registry.add_agreement(agreement_id, status);
            let mut snapshot =
                make_snapshot(agreement_id, AgreementState::CanceledByPayer, Address::ZERO);
            snapshot.accepted_at = 0;

            reconcile_agreement(
                &snapshot,
                &registry,
                &chain_client,
                test_agreement_conf().as_ref(),
            )
            .await
            .expect("reconcile ok");

            assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
            assert_eq!(
                registry.was_marked_canceled_by_requester(&agreement_id),
                status == IndexingAgreementStatus::Created
            );
            assert!(
                registry
                    .audit_writes()
                    .iter()
                    .all(|(kind, _)| *kind != "accept")
            );
        }
    }

    #[tokio::test]
    async fn reconcile_marks_an_agreement_being_cancelled_once_the_chain_shows_it_ended() {
        for (state, ended_by_dipper) in [
            (AgreementState::CanceledByPayer, true),
            (AgreementState::CanceledByServiceProvider, false),
        ] {
            let registry = MockRegistry::new();
            let chain_client = MockChainClient::default();
            let agreement_id = IndexingAgreementId::from_bytes(rand::random());
            registry.add_agreement(agreement_id, IndexingAgreementStatus::Cancelling);

            let snapshot = make_snapshot(agreement_id, state, Address::ZERO);
            reconcile_agreement(
                &snapshot,
                &registry,
                &chain_client,
                test_agreement_conf().as_ref(),
            )
            .await
            .expect("reconcile ok");

            assert_eq!(
                registry.was_marked_canceled_by_requester(&agreement_id),
                ended_by_dipper
            );
            assert_eq!(
                registry.was_marked_canceled_by_indexer(&agreement_id),
                !ended_by_dipper
            );
        }
    }

    #[tokio::test]
    async fn reconcile_accept_marks_accepted_without_emitting() {
        use crate::test_support::CapturingEventsProducer;

        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let events = CapturingEventsProducer::new();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("reconcile ok");

        // reconcile flips the row to AcceptedOnChain (and, in production, persists
        // the accept audit in the same transaction). It no longer emits
        // `accepted` -- `sweep_pending_accepted_events` is the sole emitter.
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(
            events.events().is_empty(),
            "reconcile emits no accepted event; the sweep does"
        );
    }

    #[tokio::test]
    async fn reconcile_indexer_cancel_marks_canceled_without_emitting() {
        use crate::test_support::CapturingEventsProducer;

        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let events = CapturingEventsProducer::new();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        // Indexer-initiated on-chain cancel: canceled_by != signer (ZERO) -> ByIndexer.
        let indexer: Address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();
        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            indexer,
        );
        reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("reconcile ok");

        // reconcile flips the row to CanceledByIndexer (and, in production,
        // persists the cancel audit in the same transaction). It no longer emits
        // `terminated` -- `sweep_pending_terminated_events` is the sole emitter.
        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(
            events.events().is_empty(),
            "reconcile emits no terminated event; the sweep does"
        );
    }

    #[tokio::test]
    async fn test_reconcile_queues_a_chain_check_for_an_ended_agreement_accepted_on_chain() {
        // Dipper rejected or cancelled it, but the indexer accepted it on-chain anyway, such as an
        // offer that landed after dipper's cancel. The chain is read by the queued job, not here,
        // so a slow read can't hold up the page, and the job records the accept if it reopens it.
        for status in [
            IndexingAgreementStatus::Rejected,
            IndexingAgreementStatus::CanceledByRequester,
        ] {
            let registry = MockRegistry::new();
            let chain_client = live_chain();
            let agreement_id = IndexingAgreementId::from_bytes(rand::random());
            registry.add_agreement(agreement_id, status);

            let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
            let queued = reconcile_agreement(
                &snapshot,
                &registry,
                &chain_client,
                test_agreement_conf().as_ref(),
            )
            .await
            .expect("reconcile ok");

            assert_eq!(queued, vec![(agreement_id, snapshot_accept())], "{status}");
            assert_eq!(
                chain_client.reads.load(std::sync::atomic::Ordering::SeqCst),
                0,
                "{status}"
            );
            assert!(!registry.was_reopened(&agreement_id), "{status}");
            assert!(registry.audit_writes().is_empty(), "{status}");
            assert!(
                !registry.was_marked_accepted_on_chain(&agreement_id),
                "{status}"
            );
        }
    }

    #[tokio::test]
    async fn test_reconcile_rejected_already_canceled_skips_queue_and_marks_local_cancel() {
        // The Rejected agreement was canceled on-chain between polls. Dipper
        // should not queue another cancel job (it would just revert against
        // the already-canceled contract state), and step 2 should drive the
        // local status from Rejected straight to the terminal cancel matching
        // the canceler address.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Rejected);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.expect("reconcile ok").is_empty(), "no check queued");
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_ignores_unknown_agreement() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        // Don't add the agreement to the registry

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(!registry.was_reopened(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_recovers_expired_agreement() {
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let old_agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Expired);
        registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(agreement_id, old_agreement_id);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&old_agreement_id));
        assert!(registry.was_pending_cancellation_deleted(&agreement_id, &old_agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_marks_canceled_by_indexer() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let indexer_address: Address = "0x1234567890123456789012345678901234567890"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            indexer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_marks_canceled_by_requester() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_rejected_payer_cancel_recovers_by_requester() {
        // A Rejected row observed canceled on-chain by the payer. The canceler
        // is the payer, never dipper's signer, so classification must come from
        // the state: CanceledByPayer -> ByRequester, the only kind allowed here.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        // A payer address deliberately distinct from dipper's signer key.
        let payer_address: Address = "0xcccccccccccccccccccccccccccccccccccccccc"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Rejected);

        let snapshot = make_snapshot(agreement_id, AgreementState::CanceledByPayer, payer_address);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        // Already canceled on-chain: must not queue a fresh check.
        assert!(result.expect("reconcile ok").is_empty());
        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_cancelled_agreement_already_cancelled_on_chain_queues_nothing() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByRequester);

        let snapshot = make_snapshot(agreement_id, AgreementState::CanceledByPayer, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.expect("reconcile ok").is_empty());
    }

    #[tokio::test]
    async fn test_reconcile_records_accept_and_cancel_of_cancelled_agreement_that_went_live() {
        // Dipper had marked the agreement cancelled, but it was accepted on-chain
        // before being ended there. Recording both lets the accepted and terminated
        // events go out, in 1 write, as the terminated sweep only waits for the accept.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByRequester);
        // The listener lagged: dipper only cancelled it locally long after the offer's
        // deadline, which once made this accept look too old to announce.
        registry.set_agreement_deadline_from_now(agreement_id, -2 * 86_400);

        let snapshot = make_snapshot(agreement_id, AgreementState::CanceledByPayer, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(
            registry.audit_writes(),
            vec![("accept and cancel", agreement_id)]
        );
    }

    #[tokio::test]
    async fn test_reconcile_records_the_end_of_an_abandoned_agreement() {
        // Dipper can mark it ended without the transaction that ended it, and its terminated
        // event waits for that transaction, which only this record supplies.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AbandonedByIndexer);

        let snapshot = make_snapshot(agreement_id, AgreementState::CanceledByPayer, Address::ZERO);
        reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("reconcile ok");

        assert_eq!(
            registry.audit_writes(),
            vec![("accept and cancel", agreement_id)]
        );
    }

    #[tokio::test]
    async fn test_reconcile_records_nothing_for_cancelled_agreement_never_accepted() {
        // A withdrawn offer was never accepted, so there is nothing to announce.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByRequester);

        let mut snapshot =
            make_snapshot(agreement_id, AgreementState::CanceledByPayer, Address::ZERO);
        snapshot.accepted_at = 0;
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.audit_writes().is_empty());
    }

    #[tokio::test]
    async fn test_reconcile_records_nothing_while_cancelled_agreement_is_still_live() {
        // Recording the accept now would let the terminated event go out before the
        // agreement has actually ended on-chain.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByRequester);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.audit_writes().is_empty());
    }

    #[tokio::test]
    async fn test_reconcile_ignores_already_canceled() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::CanceledByIndexer);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            Address::ZERO,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_no_op_when_local_status_blocks_cancel_filter() {
        // Local is in a terminal-but-not-cancel status (Unresponsive),
        // remote snapshot says canceled. The chain_listener's Rust-side
        // already_terminal_cancel guard does not catch Unresponsive,
        // so apply_reconciliation is invoked with apply_accept=false and
        // cancel=Some(...). The cancel UPDATE matches no row because
        // Unresponsive is not in the allowed_from list. The function
        // must commit the empty tx and return Ok with both flags false,
        // so the chain_listener treats the snapshot as a successful no-op
        // rather than incrementing its `errors` counter.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let signer_address: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Unresponsive);

        let snapshot = make_snapshot(
            agreement_id,
            AgreementState::CanceledByPayer,
            signer_address,
        );
        let result = reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(!registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_cancels_the_replaced_agreement_once_its_replacement_is_accepted() {
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let old_agreement_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(agreement_id, old_agreement_id);

        let snapshot = make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        reconcile_agreement(
            &snapshot,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await
        .unwrap();

        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(chain_client.was_on_chain_cancel_attempted(&old_agreement_id));
        assert!(registry.was_pending_cancellation_deleted(&agreement_id, &old_agreement_id));
    }

    #[tokio::test]
    async fn test_reconcile_accept_then_cancel_in_one_snapshot_keeps_the_replaced_agreement() {
        // One poll shows a replacement accepted and already cancelled, by the payer or by its
        // indexer. Both are recorded, but the agreement it was to replace is left running:
        // cancelling that one too would leave the request with neither.
        for (ended, by_indexer) in [
            (AgreementState::CanceledByPayer, false),
            (AgreementState::CanceledByServiceProvider, true),
        ] {
            let registry = MockRegistry::new();
            let chain_client = live_chain();
            let agreement_id = IndexingAgreementId::from_bytes(rand::random());
            let old_agreement_id = IndexingAgreementId::from_bytes(rand::random());
            let canceler: Address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .parse()
                .unwrap();
            let conf = test_agreement_conf();

            registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
            registry.add_agreement(old_agreement_id, IndexingAgreementStatus::AcceptedOnChain);
            registry.add_pending_cancellation(agreement_id, old_agreement_id);

            let snapshot = make_snapshot(agreement_id, ended, canceler);
            reconcile_agreement(&snapshot, &registry, &chain_client, conf.as_ref())
                .await
                .unwrap();

            // The replacement's accept and its cancel are both on record.
            assert!(
                registry.was_marked_accepted_on_chain(&agreement_id),
                "{ended:?}"
            );
            assert_eq!(
                registry.was_marked_canceled_by_indexer(&agreement_id),
                by_indexer,
                "{ended:?}"
            );
            assert_eq!(
                registry.was_marked_canceled_by_requester(&agreement_id),
                !by_indexer,
                "{ended:?}"
            );
            // The agreement it was to replace is untouched, on chain and in the registry.
            assert!(
                !chain_client.was_on_chain_cancel_attempted(&old_agreement_id),
                "{ended:?}"
            );
            assert!(
                !registry.was_marked_cancelling(&old_agreement_id),
                "{ended:?}"
            );
            // The link between the 2 is gone, so nothing can act on it later.
            let left = registry
                .get_pending_cancellations_by_new_agreement(agreement_id)
                .await
                .unwrap();
            assert!(left.is_empty(), "{ended:?}");
        }
    }

    // -- execute_pending_cancellations tests --

    #[tokio::test]
    async fn test_pending_cancellations_all_succeed_records_deleted() {
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id_1 = IndexingAgreementId::from_bytes(rand::random());
        let old_id_2 = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_1, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id_2, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id_1);
        registry.add_pending_cancellation(new_id, old_id_2);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_requester(&old_id_1));
        assert!(registry.was_marked_canceled_by_requester(&old_id_2));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_1));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id_2));
        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_pending_cancellations_records_cancel_audit() {
        // execute_pending no longer emits `terminated` directly; it records the
        // cancel audit and the chain_listener sweep announces it durably.
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_canceled_by_requester(&old_id));
        assert!(
            registry.was_cancel_audit_recorded(&old_id),
            "cancel audit recorded so the sweep can emit `terminated`"
        );
    }

    #[tokio::test]
    async fn test_pending_cancellations_leave_a_never_accepted_agreement_cancelling() {
        // Its offer could still land and be accepted until the deadline, so the cancel
        // retry finishes it then. No cancel is recorded: one would win over the chain's
        // own if the indexer accepted it after all, reporting an end before the accept.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::Created);
        registry.add_pending_cancellation(new_id, old_id);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(registry.was_marked_cancelling(&old_id));
        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        assert!(!registry.was_cancel_audit_recorded(&old_id));
    }

    /// Runs the pending cancellation of 1 old agreement in `status`, returning
    /// whether it was already marked cancelling when its on-chain cancel went out.
    async fn marked_at_pending_cancel(status: IndexingAgreementStatus) -> Vec<bool> {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient {
            registry: Some(registry.clone()),
            live_until_cancelled: true,
            ..MockChainClient::default()
        };
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, status);
        registry.add_pending_cancellation(new_id, old_id);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok(), "got {result:?}");
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
        chain_client.marked_at_cancel.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn test_pending_cancellations_mark_an_unaccepted_agreement_before_its_cancel() {
        // Its offer may be in flight. An offer landing after the cancel can't be
        // withdrawn by it, so its job must find the agreement already marked.
        let marked = marked_at_pending_cancel(IndexingAgreementStatus::Created).await;
        assert_eq!(marked, vec![true]);
    }

    #[tokio::test]
    async fn test_pending_cancellations_cancel_an_expired_agreement_only_if_live() {
        // A lagging listener can mark an agreement expired that was in fact accepted; one
        // that really expired keeps its status, and no needless cancel is sent.
        for live in [true, false] {
            let registry = MockRegistry::new();
            let chain_client = MockChainClient {
                live_until_cancelled: live,
                ..MockChainClient::default()
            };
            let new_id = IndexingAgreementId::from_bytes(rand::random());
            let old_id = IndexingAgreementId::from_bytes(rand::random());
            registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
            registry.add_agreement(old_id, IndexingAgreementStatus::Expired);
            registry.add_pending_cancellation(new_id, old_id);

            let result = execute_pending_cancellations(
                &new_id,
                &registry,
                &chain_client,
                test_agreement_conf().as_ref(),
            )
            .await;

            assert!(result.is_ok(), "got {result:?}");
            assert_eq!(registry.was_marked_cancelling(&old_id), live);
            assert_eq!(chain_client.was_on_chain_cancel_attempted(&old_id), live);
            assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
            // Once before the cancel, and once after it to confirm it worked.
            let reads = if live { 2 } else { 1 };
            assert_eq!(
                chain_client.reads.load(std::sync::atomic::Ordering::SeqCst),
                reads
            );
        }
    }

    #[tokio::test]
    async fn test_pending_cancellations_mark_an_accepted_agreement_before_its_cancel() {
        // A failed cancel then leaves it cancelling, which the cancel retry picks up.
        let marked = marked_at_pending_cancel(IndexingAgreementStatus::AcceptedOnChain).await;
        assert_eq!(marked, vec![true]);
    }

    #[tokio::test]
    async fn test_pending_cancellations_leave_a_failed_cancel_to_the_cancel_retry() {
        // Retried from here, a cancel that never works was resent on every sweep with no
        // limit, for as long as the replacement stayed accepted. The cancel retry limits it.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient {
            fail_cancels: true,
            ..MockChainClient::default()
        };
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok(), "got {result:?}");
        assert!(registry.was_marked_cancelling(&old_id));
        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        assert!(!registry.was_cancel_audit_recorded(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_pending_cancellations_failed_cancel_records_no_audit() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_fail = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_fail, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_fail);
        registry.fail_cancel_for(old_fail);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        // The row couldn't be marked, so no cancel went out, nothing is recorded (the
        // sweep emits nothing), and the pending row stays for a retry.
        assert!(result.is_err());
        assert!(!chain_client.was_on_chain_cancel_attempted(&old_fail));
        assert!(!registry.was_marked_canceled_by_requester(&old_fail));
        assert!(!registry.was_cancel_audit_recorded(&old_fail));
    }

    #[tokio::test]
    async fn test_pending_cancellations_transient_failure_retains_record() {
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_ok = IndexingAgreementId::from_bytes(rand::random());
        let old_fail = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_ok, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_fail, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_ok);
        registry.add_pending_cancellation(new_id, old_fail);
        registry.fail_cancel_for(old_fail);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("1 pending cancellation(s) failed"),
            "unexpected error: {err_msg}"
        );

        assert!(registry.was_marked_canceled_by_requester(&old_ok));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_ok));

        assert!(!registry.was_marked_canceled_by_requester(&old_fail));
        assert!(!registry.was_pending_cancellation_deleted(&new_id, &old_fail));

        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].old_agreement_id, old_fail);
    }

    #[tokio::test]
    async fn test_pending_cancellations_nonexistent_agreement_cleans_up() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id);
        // old_id never added -- simulates a stale pending cancellation whose
        // referenced agreement no longer exists.

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_pending_cancellations_empty_is_noop() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pending_cancellations_already_canceled_on_chain_succeeds() {
        // Crash-recovery edge case: the cancel landed on a prior pass, but dipper
        // crashed before deleting the pending row. The chain shows nothing live, so no
        // cancel is sent; the row is left cancelling for the cancel retry to confirm
        // and the pending row is deleted rather than retried forever.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id);

        let result = execute_pending_cancellations(
            &new_id,
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(
            result.is_ok(),
            "expected idempotent success, got {result:?}"
        );
        assert!(!chain_client.was_on_chain_cancel_attempted(&old_id));
        assert!(registry.was_marked_cancelling(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_sweep_clears_already_canceled_pending_rows() {
        // The sweep is the long-lived recovery path. If the on-chain cancel
        // has already happened but the pending row survived, the sweep must
        // tear it down on the next poll cycle without an error.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id);

        sweep_executable_pending_cancellations(
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(registry.was_marked_cancelling(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
        let remaining = registry
            .get_pending_cancellations_by_new_agreement(new_id)
            .await
            .unwrap();
        assert!(remaining.is_empty());
    }

    // -- sweep_executable_pending_cancellations tests --

    #[tokio::test]
    async fn test_sweep_recovers_orphaned_pending_cancellations() {
        // Crash recovery scenario: a prior reconcile committed the new
        // agreement to AcceptedOnChain but execute_pending_cancellations
        // never ran (or crashed mid-fanout), so the pending row lingers
        // and the old agreement is still alive. The sweep must complete
        // the cancellation without needing another snapshot to arrive.
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id);

        sweep_executable_pending_cancellations(
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(registry.was_marked_canceled_by_requester(&old_id));
        assert!(registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_sweep_skips_when_new_agreement_not_accepted() {
        // The sweep must not act on pending rows whose new agreement is
        // still Created — those belong to the normal reconcile path and
        // running the cancellation early would prematurely kill the old
        // agreement before the replacement is on-chain.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let new_id = IndexingAgreementId::from_bytes(rand::random());
        let old_id = IndexingAgreementId::from_bytes(rand::random());

        registry.add_agreement(new_id, IndexingAgreementStatus::Created);
        registry.add_agreement(old_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.add_pending_cancellation(new_id, old_id);

        sweep_executable_pending_cancellations(
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;

        assert!(!registry.was_marked_canceled_by_requester(&old_id));
        assert!(!registry.was_pending_cancellation_deleted(&new_id, &old_id));
    }

    #[tokio::test]
    async fn test_sweep_no_orphans_is_noop() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();

        sweep_executable_pending_cancellations(
            &registry,
            &chain_client,
            test_agreement_conf().as_ref(),
        )
        .await;
    }

    // -- notify wakeup integration test --

    #[async_trait::async_trait]
    impl ChainListenerStateRegistry for MockRegistry {
        async fn get_chain_listener_state(
            &self,
            _chain_id: u64,
        ) -> crate::registry::Result<Option<ChainListenerState>> {
            let last_processed_block = self.state.lock().unwrap().initial_last_processed_block;
            Ok(Some(ChainListenerState {
                _chain_id: 1337,
                last_processed_block,
                last_processed_id: None,
                last_processed_block_timestamp: None,
            }))
        }

        async fn update_chain_listener_state(
            &self,
            _chain_id: u64,
            cursor: &Cursor,
            last_processed_block_timestamp: Option<u64>,
        ) -> crate::registry::Result<()> {
            self.state
                .lock()
                .unwrap()
                .chain_listener_state_updates
                .push((cursor.clone(), last_processed_block_timestamp));
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl crate::registry::SetAsideChangeRegistry for MockRegistry {
        async fn record_set_aside_changes(
            &self,
            changes: &[crate::registry::SetAsideChange],
        ) -> crate::registry::Result<()> {
            let mut state = self.state.lock().unwrap();
            if state.fail_set_aside {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "forced set-aside failure (test)".into(),
                    )),
                ));
            }
            for change in changes {
                match state
                    .set_aside
                    .iter_mut()
                    .find(|kept| kept.agreement_id == change.agreement_id)
                {
                    Some(kept) => kept.block = kept.block.max(change.block),
                    None => state.set_aside.push(*change),
                }
            }
            Ok(())
        }

        async fn get_set_aside_changes(
            &self,
        ) -> crate::registry::Result<Vec<crate::registry::SetAsideChange>> {
            if self.state.lock().unwrap().fail_set_aside_read {
                return Err(crate::registry::Error::BackendError(
                    dipper_pgregistry::Error::DbError(sqlx::Error::Protocol(
                        "forced set-aside read failure (test)".into(),
                    )),
                ));
            }
            Ok(self.set_aside())
        }

        async fn delete_set_aside_changes(
            &self,
            agreement_ids: &[IndexingAgreementId],
        ) -> crate::registry::Result<()> {
            self.state
                .lock()
                .unwrap()
                .set_aside
                .retain(|change| !agreement_ids.contains(&change.agreement_id));
            Ok(())
        }
    }

    /// A mock event source that records when it was polled.
    struct TimingEventSource {
        poll_times: Arc<Mutex<Vec<tokio::time::Instant>>>,
    }

    #[async_trait::async_trait]
    impl super::super::chain_events::ChainEventSource for TimingEventSource {
        async fn get_changed_agreements(
            &self,
            _since: &Cursor,
            _pinned_block: Option<u64>,
        ) -> Result<
            super::super::chain_events::ChangedAgreementsResult,
            super::super::chain_events::ChainEventError,
        > {
            self.poll_times
                .lock()
                .unwrap()
                .push(tokio::time::Instant::now());
            Ok(super::super::chain_events::ChangedAgreementsResult {
                snapshots: vec![],
                latest_block: 1,
                latest_block_timestamp: Some(1000),
                cursor: Cursor::at_block(1),
            })
        }

        async fn get_agreement_snapshots(
            &self,
            _agreement_ids: &[IndexingAgreementId],
        ) -> Result<
            super::super::chain_events::AgreementSnapshots,
            super::super::chain_events::ChainEventError,
        > {
            Ok(super::super::chain_events::AgreementSnapshots::default())
        }
    }

    /// Persist the freshly-fetched chain timestamp even when a parse
    /// failure holds the cursor back. The expiration service reads the
    /// persisted timestamp to decide which agreements are past their
    /// deadline; coupling the timestamp write to a cursor advance lets
    /// expiration go dormant whenever the cursor stalls.
    #[tokio::test]
    async fn test_persists_timestamp_when_cursor_held_back() {
        let registry = MockRegistry::new();
        // Pin the initial cursor at block 100 so the held-back cursor
        // (also 100, no id) is not strictly greater and `advance_cursor`
        // is false.
        registry.set_initial_last_processed_block(100);

        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(200);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        event_source.set_cursor_override(Some(Cursor::at_block(100)));

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: Some("http://localhost:8000/subgraphs/name/test".parse().unwrap()),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            // Disable the reorg buffer for this test so the read-side
            // cursor stays at the persisted value and the held-back
            // assertion is unambiguous.
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
            bypass_chain_clock_defenses: false,
        };

        let ctx = Ctx {
            registry: registry.clone(),
            chain_client: MockChainClient::default(),
            event_source,
            config,
            agreement_conf: test_agreement_conf(),
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
            subgraph_indexing_agreements_events_emitter: Arc::new(
                SubgraphIndexingAgreementsEventsEmitter::disabled(),
            ),
            worker_queue: MockWorkerQueue::default(),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(200)).await;

        handle.stop().await;
        let _ = svc_handle.await;

        let updates = registry.chain_listener_state_updates();
        assert!(
            !updates.is_empty(),
            "expected at least one update_chain_listener_state call despite held-back cursor"
        );
        for (cursor, ts) in &updates {
            assert_eq!(
                cursor.block, 100,
                "cursor must stay at 100 when get_changed_agreements returns a held-back cursor"
            );
            assert_eq!(
                *ts,
                Some(1_700_000_000),
                "timestamp must be persisted on every poll while the subgraph reports it"
            );
        }
    }

    /// Every poll tries the set-aside changes again, though the cursor is past them.
    #[tokio::test]
    async fn test_chain_listener_retries_set_aside_changes_each_poll() {
        let registry = MockRegistry::new();
        registry.set_initial_last_processed_block(200);
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(200);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        event_source.add_snapshots(vec![make_snapshot(
            agreement_id,
            AgreementState::Accepted,
            Address::ZERO,
        )]);
        set_aside_at(&registry, agreement_id, 100).await;

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: Some("http://localhost:8000/subgraphs/name/test".parse().unwrap()),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(20),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            // The drain reads nothing before the cursor at 200, so only the retry can apply it.
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
            bypass_chain_clock_defenses: false,
        };
        let ctx = Ctx {
            registry: registry.clone(),
            chain_client: MockChainClient::default(),
            event_source,
            config,
            agreement_conf: test_agreement_conf(),
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
            subgraph_indexing_agreements_events_emitter: Arc::new(
                SubgraphIndexingAgreementsEventsEmitter::disabled(),
            ),
            worker_queue: MockWorkerQueue::default(),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);
        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.stop().await;
        let _ = svc_handle.await;

        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.set_aside().is_empty());
    }

    /// Drives a multi-page drain through the listener loop and asserts
    /// every snapshot past the per-query cap still gets reconciled.
    #[tokio::test]
    async fn test_chain_listener_paginates_across_subgraph_page_cap() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        // Cap of 2 per poll forces multiple pages over the 5 snapshots.
        event_source.set_page_size(Some(2));

        let agreement_ids: Vec<IndexingAgreementId> = (0..5)
            .map(|_| IndexingAgreementId::from_bytes(rand::random()))
            .collect();
        for (i, id) in agreement_ids.iter().enumerate() {
            registry.add_agreement(*id, IndexingAgreementStatus::Created);
            event_source.add_snapshots(vec![AgreementStateSnapshot {
                agreement_id: *id,
                indexer: "0x1234567890123456789012345678901234567890"
                    .parse()
                    .unwrap(),
                state: super::super::chain_events::AgreementState::Accepted,
                canceled_by: Address::ZERO,
                last_state_change_block: ((i + 1) as u64) * 10,
                accepted_at: 0,
                accepted_tx: String::new(),
                canceled_at: 0,
                canceled_tx: String::new(),
            }]);
        }

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: Some("http://localhost:8000/subgraphs/name/test".parse().unwrap()),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(20),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
            bypass_chain_clock_defenses: false,
        };

        let ctx = Ctx {
            registry: registry.clone(),
            chain_client: MockChainClient::default(),
            event_source,
            config,
            agreement_conf: test_agreement_conf(),
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
            subgraph_indexing_agreements_events_emitter: Arc::new(
                SubgraphIndexingAgreementsEventsEmitter::disabled(),
            ),
            worker_queue: MockWorkerQueue::default(),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        // Allow several poll intervals: at page_size=2 over 5 snapshots
        // the loop needs ~3 polls to consume them all, plus one extra
        // for the final cursor advance to latest_block.
        tokio::time::sleep(Duration::from_millis(500)).await;

        handle.stop().await;
        let _ = svc_handle.await;

        for id in &agreement_ids {
            assert!(
                registry.was_marked_accepted_on_chain(id),
                "agreement {id} past the page cap was never reconciled; \
                 cursor advanced past it"
            );
        }
    }

    /// When N agreements share one `lastStateChangeBlock` and N exceeds
    /// the page cap, the keyset cursor's `id` discriminator must drain
    /// the tie across multiple pages.
    #[tokio::test]
    async fn test_chain_listener_drains_same_block_tied_entries() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        // Cap of 2 forces multiple pages over the 5 tied entries.
        event_source.set_page_size(Some(2));

        // Five agreements all at the same block. With distinct random ids
        // they carry the implicit `(block, id)` order graph-node uses.
        let agreement_ids: Vec<IndexingAgreementId> = (0..5)
            .map(|_| IndexingAgreementId::from_bytes(rand::random()))
            .collect();
        for id in &agreement_ids {
            registry.add_agreement(*id, IndexingAgreementStatus::Created);
            event_source.add_snapshots(vec![AgreementStateSnapshot {
                agreement_id: *id,
                indexer: "0x1234567890123456789012345678901234567890"
                    .parse()
                    .unwrap(),
                state: super::super::chain_events::AgreementState::Accepted,
                canceled_by: Address::ZERO,
                last_state_change_block: 50,
                accepted_at: 0,
                accepted_tx: String::new(),
                canceled_at: 0,
                canceled_tx: String::new(),
            }]);
        }

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: Some("http://localhost:8000/subgraphs/name/test".parse().unwrap()),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(20),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
            bypass_chain_clock_defenses: false,
        };

        let ctx = Ctx {
            registry: registry.clone(),
            chain_client: MockChainClient::default(),
            event_source,
            config,
            agreement_conf: test_agreement_conf(),
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
            subgraph_indexing_agreements_events_emitter: Arc::new(
                SubgraphIndexingAgreementsEventsEmitter::disabled(),
            ),
            worker_queue: MockWorkerQueue::default(),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        // 5 entries at cap=2 needs ~3 polls plus one tail tick.
        tokio::time::sleep(Duration::from_millis(500)).await;

        handle.stop().await;
        let _ = svc_handle.await;

        for id in &agreement_ids {
            assert!(
                registry.was_marked_accepted_on_chain(id),
                "agreement {id} sharing block 50 with the others was never reconciled; \
                 keyset cursor failed to drain the tie"
            );
        }
    }

    /// Drives the drain through a forced `apply_reconciliation_batch`
    /// failure and asserts the row counters do not double-count: every
    /// item lands in `errors` and none in `processed`. Without the fix
    /// in `drain_once`, the per-prep finalize loop would tick
    /// `processed += 1` for the same items already counted as errors.
    #[tokio::test]
    async fn test_drain_counters_no_double_count_on_batch_failure() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        registry.set_fail_batch(true);

        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));

        const ITEMS: usize = 4;
        let agreement_ids: Vec<IndexingAgreementId> = (0..ITEMS)
            .map(|_| IndexingAgreementId::from_bytes(rand::random()))
            .collect();
        for (i, id) in agreement_ids.iter().enumerate() {
            registry.add_agreement(*id, IndexingAgreementStatus::Created);
            event_source.add_snapshots(vec![AgreementStateSnapshot {
                agreement_id: *id,
                indexer: "0x1234567890123456789012345678901234567890"
                    .parse()
                    .unwrap(),
                state: super::super::chain_events::AgreementState::Accepted,
                canceled_by: Address::ZERO,
                last_state_change_block: ((i + 1) as u64) * 10,
                accepted_at: 0,
                accepted_tx: String::new(),
                canceled_at: 0,
                canceled_tx: String::new(),
            }]);
        }

        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp: Option<u64> = None;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let mut live_checks = LiveChecks::new(MockWorkerQueue::default());

        let outcome = drain_once(
            &mut cursor,
            &mut None,
            &mut last_persisted_timestamp,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            false,
            &registry,
            &chain_client,
            &mut live_checks,
            &event_source,
            &mut rx_stop,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("subgraph fetch itself succeeded; only the batch apply failed");

        assert_eq!(
            outcome.errors, ITEMS as u64,
            "every batch-failed item must be counted exactly once as an error"
        );
        assert_eq!(
            outcome.processed, 0,
            "items whose batch failed must not also count as processed (double-count regression)"
        );

        // Side-effect cross-check: nothing was actually applied.
        for id in &agreement_ids {
            assert!(
                !registry.was_marked_accepted_on_chain(id),
                "batch failure must not leave per-row mock side-effects"
            );
        }
    }

    /// A registry holding a `Created` agreement, and an event source whose 1 page holds a change
    /// at block 10 that accepts it.
    fn a_page_accepting_an_agreement() -> (
        MockRegistry,
        super::super::chain_events::mock::MockEventSource,
        IndexingAgreementId,
    ) {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        event_source.add_snapshots(vec![AgreementStateSnapshot {
            agreement_id,
            indexer: Address::ZERO,
            state: super::super::chain_events::AgreementState::Accepted,
            canceled_by: Address::ZERO,
            last_state_change_block: 10,
            accepted_at: 0,
            accepted_tx: String::new(),
            canceled_at: 0,
            canceled_tx: String::new(),
        }]);
        (registry, event_source, agreement_id)
    }

    /// Drain 1 page holding a change that accepts an agreement, its apply failing or not, with
    /// the cursor held before it since `held_since` if given; the cursor it leaves, and whether
    /// the agreement was accepted.
    async fn drain_a_page(
        apply_fails: &[bool],
        held_since: Option<std::time::Instant>,
    ) -> (Cursor, Option<CursorHold>, bool) {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();
        let (cursor, cursor_hold) =
            drain_page(&registry, &event_source, apply_fails, held_since).await;
        let accepted = registry.was_marked_accepted_on_chain(&agreement_id);
        (cursor, cursor_hold, accepted)
    }

    /// Drain the page of `event_source` from the start once per entry of `apply_fails`, its apply
    /// failing or not, with the cursor held before it since `held_since` if given; the cursor and
    /// the hold it leaves.
    async fn drain_page(
        registry: &MockRegistry,
        event_source: &super::super::chain_events::mock::MockEventSource,
        apply_fails: &[bool],
        held_since: Option<std::time::Instant>,
    ) -> (Cursor, Option<CursorHold>) {
        let chain_client = MockChainClient::default();
        let mut cursor = Cursor::genesis();
        let mut cursor_hold = held_since.map(|since| CursorHold {
            cursor: Cursor::genesis(),
            since,
        });
        let mut last_persisted_timestamp: Option<u64> = None;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let mut live_checks = LiveChecks::new(MockWorkerQueue::default());

        for fails in apply_fails {
            registry.set_fail_batch(*fails);
            drain_once(
                &mut cursor,
                &mut cursor_hold,
                &mut last_persisted_timestamp,
                &mut last_subgraph_head,
                &mut stall_count,
                &mut consecutive_failures,
                1337,
                0,
                10,
                false,
                registry,
                &chain_client,
                &mut live_checks,
                event_source,
                &mut rx_stop,
                test_agreement_conf().as_ref(),
            )
            .await
            .expect("subgraph fetch succeeded");
        }
        (cursor, cursor_hold)
    }

    /// A page whose changes fail to apply keeps the cursor before it, so the next poll
    /// re-reads it: no later change to those agreements may come to apply it instead.
    #[tokio::test]
    async fn test_drain_holds_the_cursor_until_a_failed_page_applies() {
        let (cursor, hold, _) = drain_a_page(&[true], None).await;
        assert_eq!(cursor, Cursor::genesis(), "held before the failed page");
        assert!(hold.is_some());

        let (cursor, hold, accepted) = drain_a_page(&[true, false], None).await;
        assert!(accepted);
        assert!(cursor > Cursor::genesis());
        assert!(hold.is_none());
    }

    /// A held cursor is an ERROR once, tagged for an alert, then a WARN with no tag on each poll
    /// it goes on, so a 10-minute hold isn't alerted on every few seconds.
    #[tokio::test]
    async fn test_drain_logs_a_held_cursor_as_an_error_once_then_as_warnings() {
        let first = crate::test_support::logged(async {
            drain_a_page(&[true], None).await;
        })
        .await;
        let again = crate::test_support::logged(async {
            drain_a_page(&[true], Some(std::time::Instant::now())).await;
        })
        .await;

        let started = first
            .lines()
            .find(|line| line.contains("Failed to apply on-chain changes"))
            .expect("the hold is logged");
        assert!(started.contains("ERROR"), "{started}");
        assert!(
            started.contains(r#"event="chain_listener_cursor_held""#),
            "{started}"
        );
        let went_on = again
            .lines()
            .find(|line| line.contains("Still failing to apply on-chain changes"))
            .expect("the hold going on is logged");
        assert!(went_on.contains("WARN"), "{went_on}");
        assert!(!again.contains("chain_listener_cursor_held"), "{again}");
    }

    /// A run of failed fetches alerts once, at the 10th; a fetch that works ends the run, so the
    /// next run alerts once more.
    #[tokio::test]
    async fn test_drain_alerts_once_per_run_of_failed_fetches() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp: Option<u64> = None;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let mut live_checks = LiveChecks::new(MockWorkerQueue::default());
        let fetch_fails = [vec![true; 15], vec![false], vec![true; 10]].concat();

        let logged = crate::test_support::logged(async {
            for fails in fetch_fails {
                if fails {
                    event_source.set_error(
                        super::super::chain_events::ChainEventError::RateLimited(
                            "HTTP error: 429 Too Many Requests".into(),
                        ),
                    );
                }
                let drained = drain_once(
                    &mut cursor,
                    &mut None,
                    &mut last_persisted_timestamp,
                    &mut last_subgraph_head,
                    &mut stall_count,
                    &mut consecutive_failures,
                    1337,
                    0,
                    10,
                    false,
                    &registry,
                    &chain_client,
                    &mut live_checks,
                    &event_source,
                    &mut rx_stop,
                    test_agreement_conf().as_ref(),
                )
                .await;
                assert_eq!(drained.is_err(), fails);
            }
        })
        .await;

        assert_eq!(
            logged
                .matches(r#"event="chain_listener_fetch_failing""#)
                .count(),
            2,
            "{logged}"
        );
    }

    /// A page that keeps failing is passed over once the cursor has been held long enough, so
    /// it can't hold up every later change.
    #[tokio::test]
    async fn test_drain_moves_past_a_page_that_keeps_failing() {
        let long_ago = std::time::Instant::now()
            .checked_sub(MAX_CURSOR_HOLD + Duration::from_secs(1))
            .expect("instant");

        let (cursor, hold, accepted) = drain_a_page(&[true], Some(long_ago)).await;

        assert!(!accepted);
        assert!(cursor > Cursor::genesis());
        assert!(hold.is_none());
    }

    /// A time the cursor has been held since for longer than [`MAX_CURSOR_HOLD`].
    fn held_too_long() -> std::time::Instant {
        std::time::Instant::now()
            .checked_sub(MAX_CURSOR_HOLD + Duration::from_secs(1))
            .expect("instant")
    }

    /// Moving past a page drops its changes from the read, so the agreements whose change failed
    /// are recorded first, at the block of the change, to be tried again.
    #[tokio::test]
    async fn test_drain_sets_aside_the_changes_of_a_page_it_moves_past() {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();

        let (cursor, hold) =
            drain_page(&registry, &event_source, &[true], Some(held_too_long())).await;

        assert!(cursor > Cursor::genesis());
        assert!(hold.is_none());
        assert_eq!(
            registry.set_aside(),
            vec![crate::registry::SetAsideChange {
                agreement_id,
                block: 10
            }]
        );
    }

    /// If recording the changes fails, the cursor stays before the page, so nothing is dropped,
    /// and the next poll tries again to record them rather than holding for another 10 minutes.
    #[tokio::test]
    async fn test_drain_keeps_holding_a_page_whose_changes_fail_to_be_set_aside() {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();
        registry.set_fail_set_aside(true);
        let since = held_too_long();

        let (cursor, hold) = drain_page(&registry, &event_source, &[true], Some(since)).await;

        assert_eq!(cursor, Cursor::genesis(), "held before the page");
        assert!(registry.set_aside().is_empty());
        let hold = hold.expect("still held");
        assert_eq!(hold.since, since, "the hold is still over");

        registry.set_fail_set_aside(false);
        let (cursor, _) = drain_page(&registry, &event_source, &[true], Some(hold.since)).await;

        assert!(cursor > Cursor::genesis());
        assert_eq!(
            registry.set_aside(),
            vec![crate::registry::SetAsideChange {
                agreement_id,
                block: 10
            }]
        );
    }

    /// Drain a page holding a change in `state` to an agreement in `status` 3 times: with its
    /// audit writes and job queuing failing, then working, then from the start again, as the
    /// reorg buffer re-reads it. The agreement, the registry, the queue and each drain's cursor.
    async fn drain_a_page_whose_audit_write_fails(
        status: IndexingAgreementStatus,
        state: AgreementState,
    ) -> (
        IndexingAgreementId,
        MockRegistry,
        MockWorkerQueue,
        Vec<Cursor>,
    ) {
        let chain_client = live_chain();
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(1_700_000_000));
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, status);
        event_source.add_snapshots(vec![make_snapshot(agreement_id, state, Address::ZERO)]);
        let mut cursor = Cursor::genesis();
        let mut cursor_hold = None;
        let mut last_persisted_timestamp: Option<u64> = None;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let queue = MockWorkerQueue::default();
        let mut live_checks = LiveChecks::new(queue.clone());
        let mut cursors = Vec::new();

        for (writes_fail, from_start) in [(true, false), (false, false), (false, true)] {
            {
                let mut mock = registry.state.lock().unwrap();
                mock.fail_accept_audit = writes_fail;
                mock.fail_cancel_audit = writes_fail;
            }
            queue
                .fail
                .store(writes_fail, std::sync::atomic::Ordering::SeqCst);
            if from_start {
                cursor = Cursor::genesis();
            }
            drain_once(
                &mut cursor,
                &mut cursor_hold,
                &mut last_persisted_timestamp,
                &mut last_subgraph_head,
                &mut stall_count,
                &mut consecutive_failures,
                1337,
                0,
                10,
                false,
                &registry,
                &chain_client,
                &mut live_checks,
                &event_source,
                &mut rx_stop,
                test_agreement_conf().as_ref(),
            )
            .await
            .expect("subgraph fetch succeeded");
            cursors.push(cursor.clone());
        }
        assert_eq!(
            chain_client.reads.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the drain reads nothing from the chain"
        );
        (agreement_id, registry, queue, cursors)
    }

    /// The end of an agreement dipper already ended is its last change on-chain, so if recording
    /// it fails, only holding the cursor gets it read again. Reading it again once recorded only
    /// repeats the write, which keeps what is already on record.
    #[tokio::test]
    async fn test_drain_rereads_an_accept_and_cancel_that_failed_to_record() {
        let (id, registry, _, cursors) = drain_a_page_whose_audit_write_fails(
            IndexingAgreementStatus::CanceledByRequester,
            AgreementState::CanceledByPayer,
        )
        .await;

        assert_eq!(
            cursors[0],
            Cursor::genesis(),
            "held before the failed write"
        );
        assert!(
            cursors[1..]
                .iter()
                .all(|cursor| *cursor > Cursor::genesis())
        );
        assert_eq!(registry.audit_writes(), vec![("accept and cancel", id); 2]);
        assert!(!registry.was_reopened(&id));
        assert!(!registry.was_marked_canceled_by_requester(&id));
    }

    /// The same holds for the accept of an agreement being cancelled.
    #[tokio::test]
    async fn test_drain_rereads_the_accept_of_an_agreement_being_cancelled_that_failed_to_record() {
        let (id, registry, _, cursors) = drain_a_page_whose_audit_write_fails(
            IndexingAgreementStatus::Cancelling,
            AgreementState::Accepted,
        )
        .await;

        assert_eq!(
            cursors[0],
            Cursor::genesis(),
            "held before the failed write"
        );
        assert!(
            cursors[1..]
                .iter()
                .all(|cursor| *cursor > Cursor::genesis())
        );
        assert_eq!(registry.audit_writes(), vec![("accept", id); 2]);
    }

    /// A rejected or cancelled agreement accepted on-chain is read again until its chain check
    /// is queued, and only queued once, however often the change is read.
    #[tokio::test]
    async fn test_drain_queues_the_chain_check_of_an_ended_agreement_once() {
        for status in [
            IndexingAgreementStatus::Rejected,
            IndexingAgreementStatus::CanceledByRequester,
        ] {
            let (id, registry, queue, cursors) =
                drain_a_page_whose_audit_write_fails(status, AgreementState::Accepted).await;

            assert_eq!(
                cursors[0],
                Cursor::genesis(),
                "{status}: held before the failed queuing"
            );
            assert!(
                cursors[1..]
                    .iter()
                    .all(|cursor| *cursor > Cursor::genesis()),
                "{status}"
            );
            assert_eq!(queue.queued(), vec![(id, snapshot_accept())], "{status}");
            assert!(!registry.was_reopened(&id), "{status}");
            assert!(registry.audit_writes().is_empty(), "{status}");
        }
    }

    /// Try the set-aside changes again once, as a poll does before it drains.
    async fn retry_set_aside(
        registry: &MockRegistry,
        event_source: &super::super::chain_events::mock::MockEventSource,
        warning: &mut SetAsideWarning,
    ) {
        retry_set_aside_queueing_on(registry, event_source, warning, MockWorkerQueue::default())
            .await;
    }

    /// Try the set-aside changes again once, queueing jobs on `queue`.
    async fn retry_set_aside_queueing_on(
        registry: &MockRegistry,
        event_source: &super::super::chain_events::mock::MockEventSource,
        warning: &mut SetAsideWarning,
        queue: MockWorkerQueue,
    ) {
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let mut live_checks = LiveChecks::new(queue);
        let stopped = retry_set_aside_changes(
            registry,
            event_source,
            &MockChainClient::default(),
            &mut live_checks,
            test_agreement_conf().as_ref(),
            warning,
            &mut rx_stop,
        )
        .await;
        assert!(!stopped);
    }

    /// Record `agreement_id`'s change at `block` as set aside.
    async fn set_aside_at(registry: &MockRegistry, agreement_id: IndexingAgreementId, block: u64) {
        crate::registry::SetAsideChangeRegistry::record_set_aside_changes(
            registry,
            &[crate::registry::SetAsideChange {
                agreement_id,
                block,
            }],
        )
        .await
        .unwrap();
    }

    /// An indexer's cancel that was set aside and applied later by the retry leaves its request a
    /// slot short just the same, so it queues the same reassessment as one applied from a page.
    #[tokio::test]
    async fn test_retry_of_an_indexer_cancel_queues_a_reassessment_of_its_request() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(200);
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        let request_id = IndexingRequestId::new();
        registry.set_agreement_request_id(agreement_id, request_id);
        event_source.add_snapshots(vec![make_snapshot(
            agreement_id,
            AgreementState::CanceledByServiceProvider,
            Address::ZERO,
        )]);
        set_aside_at(&registry, agreement_id, 100).await;
        let queue = MockWorkerQueue::default();

        retry_set_aside_queueing_on(
            &registry,
            &event_source,
            &mut SetAsideWarning::default(),
            queue.clone(),
        )
        .await;

        assert!(registry.was_marked_canceled_by_indexer(&agreement_id));
        assert!(registry.set_aside().is_empty());
        assert_eq!(queue.reassessments(), [(request_id, REQUEST_TARGET)]);
    }

    /// A change set aside is read again on a later poll and applied, then no longer recorded.
    #[tokio::test]
    async fn test_retry_applies_a_set_aside_change_and_forgets_it() {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();
        drain_page(&registry, &event_source, &[true], Some(held_too_long())).await;
        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        registry.set_fail_batch(false);

        retry_set_aside(&registry, &event_source, &mut SetAsideWarning::default()).await;

        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.set_aside().is_empty());
    }

    /// A set-aside change that fails again stays recorded, and applies on a later poll.
    #[tokio::test]
    async fn test_retry_keeps_a_set_aside_change_that_fails_again() {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();
        drain_page(&registry, &event_source, &[true], Some(held_too_long())).await;
        let mut warning = SetAsideWarning::default();

        registry.set_fail_batch(true);
        retry_set_aside(&registry, &event_source, &mut warning).await;

        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert_eq!(
            registry.set_aside(),
            vec![crate::registry::SetAsideChange {
                agreement_id,
                block: 10
            }]
        );

        registry.set_fail_batch(false);
        retry_set_aside(&registry, &event_source, &mut warning).await;

        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.set_aside().is_empty());
    }

    /// Set-aside changes apply 1 at a time, so 1 that fails can't roll back the others.
    #[tokio::test]
    async fn test_retry_applies_set_aside_changes_1_at_a_time() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(100);
        let applies = IndexingAgreementId::from_bytes([1; 16]);
        let fails = IndexingAgreementId::from_bytes([2; 16]);
        for id in [applies, fails] {
            registry.add_agreement(id, IndexingAgreementStatus::Created);
            event_source.add_snapshots(vec![make_snapshot(
                id,
                AgreementState::Accepted,
                Address::ZERO,
            )]);
            set_aside_at(&registry, id, 100).await;
        }
        registry.fail_apply_for(fails);

        retry_set_aside(&registry, &event_source, &mut SetAsideWarning::default()).await;

        assert!(registry.was_marked_accepted_on_chain(&applies));
        assert!(!registry.was_marked_accepted_on_chain(&fails));
        assert_eq!(
            registry.set_aside(),
            vec![crate::registry::SetAsideChange {
                agreement_id: fails,
                block: 100
            }]
        );
    }

    /// A subgraph that hasn't yet indexed the block of a set-aside change, such as one behind the
    /// subgraph the change was read from, shows the agreement as it was before the change, or not
    /// at all. Either way the change stays recorded until the subgraph reaches it.
    #[tokio::test]
    async fn test_retry_keeps_a_set_aside_change_while_the_subgraph_is_behind_it() {
        for before_the_accept in [false, true] {
            let registry = MockRegistry::new();
            let event_source = super::super::chain_events::mock::MockEventSource::new();
            event_source.set_latest_block(99);
            let agreement_id = IndexingAgreementId::from_bytes(rand::random());
            registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
            if before_the_accept {
                let mut snapshot =
                    make_snapshot(agreement_id, AgreementState::NotAccepted, Address::ZERO);
                snapshot.last_state_change_block = 90;
                event_source.add_snapshots(vec![snapshot]);
            }
            set_aside_at(&registry, agreement_id, 100).await;
            let mut warning = SetAsideWarning::default();

            retry_set_aside(&registry, &event_source, &mut warning).await;

            assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
            assert_eq!(
                registry.set_aside().len(),
                1,
                "still recorded, the subgraph showing the state before the accept: {before_the_accept}"
            );

            event_source.set_latest_block(100);
            event_source.add_snapshots(vec![make_snapshot(
                agreement_id,
                AgreementState::Accepted,
                Address::ZERO,
            )]);
            retry_set_aside(&registry, &event_source, &mut warning).await;

            assert!(registry.was_marked_accepted_on_chain(&agreement_id));
            assert!(registry.set_aside().is_empty());
        }
    }

    /// Once the subgraph has indexed past a set-aside change, what it shows is the chain as it
    /// is. A state older than the change, as when a reorg dropped the change, is applied, and the
    /// change is no longer tried, with a warning saying so.
    #[tokio::test]
    async fn test_retry_applies_an_older_state_once_the_subgraph_is_past_the_change() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(200);
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        let mut accepted_earlier =
            make_snapshot(agreement_id, AgreementState::Accepted, Address::ZERO);
        accepted_earlier.last_state_change_block = 90;
        event_source.add_snapshots(vec![accepted_earlier]);
        set_aside_at(&registry, agreement_id, 100).await;

        let logged = crate::test_support::logged(retry_set_aside(
            &registry,
            &event_source,
            &mut SetAsideWarning::default(),
        ))
        .await;

        assert!(registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.set_aside().is_empty());
        let warned = logged
            .lines()
            .find(|line| line.contains(r#"event="chain_listener_set_aside_change_gone""#))
            .expect("the change dropping out is logged");
        assert!(warned.contains("WARN"), "{warned}");
    }

    /// An agreement the subgraph has no state for at all, once it has indexed past the change,
    /// as after a reorg or a change of the payer address, is no longer tried, with a warning.
    #[tokio::test]
    async fn test_retry_forgets_a_set_aside_change_the_subgraph_past_it_has_no_state_for() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(200);
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        set_aside_at(&registry, agreement_id, 100).await;

        let logged = crate::test_support::logged(retry_set_aside(
            &registry,
            &event_source,
            &mut SetAsideWarning::default(),
        ))
        .await;

        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert!(registry.set_aside().is_empty());
        let warned = logged
            .lines()
            .find(|line| line.contains(r#"event="chain_listener_set_aside_change_gone""#))
            .expect("the change dropping out is logged");
        assert!(warned.contains("WARN"), "{warned}");
    }

    /// A state the subgraph returns in a form that can't be read says nothing about the
    /// agreement, so its change stays recorded however far the subgraph has indexed.
    #[tokio::test]
    async fn test_retry_keeps_a_set_aside_change_whose_state_cannot_be_read() {
        let registry = MockRegistry::new();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        event_source.set_latest_block(200);
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        registry.add_agreement(agreement_id, IndexingAgreementStatus::Created);
        event_source.add_snapshots(vec![make_snapshot(
            agreement_id,
            AgreementState::Accepted,
            Address::ZERO,
        )]);
        event_source.set_unreadable(vec![agreement_id]);
        set_aside_at(&registry, agreement_id, 100).await;

        retry_set_aside(&registry, &event_source, &mut SetAsideWarning::default()).await;

        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert_eq!(registry.set_aside().len(), 1, "still recorded");
    }

    /// The retry runs every poll, so changes that keep failing are warned about once, not on
    /// each poll.
    #[tokio::test]
    async fn test_retry_warns_once_while_the_same_set_aside_changes_keep_failing() {
        let (registry, event_source, _) = a_page_accepting_an_agreement();
        drain_page(&registry, &event_source, &[true], Some(held_too_long())).await;
        registry.set_fail_batch(true);
        let mut warning = SetAsideWarning::default();

        let logged = crate::test_support::logged(async {
            for _ in 0..3 {
                retry_set_aside(&registry, &event_source, &mut warning).await;
            }
        })
        .await;

        let warned: Vec<&str> = logged
            .lines()
            .filter(|line| line.contains("WARN"))
            .collect();
        assert_eq!(warned.len(), 1, "{logged}");
        assert!(warned[0].contains("still fail"), "{logged}");
        assert!(warned[0].contains("failed to apply the change"), "{logged}");
    }

    /// A failed read of the record changes nothing in it, and says so rather than passing over
    /// the changes in silence.
    #[tokio::test]
    async fn test_retry_leaves_the_record_as_it_is_when_it_cannot_be_read() {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();
        set_aside_at(&registry, agreement_id, 10).await;
        registry.set_fail_set_aside_read(true);

        let logged = crate::test_support::logged(retry_set_aside(
            &registry,
            &event_source,
            &mut SetAsideWarning::default(),
        ))
        .await;

        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert_eq!(registry.set_aside().len(), 1, "still recorded");
        let warned = logged
            .lines()
            .find(|line| line.contains("Failed to read the on-chain changes set aside"))
            .expect("the failed read is logged");
        assert!(warned.contains("WARN"), "{warned}");
    }

    /// A failed read of the agreements' state from the subgraph says nothing about them, so
    /// every change stays recorded.
    #[tokio::test]
    async fn test_retry_keeps_set_aside_changes_when_the_subgraph_read_fails() {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();
        set_aside_at(&registry, agreement_id, 10).await;
        event_source.set_snapshots_error(super::super::chain_events::ChainEventError::Transient(
            "timed out".into(),
        ));

        retry_set_aside(&registry, &event_source, &mut SetAsideWarning::default()).await;

        assert!(!registry.was_marked_accepted_on_chain(&agreement_id));
        assert_eq!(registry.set_aside().len(), 1, "still recorded");
    }

    /// Nor does a failed read of the agreements from the database, which must not pass for the
    /// agreements not being there.
    #[tokio::test]
    async fn test_retry_keeps_set_aside_changes_when_the_agreements_cannot_be_read() {
        let (registry, event_source, agreement_id) = a_page_accepting_an_agreement();
        set_aside_at(&registry, agreement_id, 10).await;
        registry.set_fail_agreement_reads(true);

        retry_set_aside(&registry, &event_source, &mut SetAsideWarning::default()).await;

        assert_eq!(registry.set_aside().len(), 1, "still recorded");
    }

    #[test]
    fn warns_about_set_aside_changes_when_those_failing_change_or_every_10_minutes() {
        let now = Instant::now();
        let first = IndexingAgreementId::from_bytes([1; 16]);
        let second = IndexingAgreementId::from_bytes([2; 16]);
        let mut warning = SetAsideWarning::default();

        assert!(warning.due(&[first], now));
        assert!(!warning.due(
            &[first],
            now + SET_ASIDE_WARNING_INTERVAL - Duration::from_secs(1)
        ));
        assert!(warning.due(&[first], now + SET_ASIDE_WARNING_INTERVAL));
        let later = now + SET_ASIDE_WARNING_INTERVAL;
        assert!(warning.due(&[first, second], later), "another fails");
        assert!(!warning.due(&[first, second], later));
        assert!(warning.due(&[second], later), "one applied");
        assert!(!warning.due(&[], later), "none failing");
        assert!(
            warning.due(&[second], later),
            "fails again after a run ended"
        );
    }

    #[test]
    fn holds_the_cursor_before_a_failing_page_for_a_while() {
        let now = std::time::Instant::now();
        let page = Cursor::genesis();
        let mut hold = None;

        assert!(
            !hold_expired(&mut hold, &page, now),
            "the first failure holds"
        );
        assert!(!hold_expired(
            &mut hold,
            &page,
            now + MAX_CURSOR_HOLD - Duration::from_secs(1)
        ));
        assert!(hold_expired(&mut hold, &page, now + MAX_CURSOR_HOLD));
        assert!(hold.is_some(), "the hold lasts until the page is passed");
    }

    #[test]
    fn lists_at_most_20_agreement_ids_in_their_0x_form() {
        let ids: Vec<IndexingAgreementId> = (0..25u8)
            .map(|n| IndexingAgreementId::from_bytes([n; 16]))
            .collect();

        assert_eq!(
            listed_ids(&ids[1..3]),
            "0x01010101010101010101010101010101, 0x02020202020202020202020202020202"
        );
        let many = listed_ids(&ids);
        assert_eq!(many.matches("0x").count(), 20, "{many}");
        assert!(
            many.ends_with("0x13131313131313131313131313131313 and 5 more"),
            "{many}"
        );
    }

    /// A subgraph claiming a time far ahead of the wall clock must not move
    /// expiration's clock past `now + tolerance`, or agreements expire early.
    #[tokio::test]
    async fn test_chain_ts_drift_capped_against_wall_clock() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        let now = dipper_core::time::now_secs();
        let attempted = now + 100_000;

        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(attempted));

        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp = Some(now);
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let mut live_checks = LiveChecks::new(MockWorkerQueue::default());

        drain_once(
            &mut cursor,
            &mut None,
            &mut last_persisted_timestamp,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            false,
            &registry,
            &chain_client,
            &mut live_checks,
            &event_source,
            &mut rx_stop,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("poll must succeed");

        let persisted = last_persisted_timestamp.expect("must be set");
        let latest_allowed = dipper_core::time::now_secs() + 10;
        assert!(
            (now..=latest_allowed).contains(&persisted),
            "persisted {persisted} must be held to the wall clock, not the attempted {attempted}"
        );
    }

    /// A jump far past the wall clock is clamped to the wall clock plus tolerance.
    #[test]
    fn test_apply_chain_ts_drift_cap_bypass_off_clamps_jump() {
        let now = 1_700_000_000u64;
        let capped = apply_chain_ts_drift_cap(now + 100_000, now - 300, now, 10, false);
        assert_eq!(
            capped,
            now + 10,
            "must clamp to the wall clock plus tolerance"
        );
    }

    /// With bypass on, a jump far past the wall clock is accepted as-is.
    #[test]
    fn test_apply_chain_ts_drift_cap_bypass_on_skips_cap() {
        let now = 1_700_000_000u64;
        let unclamped = apply_chain_ts_drift_cap(now + 100_000, now, now, 10, true);
        assert_eq!(
            unclamped,
            now + 100_000,
            "bypass must accept the jump as-is"
        );
    }

    /// A time within tolerance of the wall clock is accepted however far behind
    /// the persisted value was, so a clock left behind catches up in 1 poll.
    #[test]
    fn test_apply_chain_ts_drift_cap_within_bound_passes_through() {
        let now = 1_700_000_000u64;
        let result = apply_chain_ts_drift_cap(now + 7, now - 3_600, now, 10, false);
        assert_eq!(result, now + 7, "advances inside the bound are not clamped");
    }

    /// A persisted value already past the cap (the host clock stepped back) is
    /// kept, because expiration's clock must never run backwards.
    #[test]
    fn test_apply_chain_ts_drift_cap_never_moves_the_clock_back() {
        let now = 1_700_000_000u64;
        let result = apply_chain_ts_drift_cap(now + 100, now + 50, now, 10, false);
        assert_eq!(
            result,
            now + 50,
            "the persisted value must not go backwards"
        );
    }

    /// The first timestamp ever saved is held to the wall clock too, so a subgraph
    /// that answers a fresh database with a future time can't expire agreements early.
    #[tokio::test]
    async fn test_chain_ts_first_save_capped_against_wall_clock() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        let now = dipper_core::time::now_secs();

        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(now + 55));

        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp: Option<u64> = None;
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let mut live_checks = LiveChecks::new(MockWorkerQueue::default());

        drain_once(
            &mut cursor,
            &mut None,
            &mut last_persisted_timestamp,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            false,
            &registry,
            &chain_client,
            &mut live_checks,
            &event_source,
            &mut rx_stop,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("poll must succeed");

        let persisted = last_persisted_timestamp.expect("must be set");
        let latest_allowed = dipper_core::time::now_secs() + 10;
        assert!(
            persisted <= latest_allowed,
            "persisted {persisted} is past the wall clock bound {latest_allowed}"
        );
    }

    /// After a restart the persisted chain timestamp is as old as the downtime,
    /// while the subgraph is already at current chain time. The first poll must
    /// bring it up to date, or agreements expire late until it catches up.
    #[tokio::test]
    async fn test_chain_ts_catches_up_in_one_poll_after_a_restart() {
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let event_source = super::super::chain_events::mock::MockEventSource::new();
        let now = dipper_core::time::now_secs();
        let downtime_secs = 3_600u64;

        event_source.set_latest_block(100);
        event_source.set_latest_block_timestamp(Some(now));

        let mut cursor = Cursor::genesis();
        let mut last_persisted_timestamp = Some(now - downtime_secs);
        let mut last_subgraph_head: u64 = 0;
        let mut stall_count: u32 = 0;
        let mut consecutive_failures: u32 = 0;
        let (_tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let mut live_checks = LiveChecks::new(MockWorkerQueue::default());

        drain_once(
            &mut cursor,
            &mut None,
            &mut last_persisted_timestamp,
            &mut last_subgraph_head,
            &mut stall_count,
            &mut consecutive_failures,
            1337,
            0,
            10,
            false,
            &registry,
            &chain_client,
            &mut live_checks,
            &event_source,
            &mut rx_stop,
            test_agreement_conf().as_ref(),
        )
        .await
        .expect("post-restart poll must succeed");

        assert_eq!(last_persisted_timestamp, Some(now));
    }

    /// Verify that notify_one() wakes the chain_listener from its idle
    /// 300s interval and triggers a poll within a few seconds.
    #[tokio::test]
    async fn test_notify_wakes_listener_from_idle_interval() {
        let poll_times: Arc<Mutex<Vec<tokio::time::Instant>>> = Arc::new(Mutex::new(vec![]));
        let notify = Arc::new(tokio::sync::Notify::new());

        let config = crate::config::ChainListenerConfig {
            enabled: true,
            subgraph_endpoint: Some("http://localhost:8000/subgraphs/name/test".parse().unwrap()),
            subgraph_api_key: None,
            chain_id: 1337,
            poll_interval: Duration::from_millis(50),
            request_timeout: Duration::from_secs(5),
            max_retries: 0,
            reorg_buffer_blocks: 0,
            wall_clock_skew_tolerance_secs: 60,
            chain_ts_drift_tolerance_secs: 10,
            bypass_chain_clock_defenses: false,
        };

        let ctx = Ctx {
            registry: MockRegistry::new(),
            chain_client: MockChainClient::default(),
            event_source: TimingEventSource {
                poll_times: poll_times.clone(),
            },
            config,
            agreement_conf: test_agreement_conf(),
            chain_listener_notify: notify.clone(),
            subgraph_indexing_agreements_events_emitter: Arc::new(
                SubgraphIndexingAgreementsEventsEmitter::disabled(),
            ),
            worker_queue: MockWorkerQueue::default(),
        };

        let (handle, service) = new(ctx);
        let svc_handle = tokio::spawn(service);

        tokio::time::sleep(Duration::from_millis(200)).await;

        let polls_before = poll_times.lock().unwrap().len();

        notify.notify_one();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let polls_after = poll_times.lock().unwrap().len();
        assert!(
            polls_after > polls_before,
            "expected at least one poll after notify_one(), got {polls_before} -> {polls_after}"
        );

        handle.stop().await;
        let _ = svc_handle.await;
    }

    #[tokio::test]
    async fn test_orphan_sweep_cancels_when_parent_request_canceled() {
        // An AcceptedOnChain agreement whose parent request was flipped to
        // Canceled is the orphan signature: reassessment fired the chain
        // cancel and failed, then bailed out. The sweep must pick it up.
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.set_agreement_request_id(agreement_id, request_id);
        registry.mark_request_canceled(request_id);

        sweep_orphan_canceled_agreements(&registry, &chain_client, test_agreement_conf().as_ref())
            .await;

        assert!(
            chain_client.was_on_chain_cancel_attempted(&agreement_id),
            "sweep should have canceled the agreement on-chain"
        );
        assert!(
            registry.was_marked_canceled_by_requester(&agreement_id),
            "sweep should have marked the agreement CanceledByRequester"
        );
    }

    #[tokio::test]
    async fn test_orphan_sweep_leaves_a_failed_cancel_to_the_cancel_retry() {
        // Retried by this sweep, a cancel that never works was resent with no limit.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient {
            fail_cancels: true,
            ..MockChainClient::default()
        };
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();
        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.set_agreement_request_id(agreement_id, request_id);
        registry.mark_request_canceled(request_id);

        sweep_orphan_canceled_agreements(&registry, &chain_client, test_agreement_conf().as_ref())
            .await;

        assert!(registry.was_marked_cancelling(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_orphan_sweep_records_cancel_audit() {
        // The orphan sweep no longer emits `terminated` directly; it records the
        // cancel audit and `sweep_pending_terminated_events` announces it.
        let registry = MockRegistry::new();
        let chain_client = live_chain();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.set_agreement_request_id(agreement_id, request_id);
        registry.mark_request_canceled(request_id);

        sweep_orphan_canceled_agreements(&registry, &chain_client, test_agreement_conf().as_ref())
            .await;

        assert!(registry.was_marked_canceled_by_requester(&agreement_id));
        assert!(
            registry.was_cancel_audit_recorded(&agreement_id),
            "cancel audit recorded so the sweep can emit `terminated`"
        );
    }

    #[tokio::test]
    async fn test_orphan_sweep_handles_already_canceled_on_chain() {
        // The chain shows the agreement already ended, so no cancel is sent; it is
        // left cancelling for the cancel retry to confirm who ended it.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.set_agreement_request_id(agreement_id, request_id);
        registry.mark_request_canceled(request_id);

        sweep_orphan_canceled_agreements(&registry, &chain_client, test_agreement_conf().as_ref())
            .await;

        assert!(!chain_client.was_on_chain_cancel_attempted(&agreement_id));
        assert!(registry.was_marked_cancelling(&agreement_id));
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_orphan_sweep_skips_when_parent_request_still_open() {
        // An AcceptedOnChain agreement whose parent request is still Open is
        // not an orphan: it's the steady-state happy case. The sweep must
        // leave it alone.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();
        let agreement_id = IndexingAgreementId::from_bytes(rand::random());
        let request_id = IndexingRequestId::new();

        registry.add_agreement(agreement_id, IndexingAgreementStatus::AcceptedOnChain);
        registry.set_agreement_request_id(agreement_id, request_id);
        // Note: request_id NOT marked canceled.

        sweep_orphan_canceled_agreements(&registry, &chain_client, test_agreement_conf().as_ref())
            .await;

        assert!(
            !chain_client.was_on_chain_cancel_attempted(&agreement_id),
            "sweep should not touch agreements whose parent request is Open"
        );
        assert!(!registry.was_marked_canceled_by_requester(&agreement_id));
    }

    #[tokio::test]
    async fn test_orphan_sweep_noop_when_no_orphans() {
        // Empty DB: the sweep should make no chain calls and no DB writes.
        let registry = MockRegistry::new();
        let chain_client = MockChainClient::default();

        sweep_orphan_canceled_agreements(&registry, &chain_client, test_agreement_conf().as_ref())
            .await;

        // Nothing observable to assert beyond "didn't panic"; chain_client's
        // cancels list stays empty by definition because no agreement IDs
        // were registered.
        assert!(chain_client.cancels.lock().unwrap().is_empty());
    }

    // -- broker cooldown and sweep-health tests --

    /// An emitter whose durable sends always fail, standing in for a hung or
    /// unreachable Kafka broker so a sweep reports `SweepHealth::Failed`.
    struct FailingEmitter;

    #[async_trait::async_trait]
    impl SubgraphIndexingAgreementEventsProducer for FailingEmitter {
        fn produce_subgraph_indexing_agreement_request_received(
            &self,
            _: DeploymentId,
            _: ChainId,
            _: proto::SubgraphIndexingAgreementRequestReceived,
        ) {
        }
        fn produce_subgraph_indexing_agreement_proposed(
            &self,
            _: DeploymentId,
            _: ChainId,
            _: proto::SubgraphIndexingAgreementProposed,
        ) {
        }
        fn produce_subgraph_indexing_agreement_n_indexers_unavailable(
            &self,
            _: DeploymentId,
            _: ChainId,
            _: proto::SubgraphIndexingAgreementNIndexersUnavailable,
        ) {
        }
        async fn emit_subgraph_indexing_agreement_accepted(
            &self,
            _: DeploymentId,
            _: ChainId,
            _: proto::SubgraphIndexingAgreementAccepted,
        ) -> Result<(), dipper_producer::events::EmitError> {
            Err(dipper_producer::events::EmitError::Send(
                "broker down".to_string(),
            ))
        }
        async fn emit_subgraph_indexing_agreement_request_expired(
            &self,
            _: DeploymentId,
            _: ChainId,
            _: proto::SubgraphIndexingAgreementRequestExpired,
        ) -> Result<(), dipper_producer::events::EmitError> {
            Err(dipper_producer::events::EmitError::Send(
                "broker down".to_string(),
            ))
        }
        async fn emit_subgraph_indexing_agreement_terminated(
            &self,
            _: DeploymentId,
            _: ChainId,
            _: proto::SubgraphIndexingAgreementTerminated,
        ) -> Result<(), dipper_producer::events::EmitError> {
            Err(dipper_producer::events::EmitError::Send(
                "broker down".to_string(),
            ))
        }
    }

    fn sample_pending_accepted() -> crate::registry::PendingAcceptedEvent {
        crate::registry::PendingAcceptedEvent {
            agreement_id: IndexingAgreementId::from_bytes(rand::random()),
            indexer_id: "0x1234567890123456789012345678901234567890"
                .parse()
                .unwrap(),
            deployment_id: "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
                .parse()
                .unwrap(),
            protocol_network: ChainId::from(42161u64),
            accepted_at: 100,
            accepted_tx: "0xabc".to_string(),
            ends_at: 200,
            payer: Address::ZERO,
        }
    }

    #[test]
    fn broker_cooldown_healthy_broker_never_pauses() {
        let base = Duration::from_secs(30);
        let mut cd = BrokerCooldown::new(base, Duration::from_secs(300));
        let now = Instant::now();
        assert!(!cd.is_paused(now));
        assert_eq!(
            cd.record(SweepHealth::Idle, now),
            CooldownTransition::Unchanged
        );
        assert_eq!(
            cd.record(SweepHealth::Sent, now),
            CooldownTransition::Unchanged
        );
        assert!(!cd.is_paused(now));
    }

    #[test]
    fn broker_cooldown_enters_and_pauses_for_one_interval() {
        let base = Duration::from_secs(30);
        let mut cd = BrokerCooldown::new(base, Duration::from_secs(300));
        let t0 = Instant::now();
        assert_eq!(
            cd.record(SweepHealth::Failed, t0),
            CooldownTransition::Entered
        );
        assert_eq!(cd.backoff(), base);
        assert!(cd.is_paused(t0));
        assert!(cd.is_paused(t0 + base - Duration::from_millis(1)));
        // At exactly one interval the pause is over and the sweeps retry.
        assert!(!cd.is_paused(t0 + base));
    }

    #[test]
    fn broker_cooldown_doubles_backoff_up_to_ceiling() {
        let base = Duration::from_secs(30);
        let ceiling = Duration::from_secs(300);
        let mut cd = BrokerCooldown::new(base, ceiling);
        let mut t = Instant::now();
        assert_eq!(
            cd.record(SweepHealth::Failed, t),
            CooldownTransition::Entered
        );
        for secs in [60u64, 120, 240, 300, 300] {
            t += Duration::from_secs(1);
            assert_eq!(
                cd.record(SweepHealth::Failed, t),
                CooldownTransition::Extended
            );
            assert_eq!(cd.backoff(), Duration::from_secs(secs));
        }
    }

    #[test]
    fn broker_cooldown_recovers_and_resets_backoff() {
        let base = Duration::from_secs(30);
        let mut cd = BrokerCooldown::new(base, Duration::from_secs(300));
        let t0 = Instant::now();
        cd.record(SweepHealth::Failed, t0);
        cd.record(SweepHealth::Failed, t0 + Duration::from_secs(1));
        assert_eq!(cd.backoff(), Duration::from_secs(60));
        let t1 = t0 + Duration::from_secs(120);
        assert_eq!(
            cd.record(SweepHealth::Sent, t1),
            CooldownTransition::Recovered
        );
        assert!(!cd.is_paused(t1));
        assert_eq!(cd.backoff(), base, "recovery resets the backoff");
        assert_eq!(
            cd.record(SweepHealth::Sent, t1),
            CooldownTransition::Unchanged,
            "a later healthy sweep is not another recovery"
        );
    }

    #[test]
    fn broker_cooldown_idle_round_leaves_pause_intact() {
        let base = Duration::from_secs(30);
        let mut cd = BrokerCooldown::new(base, Duration::from_secs(300));
        let t0 = Instant::now();
        cd.record(SweepHealth::Failed, t0);
        assert_eq!(
            cd.record(SweepHealth::Idle, t0 + base),
            CooldownTransition::Unchanged
        );
        assert_eq!(cd.backoff(), base);
        assert_eq!(
            cd.record(SweepHealth::Failed, t0 + base),
            CooldownTransition::Extended,
            "still down, so a fresh failure extends rather than re-enters"
        );
    }

    #[test]
    fn sweep_health_merge_prefers_failure_then_sent() {
        use SweepHealth::{Failed, Idle, Sent};
        assert_eq!(Failed.merge(Sent), Failed);
        assert_eq!(Sent.merge(Failed), Failed);
        assert_eq!(Idle.merge(Failed), Failed);
        assert_eq!(Sent.merge(Idle), Sent);
        assert_eq!(Idle.merge(Sent), Sent);
        assert_eq!(Idle.merge(Idle), Idle);
    }

    #[tokio::test]
    async fn accepted_sweep_reports_sent_when_broker_accepts() {
        let registry = MockRegistry::new();
        registry.seed_pending_accepted(sample_pending_accepted());
        let events = crate::test_support::CapturingEventsProducer::new();

        let health = sweep_pending_accepted_events(&registry, &events).await;

        assert_eq!(health, SweepHealth::Sent);
        assert_eq!(
            events.events().len(),
            1,
            "the pending accepted row is announced"
        );
    }

    #[tokio::test]
    async fn accepted_sweep_reports_failed_when_broker_rejects() {
        let registry = MockRegistry::new();
        registry.seed_pending_accepted(sample_pending_accepted());

        let health = sweep_pending_accepted_events(&registry, &FailingEmitter).await;

        assert_eq!(
            health,
            SweepHealth::Failed,
            "a failed send must trip the cooldown"
        );
    }

    #[tokio::test]
    async fn event_sweeps_report_idle_when_nothing_pending() {
        let registry = MockRegistry::new();
        let events = crate::test_support::CapturingEventsProducer::new();

        let health = run_event_sweeps(&registry, test_agreement_conf().as_ref(), &events).await;

        assert_eq!(health, SweepHealth::Idle);
        assert!(
            events.events().is_empty(),
            "nothing pending means nothing announced"
        );
    }

    /// A failed check for offered or accepted agreements counts as having some, with a warning:
    /// polling slowly while offers are out would record their acceptances, and the cancellations
    /// those trigger, up to 5 minutes late.
    #[tokio::test]
    async fn a_failed_check_for_active_agreements_keeps_polling_fast_and_warns() {
        let mut fast = None;
        let logs = crate::test_support::logged(async {
            fast = Some(wants_fast_polling(Err(
                crate::registry::Error::NoRecordsUpdated,
            )));
        })
        .await;

        assert_eq!(fast, Some(true));
        assert!(
            logs.contains("WARN") && logs.contains("polling at the fast interval"),
            "no warning logged: {logs}"
        );
        assert!(wants_fast_polling(Ok(true)));
        assert!(!wants_fast_polling(Ok(false)));
    }

    /// How many times the listener reads the subgraph in 30 seconds, polling every second while it
    /// has agreements to watch, when the check for them fails or not.
    async fn polls_in_30_seconds(active_check_fails: bool) -> usize {
        let poll_times: Arc<Mutex<Vec<tokio::time::Instant>>> = Arc::new(Mutex::new(vec![]));
        let registry = MockRegistry::new();
        registry.state.lock().unwrap().fail_active_check = active_check_fails;
        // Read from settings, as the service does, so the test does not depend on how each field
        // is typed.
        let config: crate::config::ChainListenerConfig =
            serde_json::from_value(serde_json::json!({
                "subgraph_endpoint": "http://localhost:8000/subgraphs/name/test",
                "poll_interval": 1,
                "max_retries": 0,
                "reorg_buffer_blocks": 0,
            }))
            .unwrap();
        let ctx = Ctx {
            registry,
            chain_client: MockChainClient::default(),
            event_source: TimingEventSource {
                poll_times: poll_times.clone(),
            },
            config,
            agreement_conf: test_agreement_conf(),
            chain_listener_notify: Arc::new(tokio::sync::Notify::new()),
            subgraph_indexing_agreements_events_emitter: Arc::new(
                SubgraphIndexingAgreementsEventsEmitter::disabled(),
            ),
            worker_queue: MockWorkerQueue::default(),
        };

        let (handle, service) = new(ctx);
        let service = tokio::spawn(service);
        tokio::time::sleep(Duration::from_secs(30)).await;
        handle.stop().await;
        let _ = service.await;
        poll_times.lock().unwrap().len()
    }

    /// While the check keeps failing the listener stays on its fast interval, instead of falling
    /// back to the 300 second one as it does when it finds nothing to watch.
    #[tokio::test(start_paused = true)]
    async fn the_listener_polls_fast_while_it_cannot_check_for_active_agreements() {
        let idle = polls_in_30_seconds(false).await;
        let failing = polls_in_30_seconds(true).await;

        assert!(idle <= 2, "nothing to watch: {idle} polls");
        assert!(failing >= 20, "check failing: {failing} polls");
    }
}
