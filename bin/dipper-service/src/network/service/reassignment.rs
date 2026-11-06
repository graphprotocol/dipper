//! Periodic reassignment of open indexing requests. A daily full pass queues a reassessment of
//! every open request; a more frequent top-up pass queues only requests left short of indexers
//! with an indexer free to take them by dipper's own records, instead of waiting a day.

use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    future::Future,
    sync::Arc,
    time::Duration,
};

use dipper_core::ids::IndexingRequestId;
use dipper_iisa::{CandidateSelection, DipsAcceptingSnapshot, PriceCeilings};
use graph_networks_registry::NetworksRegistry;
use thegraph_core::{IndexerId, alloy::primitives::ChainId};
use time::OffsetDateTime;
use tokio::{sync::mpsc, time::Instant};

use crate::{
    config::{IndexingAgreementConfig, ReassignmentConfig},
    registry::{
        AgreementRegistry, IndexerDenylistRegistry, IndexingRequest, IndexingRequestRegistry,
    },
    worker::{
        DipsAcceptingCache, is_active_agreement, offerable_indexers, resolve_chain_name,
        service::{JobPriority, WorkerQueue},
    },
};

/// The longest 1 database read or 1 queue push may take, so neither can stall the service.
const DB_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// Calculate the duration from `now` until the next occurrence of the target UTC hour.
fn duration_until_utc_hour(now: OffsetDateTime, target_hour: u8) -> Duration {
    let current_hour = now.hour();
    let current_minute = now.minute();
    let current_second = now.second();

    let hours_until = if current_hour < target_hour {
        target_hour - current_hour
    } else if current_hour == target_hour && current_minute == 0 && current_second == 0 {
        0 // Exactly at target hour
    } else {
        // Past target hour today, wait until tomorrow
        24 - current_hour + target_hour
    };

    let seconds_into_current_hour = (current_minute as u64) * 60 + (current_second as u64);
    let total_seconds = (hours_until as u64) * 3600 - seconds_into_current_hour;

    Duration::from_secs(total_seconds)
}

/// The duration from `now` until the next whole multiple of `every` since the Unix epoch, so the
/// top-up keeps to the clock across restarts and an hourly one falls due with the daily pass.
fn until_next_multiple(now: OffsetDateTime, every: Duration) -> Duration {
    let every = every.as_secs().max(1);
    let since_epoch = u64::try_from(now.unix_timestamp()).unwrap_or(0);
    Duration::from_secs((every - since_epoch % every) % every)
}

/// The first run after `now` of a schedule that last ran at `last` and repeats every `every`.
/// Runs missed while a pass was slow are skipped, not made up.
fn next_after(last: Instant, every: Duration, now: Instant) -> Instant {
    let every = every.max(Duration::from_secs(1));
    let mut next = last + every;
    while next <= now {
        next += every;
    }
    next
}

/// Handle for controlling the reassignment service lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the reassignment service gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the reassignment service
pub struct Ctx<R, W, I> {
    /// Registry for querying indexing requests and what rules indexers out of a new offer
    pub registry: R,
    /// Worker queue for submitting reassessment jobs
    pub worker_queue: W,
    /// Service configuration
    pub config: ReassignmentConfig,
    /// IISA, asked through `dips_accepting_cache` which indexers accept DIPs on a chain
    pub iisa: I,
    pub dips_accepting_cache: DipsAcceptingCache,
    /// Lookbacks and price ceilings, the ones reassessment uses
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    pub networks_registry: Arc<NetworksRegistry>,
    pub additional_networks: Arc<BTreeMap<ChainId, String>>,
}

/// How a pass ended: with the requests it queued, or because the service was told to stop.
enum Pass {
    Done(HashSet<IndexingRequestId>),
    Stopped,
}

/// Create a new reassignment service
///
/// Returns a handle for controlling the service and a future that must be spawned on a runtime.
pub fn new<R, W, I>(ctx: Ctx<R, W, I>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: IndexingRequestRegistry + AgreementRegistry + IndexerDenylistRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
    I: CandidateSelection + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let service = async move {
        let config = &ctx.config;
        if config.interval.is_zero() {
            anyhow::bail!(
                "reassignment.interval must be at least 1 second; set enabled to false to turn it off"
            );
        }
        let (started, clock) = (Instant::now(), OffsetDateTime::now_utc());
        let initial_delay = duration_until_utc_hour(clock, config.run_at_utc_hour);
        let mut next_full = started + initial_delay;
        let top_up_every = (!config.top_up_interval.is_zero()).then_some(config.top_up_interval);
        let mut next_top_up = top_up_every.map(|every| started + until_next_multiple(clock, every));

        tracing::info!(
            run_at_utc_hour = config.run_at_utc_hour,
            initial_delay_secs = initial_delay.as_secs(),
            interval_secs = config.interval.as_secs(),
            top_up_interval_secs = config.top_up_interval.as_secs(),
            batch_size = config.batch_size,
            min_age_secs = config.min_request_age.as_secs(),
            "reassignment service started, waiting for first cycle"
        );

        loop {
            let wake = next_top_up.map_or(next_full, |at| at.min(next_full));
            tokio::select! {
                _ = rx_stop.recv() => break,
                () = tokio::time::sleep_until(wake) => {}
            }
            let now = Instant::now();
            let full_due = now >= next_full;
            let top_up_due = next_top_up.is_some_and(|at| now >= at);

            if let Pass::Stopped = run_due_passes(&ctx, full_due, top_up_due, &mut rx_stop).await {
                return Ok(());
            }
            if full_due {
                next_full = next_after(next_full, config.interval, now);
            }
            if let (true, Some(at), Some(every)) = (top_up_due, next_top_up, top_up_every) {
                next_top_up = Some(next_after(at, every, now));
            }
        }

        tracing::debug!("reassignment service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// Run the passes that are due, full first. Both run in this 1 task, so they never overlap.
/// Neither queues a request that already has a reassessment queued or running, and the top-up
/// skips what the full pass has just queued.
async fn run_due_passes<R, W, I>(
    ctx: &Ctx<R, W, I>,
    full_due: bool,
    top_up_due: bool,
    rx_stop: &mut mpsc::Receiver<()>,
) -> Pass
where
    R: IndexingRequestRegistry + AgreementRegistry + IndexerDenylistRegistry + Sync,
    W: WorkerQueue + Sync,
    I: CandidateSelection + Sync,
{
    if !full_due && !top_up_due {
        return Pass::Done(HashSet::new());
    }
    let mut skip = already_queued(&ctx.worker_queue).await;
    if full_due {
        match full_pass(ctx, &skip, rx_stop).await {
            Pass::Done(ids) => skip.extend(ids),
            Pass::Stopped => return Pass::Stopped,
        }
    }
    if top_up_due && let Pass::Stopped = top_up_pass(ctx, &skip, rx_stop).await {
        return Pass::Stopped;
    }
    Pass::Done(skip)
}

/// The requests a reassessment is already queued or running for. When the queue can't be read
/// the passes go ahead without that check; a duplicate job only repeats a reassessment.
async fn already_queued<W: WorkerQueue + Sync>(worker_queue: &W) -> HashSet<IndexingRequestId> {
    match db(worker_queue.queued_reassessments()).await {
        Ok(ids) => ids,
        Err(err) => {
            tracing::warn!(error = %err, "can't list queued reassessments; queueing without that check");
            HashSet::new()
        }
    }
}

/// Queue a reassessment of every open request older than the minimum age, other than those in
/// `skip`.
async fn full_pass<R, W, I>(
    ctx: &Ctx<R, W, I>,
    skip: &HashSet<IndexingRequestId>,
    rx_stop: &mut mpsc::Receiver<()>,
) -> Pass
where
    R: IndexingRequestRegistry + Sync,
    W: WorkerQueue + Sync,
{
    let min_age_seconds = i64::try_from(ctx.config.min_request_age.as_secs()).unwrap_or(i64::MAX);
    let listed = db(ctx
        .registry
        .get_open_indexing_requests_for_reassessment(min_age_seconds, ctx.config.batch_size))
    .await;
    match listed {
        Ok(mut requests) => {
            requests.retain(|request| !skip.contains(&request.id));
            queue_reassessments(&ctx.worker_queue, &requests, rx_stop, "full").await
        }
        Err(err) => {
            tracing::error!(error = %err, "failed to query indexing requests for reassessment");
            Pass::Done(HashSet::new())
        }
    }
}

/// Queue a reassessment of each short request a free indexer could fill now, other than those in
/// `skip`. A failed read ends the pass with a warning; the next pass tries again.
async fn top_up_pass<R, W, I>(
    ctx: &Ctx<R, W, I>,
    skip: &HashSet<IndexingRequestId>,
    rx_stop: &mut mpsc::Receiver<()>,
) -> Pass
where
    R: IndexingRequestRegistry + AgreementRegistry + IndexerDenylistRegistry + Sync,
    W: WorkerQueue + Sync,
    I: CandidateSelection + Sync,
{
    match short_requests_with_a_free_indexer(ctx, skip).await {
        Ok(requests) => queue_reassessments(&ctx.worker_queue, &requests, rx_stop, "top_up").await,
        Err(err) => {
            tracing::warn!(error = %err, "top-up pass failed; the next one tries again");
            Pass::Done(HashSet::new())
        }
    }
}

/// The open requests left short of indexers, other than those in `skip`, below their target
/// counting held slots and with an indexer in their chain's DIPs pool that dipper's records don't
/// rule out. IISA may still pass that indexer over; the request is then checked again next pass.
async fn short_requests_with_a_free_indexer<R, W, I>(
    ctx: &Ctx<R, W, I>,
    skip: &HashSet<IndexingRequestId>,
) -> anyhow::Result<Vec<IndexingRequest>>
where
    R: IndexingRequestRegistry + AgreementRegistry + IndexerDenylistRegistry + Sync,
    I: CandidateSelection + Sync,
{
    // Every short request is looked at, and the batch caps how many are queued, so ones nobody
    // can fill don't keep out ones a free indexer could.
    let short: Vec<IndexingRequest> = db(ctx.registry.get_short_open_indexing_requests(0))
        .await?
        .into_iter()
        .filter(|request| !skip.contains(&request.id))
        .collect();
    if short.is_empty() {
        return Ok(short);
    }

    let conf = &ctx.agreement_conf;
    let declined = db(ctx.registry.get_declined_indexers_by_deployment(
        conf.declined_indexer_lookback_days(),
        conf.price_rejection_lookback_days(),
        conf.transient_rejection_lookback_minutes(),
        conf.uncertain_rejection_lookback_days(),
    ))
    .await?;
    let denylist = db(ctx.registry.get_indexer_denylist()).await?;

    let mut chains: HashMap<ChainId, Option<ChainPool>> = HashMap::new();
    let mut fillable = Vec::new();
    for request in short {
        let chain = match chains.entry(request.deployment_chain_id) {
            Entry::Occupied(known) => known.into_mut(),
            Entry::Vacant(unread) => {
                unread.insert(chain_pool(ctx, request.deployment_chain_id).await?)
            }
        };
        let Some(chain) = chain else {
            continue;
        };

        let agreements = db(ctx
            .registry
            .get_indexing_agreements_by_deployment_id(&request.deployment_id))
        .await?;
        let held = db(ctx
            .registry
            .get_abandoned_indexers_holding_slots(&request.id))
        .await?;
        let holding = agreements
            .iter()
            .filter(|a| a.indexing_request_id == request.id && is_active_agreement(&a.status))
            .count();
        if request.num_candidates <= holding + held.len() {
            continue;
        }

        let free = offerable_indexers(
            &chain.dips.indexers,
            request.deployment_id,
            &agreements,
            &declined,
            &denylist,
            &chain.unresponsive,
            &held,
        );
        if free.is_empty() {
            continue;
        }
        tracing::debug!(
            request_id = %request.id,
            deployment_id = %request.deployment_id,
            free = free.len(),
            "short request has a free indexer"
        );
        fillable.push(request);
        if usize::try_from(ctx.config.batch_size).is_ok_and(|batch| fillable.len() == batch) {
            break;
        }
    }
    Ok(fillable)
}

/// What the top-up pass reads once per chain: IISA's DIPs pool there and its recently
/// unresponsive indexers. The pass leaves unresponsive indexers out even while reassessment's
/// mass-unresponsive breaker would let them in, so it errs towards waiting for the daily pass.
struct ChainPool {
    dips: Arc<DipsAcceptingSnapshot>,
    unresponsive: Vec<IndexerId>,
}

/// Read `chain_id`'s pool, or `None` (with a warning) when the chain has no network name or IISA
/// can't say who accepts DIPs there now, so its requests wait for the next pass.
async fn chain_pool<R, W, I>(
    ctx: &Ctx<R, W, I>,
    chain_id: ChainId,
) -> anyhow::Result<Option<ChainPool>>
where
    R: AgreementRegistry + Sync,
    I: CandidateSelection + Sync,
{
    let Some(chain) =
        resolve_chain_name(chain_id, &ctx.networks_registry, &ctx.additional_networks)
    else {
        tracing::warn!(
            chain_id,
            "top-up pass: no network name for chain id; add it to additional_networks"
        );
        return Ok(None);
    };
    let ceilings = PriceCeilings {
        max_grt_per_30_days: Some(ctx.agreement_conf.max_grt_per_30_days_for(&chain)),
        max_grt_per_billion_entities_per_30_days: Some(
            ctx.agreement_conf
                .max_grt_per_billion_entities_per_30_days(),
        ),
    };
    let Some(dips) = ctx
        .dips_accepting_cache
        .get_or_fetch(&ctx.iisa, &chain, ceilings)
        .await
    else {
        tracing::warn!(
            chain,
            "top-up pass: IISA's DIPs pool can't be read; skipping this chain's short requests"
        );
        return Ok(None);
    };
    let unresponsive = db(ctx.registry.get_unresponsive_indexers(
        ctx.agreement_conf.unresponsive_indexer_lookback_days(),
        chain_id,
    ))
    .await?;
    Ok(Some(ChainPool { dips, unresponsive }))
}

/// Queue a reassessment of each of `requests`, stopping early if the service is told to stop.
async fn queue_reassessments<W: WorkerQueue + Sync>(
    worker_queue: &W,
    requests: &[IndexingRequest],
    rx_stop: &mut mpsc::Receiver<()>,
    pass: &'static str,
) -> Pass {
    let mut queued = HashSet::new();
    for request in requests {
        // Check for shutdown between queue pushes to stay responsive
        if rx_stop.try_recv().is_ok() {
            tracing::debug!(pass, "reassignment service stopping mid-cycle");
            return Pass::Stopped;
        }
        if queue_reassessment(worker_queue, request, pass).await {
            queued.insert(request.id);
        }
    }
    log_cycle(pass, requests.len(), queued.len());
    Pass::Done(queued)
}

/// Queue a reassessment of `request`, returning whether it was queued.
async fn queue_reassessment<W: WorkerQueue + Sync>(
    worker_queue: &W,
    request: &IndexingRequest,
    pass: &'static str,
) -> bool {
    let pushed = tokio::time::timeout(
        QUEUE_PUSH_TIMEOUT,
        worker_queue.reassess_indexing_request(
            request.id,
            request.deployment_id,
            request.deployment_chain_id,
            request.num_candidates,
            // Background: the periodic sweep yields to interactive work.
            JobPriority::Background,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out"))
    .and_then(|pushed| pushed);
    match pushed {
        Ok(_job_id) => {
            tracing::debug!(
                pass,
                request_id = %request.id,
                deployment_id = %request.deployment_id,
                "queued request for reassessment"
            );
            true
        }
        Err(err) => {
            tracing::warn!(
                pass,
                request_id = %request.id,
                error = %err,
                "failed to queue request for reassessment"
            );
            false
        }
    }
}

/// Log what a pass did: at info when it had requests to queue, at debug when it had none.
fn log_cycle(pass: &'static str, listed: usize, queued: usize) {
    if listed == 0 {
        tracing::debug!(pass, "reassessment cycle: no eligible requests");
    } else {
        tracing::info!(
            pass,
            listed,
            queued,
            failed = listed - queued,
            "reassessment cycle completed"
        );
    }
}

/// Await a database read, failing it if it takes longer than `DB_QUERY_TIMEOUT`.
async fn db<T, E>(read: impl Future<Output = Result<T, E>>) -> anyhow::Result<T>
where
    E: Into<anyhow::Error>,
{
    tokio::time::timeout(DB_QUERY_TIMEOUT, read)
        .await
        .map_err(|_| anyhow::anyhow!("database read timed out"))?
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use dipper_core::ids::IndexingAgreementId;
    use dipper_iisa::{SelectedIndexer, SelectionContext, SelectionError};
    use thegraph_core::{DeploymentId, alloy::primitives::Address};
    use url::Url;

    use super::*;
    use crate::{
        cancel_dispatch::tests::agreement,
        registry::{
            IndexingAgreement, IndexingAgreementStatus, IndexingRequestStatus, SetTargetOutcome,
            StubAgreementRegistry,
        },
        worker::queue::JobId,
    };

    const CHAIN: ChainId = 421_614;

    fn indexer(hex_digit: char) -> IndexerId {
        format!("0x{}", hex_digit.to_string().repeat(40))
            .parse()
            .unwrap()
    }

    fn deployment() -> DeploymentId {
        "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap()
    }

    fn request(num_candidates: usize) -> IndexingRequest {
        IndexingRequest {
            id: IndexingRequestId::new(),
            created_at: OffsetDateTime::now_utc(),
            updated_at: OffsetDateTime::now_utc(),
            status: IndexingRequestStatus::Open,
            requested_by: Address::ZERO,
            deployment_id: deployment(),
            deployment_chain_id: CHAIN,
            num_candidates,
        }
    }

    fn holding(request: &IndexingRequest, indexer_id: IndexerId) -> IndexingAgreement {
        let mut a = agreement(IndexingAgreementStatus::AcceptedOnChain, None);
        a.indexer.id = indexer_id;
        a.indexing_request_id = request.id;
        a
    }

    #[derive(Default)]
    struct MockRegistry {
        open: Vec<IndexingRequest>,
        short: Vec<IndexingRequest>,
        agreements: Vec<IndexingAgreement>,
        declined: HashMap<DeploymentId, Vec<IndexerId>>,
        denylist: Vec<IndexerId>,
        unresponsive: Vec<IndexerId>,
        held: Vec<IndexerId>,
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            _requested_by: Address,
            _deployment_id: DeploymentId,
            _deployment_chain_id: u64,
            _num_candidates: usize,
        ) -> crate::registry::Result<SetTargetOutcome> {
            unimplemented!()
        }
        async fn get_all_indexing_requests(&self) -> crate::registry::Result<Vec<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_indexing_request_by_id(
            &self,
            _id: &IndexingRequestId,
        ) -> crate::registry::Result<Option<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> crate::registry::Result<Vec<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age_seconds: i64,
            batch_size: i64,
        ) -> crate::registry::Result<Vec<IndexingRequest>> {
            let limit = usize::try_from(batch_size).ok().filter(|n| *n > 0);
            Ok(self
                .open
                .iter()
                .take(limit.unwrap_or(usize::MAX))
                .cloned()
                .collect())
        }
        async fn get_short_requests_on_deployments_with_ended_agreements(
            &self,
            _since_seconds: i64,
            _batch_size: i64,
        ) -> crate::registry::Result<Vec<IndexingRequest>> {
            unimplemented!()
        }
        async fn get_short_open_indexing_requests(
            &self,
            batch_size: i64,
        ) -> crate::registry::Result<Vec<IndexingRequest>> {
            let limit = usize::try_from(batch_size).ok().filter(|n| *n > 0);
            Ok(self
                .short
                .iter()
                .take(limit.unwrap_or(usize::MAX))
                .cloned()
                .collect())
        }
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_indexing_agreements_by_deployment_id(
            &self,
            _deployment_id: &DeploymentId,
        ) -> crate::registry::Result<Vec<IndexingAgreement>> {
            Ok(self.agreements.clone())
        }
        async fn get_declined_indexers_by_deployment(
            &self,
            _default_lookback_days: i32,
            _price_lookback_days: i32,
            _transient_lookback_minutes: i32,
            _uncertain_lookback_days: i32,
        ) -> crate::registry::Result<HashMap<DeploymentId, Vec<IndexerId>>> {
            Ok(self.declined.clone())
        }
        async fn get_unresponsive_indexers(
            &self,
            _lookback_days: i32,
            _chain_id: ChainId,
        ) -> crate::registry::Result<Vec<IndexerId>> {
            Ok(self.unresponsive.clone())
        }
        async fn get_abandoned_indexers_holding_slots(
            &self,
            _request_id: &IndexingRequestId,
        ) -> crate::registry::Result<Vec<IndexerId>> {
            Ok(self.held.clone())
        }
    }

    #[async_trait]
    impl IndexerDenylistRegistry for MockRegistry {
        async fn get_indexer_denylist(&self) -> crate::registry::Result<Vec<IndexerId>> {
            Ok(self.denylist.clone())
        }
    }

    /// IISA's DIPs pool for every chain, or `None` to fail as when IISA is down.
    struct MockIisa(Option<Vec<IndexerId>>);

    #[async_trait]
    impl CandidateSelection for MockIisa {
        async fn select_indexers(
            &self,
            _deployment_id: DeploymentId,
            _num_candidates: usize,
            _context: &SelectionContext,
        ) -> Result<Vec<SelectedIndexer>, SelectionError> {
            unimplemented!("the reassignment service never selects indexers itself")
        }

        async fn dips_accepting_indexers(
            &self,
            _chain: &str,
            _ceilings: PriceCeilings,
        ) -> Result<DipsAcceptingSnapshot, SelectionError> {
            self.0
                .clone()
                .map(|indexers| DipsAcceptingSnapshot {
                    computed_at: None,
                    indexers,
                })
                .ok_or(SelectionError::IisaServiceUnavailable)
        }
    }

    /// Records the requests it is asked to reassess, and reports the 2nd field's requests as
    /// already having a reassessment queued.
    #[derive(Default, Clone)]
    struct MockQueue(
        Arc<Mutex<Vec<IndexingRequestId>>>,
        HashSet<IndexingRequestId>,
    );

    #[async_trait]
    impl WorkerQueue for MockQueue {
        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn reassess_indexing_request(
            &self,
            indexing_request_id: IndexingRequestId,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _num_candidates: usize,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            self.0.lock().unwrap().push(indexing_request_id);
            Ok(JobId::default())
        }
        async fn queued_reassessments(&self) -> anyhow::Result<HashSet<IndexingRequestId>> {
            Ok(self.1.clone())
        }
        async fn cancel_rejected_agreement_on_chain(
            &self,
            _agreement_id: IndexingAgreementId,
            _accept: Option<crate::worker::service::ChainAccept>,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: Url,
            _deployment_id: DeploymentId,
            _deployment_chain_id: ChainId,
            _priority: JobPriority,
        ) -> anyhow::Result<JobId> {
            unimplemented!()
        }
    }

    fn ctx(
        registry: MockRegistry,
        pool: Option<Vec<IndexerId>>,
        config: ReassignmentConfig,
    ) -> (Ctx<MockRegistry, MockQueue, MockIisa>, MockQueue) {
        let queue = MockQueue::default();
        let ctx = Ctx {
            registry,
            worker_queue: queue.clone(),
            config,
            iisa: MockIisa(pool),
            dips_accepting_cache: DipsAcceptingCache::new(Duration::from_secs(300)),
            agreement_conf: Arc::new(IndexingAgreementConfig::for_tests()),
            networks_registry: Arc::new(empty_networks_registry()),
            additional_networks: Arc::new(BTreeMap::from([(CHAIN, "arbitrum-sepolia".into())])),
        };
        (ctx, queue)
    }

    fn empty_networks_registry() -> NetworksRegistry {
        NetworksRegistry::from_json(
            r#"{
                "$schema": "https://example/schema.json",
                "description": "test",
                "networks": [],
                "title": "test",
                "updatedAt": "2024-01-01T00:00:00Z",
                "version": "0.0.0"
            }"#,
        )
        .expect("empty registry")
    }

    /// Run 1 top-up pass and return the requests it queued.
    async fn top_up(
        registry: MockRegistry,
        pool: Option<Vec<IndexerId>>,
    ) -> Vec<IndexingRequestId> {
        let (ctx, queue) = ctx(registry, pool, ReassignmentConfig::default());
        let (_tx, mut rx) = mpsc::channel(1);
        run_due_passes(&ctx, false, true, &mut rx).await;
        queue.0.lock().unwrap().clone()
    }

    #[test]
    fn test_default_config() {
        let config = ReassignmentConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(86400)); // 24 hours
        assert_eq!(config.run_at_utc_hour, 10); // 10:00 UTC
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.min_request_age, Duration::from_secs(86400));
        assert_eq!(config.top_up_interval, Duration::from_secs(3600));
    }

    #[test]
    fn test_duration_until_utc_hour() {
        // This is a basic sanity check - the actual delay depends on current time
        let delay = duration_until_utc_hour(OffsetDateTime::now_utc(), 2);
        // Should always be less than 24 hours
        assert!(delay.as_secs() < 86400);
    }

    /// An hourly top-up keeps to the hour, so after a restart it waits at most an hour and it
    /// falls due with the daily pass, which then queues first.
    #[test]
    fn an_hourly_top_up_falls_due_on_the_hour_with_the_daily_pass() {
        let hour = Duration::from_secs(3600);
        let at = |h, m, s| {
            time::Date::from_calendar_date(2026, time::Month::November, 6)
                .unwrap()
                .with_hms(h, m, s)
                .unwrap()
                .assume_utc()
        };
        let before_ten = at(9, 59, 30);
        assert_eq!(
            until_next_multiple(before_ten, hour),
            Duration::from_secs(30)
        );
        assert_eq!(
            duration_until_utc_hour(before_ten, 10),
            Duration::from_secs(30)
        );
        let on_the_hour = at(10, 0, 0);
        assert_eq!(until_next_multiple(on_the_hour, hour), Duration::ZERO);
        let after = at(10, 20, 0);
        assert_eq!(
            until_next_multiple(after, hour),
            Duration::from_secs(40 * 60)
        );
    }

    #[test]
    fn a_schedule_skips_the_runs_a_slow_pass_missed() {
        let start = Instant::now();
        let hour = Duration::from_secs(3600);
        assert_eq!(next_after(start, hour, start), start + hour);
        let late = start + hour * 3 + Duration::from_secs(5);
        assert_eq!(next_after(start, hour, late), start + hour * 4);
    }

    #[tokio::test]
    async fn tops_up_a_short_request_a_free_indexer_could_fill() {
        let short = request(3);
        let registry = MockRegistry {
            agreements: vec![holding(&short, indexer('a'))],
            short: vec![short.clone()],
            ..MockRegistry::default()
        };

        let queued = top_up(registry, Some(vec![indexer('a'), indexer('b')])).await;

        assert_eq!(queued, vec![short.id]);
    }

    #[tokio::test]
    async fn leaves_a_request_alone_when_every_indexer_already_holds_it() {
        let short = request(3);
        let registry = MockRegistry {
            agreements: vec![holding(&short, indexer('a')), holding(&short, indexer('b'))],
            short: vec![short],
            ..MockRegistry::default()
        };

        let queued = top_up(registry, Some(vec![indexer('a'), indexer('b')])).await;

        assert!(queued.is_empty(), "{queued:?}");
    }

    /// Another request's agreement on the same subgraph keeps its indexer from a new offer but
    /// takes none of this request's slots.
    #[tokio::test]
    async fn another_requests_agreement_on_the_subgraph_takes_none_of_its_slots() {
        let (short, other) = (request(1), request(1));
        let registry = MockRegistry {
            agreements: vec![holding(&other, indexer('a'))],
            short: vec![short.clone()],
            ..MockRegistry::default()
        };

        let queued = top_up(registry, Some(vec![indexer('a'), indexer('b')])).await;

        assert_eq!(queued, vec![short.id]);
    }

    #[tokio::test]
    async fn leaves_a_request_alone_when_its_only_free_indexers_are_ruled_out() {
        let short = request(3);
        let registry = MockRegistry {
            agreements: vec![holding(&short, indexer('a'))],
            declined: HashMap::from([(deployment(), vec![indexer('b')])]),
            denylist: vec![indexer('c')],
            unresponsive: vec![indexer('d')],
            short: vec![short],
            ..MockRegistry::default()
        };

        let pool = "abcd".chars().map(indexer).collect();
        let queued = top_up(registry, Some(pool)).await;

        assert!(queued.is_empty(), "{queued:?}");
    }

    #[tokio::test]
    async fn leaves_a_request_alone_when_held_slots_fill_its_target() {
        let short = request(2);
        let registry = MockRegistry {
            agreements: vec![holding(&short, indexer('a'))],
            held: vec![indexer('b')],
            short: vec![short],
            ..MockRegistry::default()
        };

        let pool = "abc".chars().map(indexer).collect();
        let queued = top_up(registry, Some(pool)).await;

        assert!(queued.is_empty(), "{queued:?}");
    }

    /// The batch limits how many requests are queued, not how many are looked at, so requests a
    /// free indexer can't fill don't crowd out those it can.
    #[tokio::test]
    async fn the_batch_counts_only_requests_a_free_indexer_could_fill() {
        let (full, fillable) = (request(2), request(3));
        let registry = MockRegistry {
            agreements: vec![holding(&full, indexer('a')), holding(&full, indexer('b'))],
            short: vec![full, fillable.clone()],
            ..MockRegistry::default()
        };
        let config = ReassignmentConfig {
            batch_size: 1,
            ..ReassignmentConfig::default()
        };
        let pool = "abc".chars().map(indexer).collect();
        let (ctx, queue) = ctx(registry, Some(pool), config);
        let (_tx, mut rx) = mpsc::channel(1);

        run_due_passes(&ctx, false, true, &mut rx).await;

        assert_eq!(*queue.0.lock().unwrap(), vec![fillable.id]);
    }

    #[tokio::test]
    async fn skips_a_chain_whose_dips_pool_cannot_be_read() {
        let registry = MockRegistry {
            short: vec![request(3)],
            ..MockRegistry::default()
        };

        let queued = top_up(registry, None).await;

        assert!(queued.is_empty(), "{queued:?}");
    }

    #[tokio::test]
    async fn skips_a_chain_with_no_network_name() {
        let mut short = request(3);
        short.deployment_chain_id = 999_999;
        let registry = MockRegistry {
            short: vec![short],
            ..MockRegistry::default()
        };

        let queued = top_up(registry, Some(vec![indexer('a')])).await;

        assert!(queued.is_empty(), "{queued:?}");
    }

    #[tokio::test]
    async fn a_request_the_full_pass_queued_is_not_queued_again_by_the_top_up() {
        let short = request(3);
        let registry = MockRegistry {
            open: vec![short.clone()],
            short: vec![short.clone()],
            ..MockRegistry::default()
        };
        let (ctx, queue) = ctx(
            registry,
            Some(vec![indexer('a')]),
            ReassignmentConfig::default(),
        );
        let (_tx, mut rx) = mpsc::channel(1);

        run_due_passes(&ctx, true, true, &mut rx).await;

        assert_eq!(*queue.0.lock().unwrap(), vec![short.id]);
    }

    #[tokio::test]
    async fn neither_pass_queues_a_request_already_waiting_for_a_reassessment() {
        let (waiting, other) = (request(3), request(3));
        let registry = MockRegistry {
            open: vec![waiting.clone()],
            short: vec![waiting.clone(), other.clone()],
            ..MockRegistry::default()
        };
        let (mut ctx, queue) = ctx(
            registry,
            Some(vec![indexer('a')]),
            ReassignmentConfig::default(),
        );
        ctx.worker_queue.1 = HashSet::from([waiting.id]);
        let (_tx, mut rx) = mpsc::channel(1);

        run_due_passes(&ctx, true, true, &mut rx).await;

        assert_eq!(*queue.0.lock().unwrap(), vec![other.id]);
    }

    /// Requests already waiting for a reassessment don't use up the full pass's batch.
    #[tokio::test]
    async fn requests_already_waiting_dont_use_up_the_full_pass_batch() {
        let (waiting, other) = (request(3), request(3));
        let registry = MockRegistry {
            open: vec![waiting.clone(), other.clone()],
            ..MockRegistry::default()
        };
        let config = ReassignmentConfig {
            batch_size: 1,
            ..ReassignmentConfig::default()
        };
        let (mut ctx, queue) = ctx(registry, Some(Vec::new()), config);
        ctx.worker_queue.1 = HashSet::from([waiting.id]);
        let (_tx, mut rx) = mpsc::channel(1);

        run_due_passes(&ctx, true, false, &mut rx).await;

        assert_eq!(*queue.0.lock().unwrap(), vec![other.id]);
    }

    /// A daily interval of 0 is refused at start-up rather than running the full pass every second.
    #[tokio::test(start_paused = true)]
    async fn refuses_a_full_pass_interval_of_0() {
        let config = ReassignmentConfig {
            interval: Duration::ZERO,
            ..ReassignmentConfig::default()
        };
        let (ctx, _queue) = ctx(MockRegistry::default(), Some(Vec::new()), config);
        let (_handle, service) = new(ctx);

        let ended = tokio::time::timeout(Duration::from_secs(48 * 3600), service).await;

        let err = ended.expect("the service should stop at once").unwrap_err();
        assert!(err.to_string().contains("interval"), "{err}");
    }

    /// The top-up runs every interval on the clock without waiting for the daily hour, and not at all
    /// when its interval is 0. In 150s a 60s top-up runs 2 or 3 times, depending on where the
    /// clock's minute falls. The full pass finds nothing to queue, wherever the clock puts its hour.
    #[tokio::test(start_paused = true)]
    async fn tops_up_every_interval_and_never_when_turned_off() {
        for (every, expected) in [(Duration::from_secs(60), 2..=3), (Duration::ZERO, 0..=0)] {
            let short = request(3);
            let registry = MockRegistry {
                short: vec![short],
                ..MockRegistry::default()
            };
            let config = ReassignmentConfig {
                top_up_interval: every,
                ..ReassignmentConfig::default()
            };
            let (ctx, queue) = ctx(registry, Some(vec![indexer('a')]), config);
            let (handle, service) = new(ctx);
            let task = tokio::spawn(service);

            tokio::time::sleep(Duration::from_secs(150)).await;
            handle.stop().await;
            task.await.unwrap().unwrap();

            let runs = queue.0.lock().unwrap().len();
            assert!(expected.contains(&runs), "every {every:?}: {runs} runs");
        }
    }
}
