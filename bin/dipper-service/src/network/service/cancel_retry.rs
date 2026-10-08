//! Finishes the cancels dipper starts, marked `Cancelling` before they go out: re-sends each
//! while the chain shows it live, then marks it ended, `AbandonedByIndexer` if its indexer
//! stopped serving it. Runs on its own, as nothing else finishes them.

use std::{future::Future, sync::Arc, time::Duration};

use dipper_core::time::now_secs;
use thegraph_core::alloy::primitives::B256;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    cancel_dispatch::{
        CancelReason, LiveCancel, cancel_if_live, confirm_cancelled, log_unconfirmed,
    },
    chain_client::{ChainClient, ChainClientError},
    config::IndexingAgreementConfig,
    registry::{
        AgreementRegistry, CancelKind, CancellingAgreement, IndexingAgreement,
        IndexingRequestRegistry,
    },
    worker::service::WorkerQueue,
};

/// Failed cancels before dipper alerts an operator and retries the agreement only hourly, so a
/// paused manager recovers once unpaused. Outages don't count (see `failed_attempts`).
pub const MAX_CANCEL_ATTEMPTS: u32 = 10;

/// Agreements a sweep takes on, those that may be paying an indexer first; the time budget
/// below decides how many it gets through.
const BATCH_SIZE: i64 = 50;

/// How often agreements still being cancelled get their cancel retried.
const SWEEP_INTERVAL: Duration = Duration::from_secs(300);

/// Time a sweep may take before leaving the rest to the next one, as each cancel can wait up
/// to 15 s to be mined.
const SWEEP_BUDGET: Duration = Duration::from_secs(30);

/// Time allowed to read an ended agreement's request, and to queue its replacement.
const DB_TIMEOUT: Duration = Duration::from_secs(30);
const QUEUE_TIMEOUT: Duration = Duration::from_secs(10);

/// Minutes an agreement stays out of the retry after it is marked, so the cancel sent
/// when it was marked can be mined first instead of being sent again. One moved back to
/// cancelling had none sent, so it doesn't wait.
const SETTLE_MINUTES: i32 = 2;

/// How long the chain listener gets, from when a check first finds an agreement ended, to
/// record when and in which transaction it ended, before the retry marks it without them.
const LISTENER_GRACE: time::Duration = time::Duration::HOUR;

/// Handle for stopping the cancel retry.
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the cancel retry, cutting short a sweep in progress.
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }
        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// What the cancel retry needs.
pub struct Ctx<R, T, W> {
    pub registry: R,
    pub chain_client: T,
    pub agreement_conf: Arc<IndexingAgreementConfig>,
    /// Queues the replacement of an agreement ended because its indexer stopped serving it.
    pub worker_queue: W,
}

/// Create the cancel retry. Returns a handle plus a future to spawn, which sweeps at once and
/// then every [`SWEEP_INTERVAL`].
pub fn new<R, T, W>(ctx: Ctx<R, T, W>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + IndexingRequestRegistry + Send + Sync,
    T: ChainClient + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);
    let Ctx {
        registry,
        chain_client,
        agreement_conf,
        worker_queue,
    } = ctx;
    let service = async move {
        tracing::info!(
            interval_secs = SWEEP_INTERVAL.as_secs(),
            "cancel retry service started"
        );
        let mut timer = tokio::time::interval(SWEEP_INTERVAL);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }
            let abandoned = tokio::select! {
                _ = rx_stop.recv() => break,
                ended = retry_cancelling_agreements(&registry, &chain_client, &agreement_conf) => ended,
            };
            for agreement in &abandoned {
                super::liveness_checker::queue_replacement(
                    agreement,
                    &registry,
                    &worker_queue,
                    DB_TIMEOUT,
                    QUEUE_TIMEOUT,
                )
                .await;
            }
        }
        tracing::debug!("cancel retry service stopped");
        Ok(())
    };
    (Handle { tx_stop }, service)
}

/// Retry the cancel of agreements still `Cancelling`, returning those it ended that were
/// abandoned by their indexer, to be replaced. The chain's own latest block time decides when
/// an offer that was never accepted no longer can be, so a lagging subgraph doesn't hold it up.
pub async fn retry_cancelling_agreements<R, T>(
    registry: &R,
    chain_client: &T,
    config: &IndexingAgreementConfig,
) -> Vec<IndexingAgreement>
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let cancelling = match registry
        .get_cancelling_agreements(BATCH_SIZE, MAX_CANCEL_ATTEMPTS, SETTLE_MINUTES)
        .await
    {
        Ok(cancelling) => cancelling,
        Err(err) => {
            tracing::warn!(error = %err, "Failed to list agreements still being cancelled");
            return Vec::new();
        }
    };
    if cancelling.is_empty() {
        return Vec::new();
    }
    let Some(chain_now) = chain_time(chain_client).await else {
        return Vec::new();
    };
    let started = std::time::Instant::now();
    let mut abandoned = Vec::new();
    for (done, row) in cancelling.iter().enumerate() {
        if started.elapsed() >= SWEEP_BUDGET {
            tracing::info!(
                left = cancelling.len() - done,
                "Cancel retry ran out of time; the rest wait for the next sweep"
            );
            break;
        }
        if retry_cancel(registry, chain_client, config, row, chain_now).await && row.abandoned {
            abandoned.push(row.agreement.clone());
        }
    }
    abandoned
}

async fn chain_time<T: ChainClient>(chain_client: &T) -> Option<u64> {
    match chain_client.latest_block_timestamp().await {
        Ok(chain_now) => Some(chain_now),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to read the chain's time; cancels are retried next sweep"
            );
            None
        }
    }
}

/// Retry 1 agreement's cancel; true once it is marked ended.
async fn retry_cancel<R, T>(
    registry: &R,
    chain_client: &T,
    config: &IndexingAgreementConfig,
    row: &CancellingAgreement,
    chain_now: u64,
) -> bool
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let agreement_id = row.agreement.id;
    let (tx_hash, by_indexer, failure) =
        match cancel_if_live(chain_client, &row.agreement, config).await {
            LiveCancel::ReadFailed(err) => {
                tracing::warn!(
                    %agreement_id,
                    error = %err,
                    "Failed to read a cancelling agreement on-chain, will retry"
                );
                // Unread, it may still be live, so it can't be confirmed ended.
                note_check(registry, row, None, None).await;
                return false;
            }
            LiveCancel::NotLive { by_indexer } => (None, by_indexer, None),
            LiveCancel::Ended(tx_hash) => {
                tracing::info!(
                    %agreement_id,
                    tx_hash = ?tx_hash,
                    "Cancelled an agreement still live on-chain"
                );
                (tx_hash, false, None)
            }
            LiveCancel::CancelFailed(err) => (None, false, Some(err)),
            LiveCancel::Unconfirmed { tx_hash, err } => {
                log_unconfirmed(&row.agreement, tx_hash, &err);
                note_check(registry, row, None, None).await;
                return false;
            }
        };
    if failure.is_none()
        && confirm_if_over(registry, config, row, tx_hash, by_indexer, chain_now).await
    {
        return true;
    }
    // A cancel that failed found it live; otherwise it is over, or withdrawn until its deadline.
    note_check(registry, row, failure.as_ref(), Some(failure.is_none())).await;
    false
}

/// Mark the agreement ended by dipper once it can't go live again: this sweep's cancel
/// ended it, or nobody accepted its offer before the deadline to. One ended otherwise is left
/// to the chain listener for a while; one the indexer ended then becomes `CanceledByIndexer`.
async fn confirm_if_over<R: AgreementRegistry + Sync>(
    registry: &R,
    config: &IndexingAgreementConfig,
    row: &CancellingAgreement,
    tx_hash: Option<B256>,
    by_indexer: bool,
    chain_now: u64,
) -> bool {
    let agreement = &row.agreement;
    let past_grace = row
        .ended_seen_at
        .is_some_and(|seen| seen < time::OffsetDateTime::now_utc() - LISTENER_GRACE);
    let can_confirm = if row.accepted_on_chain {
        tx_hash.is_some() || past_grace
    } else {
        chain_now > agreement.terms.deadline
    };
    if !can_confirm {
        return false;
    }
    if by_indexer {
        tracing::info!(
            agreement_id = %agreement.id,
            "The indexer ended an agreement dipper was cancelling"
        );
        return past_grace && record_end_by_indexer(registry, agreement).await;
    }
    let reason = if row.abandoned {
        CancelReason::Abandoned
    } else {
        CancelReason::NotWanted
    };
    confirm_cancelled(registry, agreement, reason, tx_hash, config).await
}

/// Mark an agreement the indexer ended `CanceledByIndexer` when the chain listener hasn't in
/// time, so it doesn't stay cancelling for good. The indexer is recorded as ending it first,
/// so its announcement names them; the time recorded is when dipper noticed.
async fn record_end_by_indexer<R: AgreementRegistry + Sync>(
    registry: &R,
    agreement: &IndexingAgreement,
) -> bool {
    let indexer = agreement.indexer.id.to_string();
    let marked = match registry
        .record_cancel_audit(&agreement.id, now_secs(), &indexer, None)
        .await
    {
        Ok(()) => {
            registry
                .apply_reconciliation(&agreement.id, false, Some(CancelKind::ByIndexer))
                .await
        }
        Err(err) => Err(err),
    };
    match marked {
        Ok(outcome) => {
            tracing::info!(
                agreement_id = %agreement.id,
                indexing_request_id = %agreement.indexing_request_id,
                old_status = "CANCELLING",
                new_status = "CANCELED_BY_INDEXER",
                applied = outcome.did_cancel,
                reason = "indexer_cancel_seen_on_chain",
                "agreement state transition"
            );
            true
        }
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "Failed to mark an agreement the indexer ended, will retry"
            );
            false
        }
    }
}

/// Record that the agreement was checked and is still cancelling, and whether it was found
/// ended, counting a cancel the chain answered without ending it; at the limit, an ERROR.
async fn note_check<R: AgreementRegistry + Sync>(
    registry: &R,
    row: &CancellingAgreement,
    failure: Option<&ChainClientError>,
    ended: Option<bool>,
) {
    let agreement = &row.agreement;
    let failed_attempts = failure.map_or(0, failed_attempts);
    if let Some(err) = failure
        && failed_attempts == 0
    {
        log_uncounted_failure(agreement, err);
    }
    match registry
        .record_cancel_check(&agreement.id, failed_attempts, ended)
        .await
    {
        Ok(attempts) => {
            if let Some(err) = failure.filter(|_| failed_attempts > 0) {
                log_failed_cancel(row, attempts, failed_attempts, err);
            }
        }
        Err(err) => tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "Failed to record a check of a cancelling agreement"
        ),
    }
}

fn log_uncounted_failure(agreement: &IndexingAgreement, err: &ChainClientError) {
    tracing::warn!(
        agreement_id = %agreement.id,
        error = %err,
        "Cancel of an agreement failed or could not be confirmed, will retry"
    );
}

/// One ERROR as an agreement reaches the limit, for an operator to look into; a WARN for
/// every other failed cancel.
fn log_failed_cancel(
    row: &CancellingAgreement,
    attempts: u32,
    failed: u32,
    err: &ChainClientError,
) {
    let agreement = &row.agreement;
    let reached_limit =
        attempts >= MAX_CANCEL_ATTEMPTS && attempts.saturating_sub(failed) < MAX_CANCEL_ATTEMPTS;
    if !reached_limit {
        tracing::warn!(
            agreement_id = %agreement.id,
            attempts,
            error = %err,
            "Cancel failed, never mined, or did not end the agreement; will retry"
        );
        return;
    }
    tracing::error!(
        event = "agreement_cancel_stuck",
        agreement_id = %agreement.id,
        indexer_id = %agreement.indexer.id,
        indexing_request_id = %agreement.indexing_request_id,
        abandoned = row.abandoned,
        attempts,
        error = %err,
        "Cancelling an agreement keeps failing; it may still be live. Dipper now retries it hourly"
    );
}

/// How many of an agreement's cancel attempts a failed cancel uses up. The chain was read just
/// before, so any failure counts, a refusal to send (gas over the cap, signer out of funds)
/// included, except a cancel whose receipt checks all failed, which may have mined unseen.
fn failed_attempts(err: &ChainClientError) -> u32 {
    match err {
        ChainClientError::TxDropped {
            receipt_checked: false,
            ..
        } => 0,
        ChainClientError::MissingTermsVersionHash { .. } => MAX_CANCEL_ATTEMPTS,
        _ => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    };

    use async_trait::async_trait;
    use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
    use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
    use thegraph_core::alloy::primitives::Address;

    use super::*;
    use crate::{
        cancel_dispatch::tests::agreement,
        chain_client::AgreementOnChain,
        registry::{IndexingAgreementStatus, StubAgreementRegistry},
        worker::service::JobPriority,
    };

    const DEADLINE: u64 = 1_000;

    #[derive(Default)]
    struct MockRegistry {
        cancelling: Vec<CancellingAgreement>,
        marked_cancelled: Mutex<Vec<IndexingAgreementId>>,
        marked_by_indexer: Mutex<Vec<IndexingAgreementId>>,
        audits: Mutex<Vec<Option<String>>>,
        audited_by: Mutex<Vec<String>>,
        attempts: AtomicU32,
        checks: AtomicU32,
        found_ended: Mutex<Vec<Option<bool>>>,
        /// The chain listener marks it ended before the retry's own mark lands.
        listener_ended_it: bool,
        writes: Mutex<Vec<&'static str>>,
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_cancelling_agreements(
            &self,
            _batch_size: i64,
            _max_attempts: u32,
            _min_age_minutes: i32,
        ) -> crate::registry::Result<Vec<CancellingAgreement>> {
            Ok(self.cancelling.clone())
        }
        async fn mark_indexing_agreement_as_canceled_by_requester(
            &self,
            id: &IndexingAgreementId,
        ) -> crate::registry::Result<()> {
            if self.listener_ended_it {
                return Err(crate::registry::Error::NoRecordsUpdated);
            }
            self.marked_cancelled.lock().unwrap().push(*id);
            self.writes.lock().unwrap().push("ended");
            Ok(())
        }
        async fn record_cancel_audit(
            &self,
            _id: &IndexingAgreementId,
            _canceled_at: u64,
            canceled_by: &str,
            canceled_tx: Option<&str>,
        ) -> crate::registry::Result<()> {
            self.audited_by.lock().unwrap().push(canceled_by.to_owned());
            self.writes.lock().unwrap().push("cancel recorded");
            self.audits
                .lock()
                .unwrap()
                .push(canceled_tx.map(str::to_owned));
            Ok(())
        }
        async fn apply_reconciliation(
            &self,
            id: &IndexingAgreementId,
            _apply_accept: bool,
            cancel: Option<CancelKind>,
        ) -> crate::registry::Result<crate::registry::ReconciliationOutcome> {
            assert_eq!(cancel, Some(CancelKind::ByIndexer));
            self.marked_by_indexer.lock().unwrap().push(*id);
            Ok(crate::registry::ReconciliationOutcome {
                did_accept: false,
                did_cancel: true,
            })
        }
        async fn record_cancel_check(
            &self,
            _id: &IndexingAgreementId,
            failed_attempts: u32,
            ended: Option<bool>,
        ) -> crate::registry::Result<u32> {
            self.checks.fetch_add(1, Ordering::SeqCst);
            self.found_ended.lock().unwrap().push(ended);
            Ok(self.attempts.fetch_add(failed_attempts, Ordering::SeqCst) + failed_attempts)
        }
    }

    /// An agreement live on-chain until a cancel ends it, unless set to ignore cancels.
    #[derive(Default)]
    struct MockChain {
        live: AtomicBool,
        read_fails: bool,
        /// What every send fails with, when set.
        send_error: Option<fn() -> ChainClientError>,
        mined_cancel_reverts: bool,
        never_mines: bool,
        receipt_unreadable: bool,
        reverts_before_sending: bool,
        cancel_has_no_effect: bool,
        clock_fails: bool,
        clock_reads: AtomicU32,
        now: AtomicU64,
        ended_by_indexer: bool,
        indexer_ends_it_first: bool,
        read_back_fails: bool,
        cancels_sent: AtomicU32,
        reads: AtomicU32,
    }

    #[async_trait]
    impl ChainClient for MockChain {
        async fn offer_via_manager(
            &self,
            _rca: &RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn cancel_via_manager(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            if let Some(send_error) = self.send_error {
                return Err(send_error());
            }
            if self.reverts_before_sending {
                return Err(ChainClientError::ContractRevert {
                    selector: [0xde, 0xad, 0xbe, 0xef],
                    data: Default::default(),
                });
            }
            if self.mined_cancel_reverts {
                return Err(ChainClientError::TxReverted {
                    tx_hash: B256::repeat_byte(0xee),
                });
            }
            if self.never_mines || self.receipt_unreadable {
                return Err(ChainClientError::TxDropped {
                    tx_hash: B256::repeat_byte(0xdd),
                    receipt_checked: self.never_mines,
                });
            }
            self.cancels_sent.fetch_add(1, Ordering::SeqCst);
            if !self.cancel_has_no_effect || self.indexer_ends_it_first {
                self.live.store(false, Ordering::SeqCst);
            }
            Ok(Some(B256::repeat_byte(0xcd)))
        }
        async fn reconcile_provider(
            &self,
            _collector: Address,
            _provider: Address,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn reconcile_agreement(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn agreement_on_chain(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<AgreementOnChain, ChainClientError> {
            let earlier_reads = self.reads.fetch_add(1, Ordering::SeqCst);
            if self.read_fails || (self.read_back_fails && earlier_reads > 0) {
                return Err(ChainClientError::RpcError(anyhow::anyhow!("rpc down")));
            }
            Ok(if self.live.load(Ordering::SeqCst) {
                AgreementOnChain::Live
            } else if self.ended_by_indexer || self.indexer_ends_it_first {
                AgreementOnChain::EndedByIndexer
            } else {
                AgreementOnChain::NotLive
            })
        }
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            self.clock_reads.fetch_add(1, Ordering::SeqCst);
            if self.clock_fails {
                return Err(ChainClientError::RpcError(anyhow::anyhow!("rpc down")));
            }
            Ok(self.now.load(Ordering::SeqCst))
        }
    }

    fn registry_with_one(accepted_on_chain: bool) -> MockRegistry {
        let mut cancelling = agreement(IndexingAgreementStatus::Cancelling, Some(vec![7u8; 32]));
        cancelling.terms.deadline = DEADLINE;
        MockRegistry {
            cancelling: vec![CancellingAgreement {
                agreement: cancelling,
                accepted_on_chain,
                ended_seen_at: None,
                abandoned: false,
            }],
            ..MockRegistry::default()
        }
    }

    fn live_chain() -> MockChain {
        MockChain {
            live: AtomicBool::new(true),
            ..MockChain::default()
        }
    }

    async fn retry(registry: &MockRegistry, chain: &MockChain, chain_now: u64) {
        let config = IndexingAgreementConfig::for_tests();
        chain.now.store(chain_now, Ordering::SeqCst);
        retry_cancelling_agreements(registry, chain, &config).await;
    }

    #[async_trait]
    impl IndexingRequestRegistry for MockRegistry {
        async fn set_indexing_target_candidates(
            &self,
            _requested_by: Address,
            _deployment_id: thegraph_core::DeploymentId,
            _deployment_chain_id: u64,
            _num_candidates: usize,
        ) -> crate::registry::Result<crate::registry::SetTargetOutcome> {
            unimplemented!()
        }
        async fn get_all_indexing_requests(
            &self,
        ) -> crate::registry::Result<Vec<crate::registry::IndexingRequest>> {
            unimplemented!()
        }
        async fn get_indexing_request_by_id(
            &self,
            id: &IndexingRequestId,
        ) -> crate::registry::Result<Option<crate::registry::IndexingRequest>> {
            let agreement = &self.cancelling[0].agreement;
            Ok(Some(crate::registry::IndexingRequest {
                id: *id,
                created_at: time::OffsetDateTime::now_utc(),
                updated_at: time::OffsetDateTime::now_utc(),
                status: crate::registry::IndexingRequestStatus::Open,
                requested_by: Address::ZERO,
                deployment_id: agreement.terms.metadata.subgraph_deployment_id,
                deployment_chain_id: agreement.terms.metadata.chain_id,
                num_candidates: 3,
            }))
        }
        async fn get_indexing_requests_by_deployment_id(
            &self,
            _deployment_id: &thegraph_core::DeploymentId,
        ) -> crate::registry::Result<Vec<crate::registry::IndexingRequest>> {
            unimplemented!()
        }
        async fn get_open_indexing_requests_for_reassessment(
            &self,
            _min_age_seconds: i64,
            _batch_size: i64,
        ) -> crate::registry::Result<Vec<crate::registry::IndexingRequest>> {
            unimplemented!()
        }
    }

    /// Records the requests it is asked to reassess.
    #[derive(Default)]
    struct MockQueue(Arc<Mutex<Vec<IndexingRequestId>>>);

    #[async_trait]
    impl WorkerQueue for MockQueue {
        async fn send_indexing_agreement_proposal(
            &self,
            _candidate_url: url::Url,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _deployment_id: thegraph_core::DeploymentId,
            _deployment_chain_id: u64,
            _priority: JobPriority,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            unimplemented!()
        }
        async fn reassess_indexing_request(
            &self,
            indexing_request_id: IndexingRequestId,
            _deployment_id: thegraph_core::DeploymentId,
            _deployment_chain_id: u64,
            _num_candidates: usize,
            _priority: JobPriority,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            self.0.lock().unwrap().push(indexing_request_id);
            Ok(dipper_pgmq::JobId::default())
        }
        async fn submit_offer(
            &self,
            _agreement_id: IndexingAgreementId,
            _indexing_request_id: IndexingRequestId,
            _indexer_url: url::Url,
            _deployment_id: thegraph_core::DeploymentId,
            _deployment_chain_id: u64,
            _priority: JobPriority,
        ) -> anyhow::Result<dipper_pgmq::JobId> {
            unimplemented!()
        }
    }

    fn registry_with_one_abandoned() -> MockRegistry {
        let mut registry = registry_with_one(true);
        registry.cancelling[0].abandoned = true;
        registry
    }

    /// Nothing else finishes a cancel, so the retry can't wait on the chain listener, which
    /// config can turn off.
    #[tokio::test(start_paused = true)]
    async fn sweeps_on_its_own_and_queues_the_replacement_of_an_abandoned_agreement_it_ends() {
        let registry = registry_with_one_abandoned();
        let request = registry.cancelling[0].agreement.indexing_request_id;
        let chain = Arc::new(live_chain());
        let queue = MockQueue::default();
        let reassessed = Arc::clone(&queue.0);
        let (handle, service) = new(Ctx {
            registry,
            chain_client: Arc::clone(&chain),
            agreement_conf: Arc::new(IndexingAgreementConfig::for_tests()),
            worker_queue: queue,
        });
        let service = tokio::spawn(service);

        tokio::time::sleep(SWEEP_INTERVAL + Duration::from_secs(1)).await;
        handle.stop().await;

        service.await.unwrap().unwrap();
        assert_eq!(
            chain.clock_reads.load(Ordering::SeqCst),
            2,
            "1 sweep at start, 1 later"
        );
        assert_eq!(*reassessed.lock().unwrap(), vec![request]);
    }

    /// The liveness checker leaves replacing an abandoned agreement to whoever ends it, when
    /// its own cancel failed and the agreement may still be paid.
    #[tokio::test]
    async fn reports_only_the_abandoned_agreements_it_ends() {
        let config = IndexingAgreementConfig::for_tests();
        for (registry, chain, replaced) in [
            (registry_with_one_abandoned(), live_chain(), true),
            (registry_with_one(true), live_chain(), false),
            (
                registry_with_one_abandoned(),
                MockChain {
                    read_fails: true,
                    ..live_chain()
                },
                false,
            ),
        ] {
            let ended = retry_cancelling_agreements(&registry, &chain, &config).await;

            let ids: Vec<_> = ended.iter().map(|agreement| agreement.id).collect();
            let expected = if replaced {
                vec![registry.cancelling[0].agreement.id]
            } else {
                Vec::new()
            };
            assert_eq!(ids, expected);
        }
    }

    #[tokio::test]
    async fn waits_for_the_next_sweep_when_the_chain_time_cannot_be_read() {
        let registry = registry_with_one(true);
        let chain = MockChain {
            clock_fails: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 0);
        assert_eq!(registry.checks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn leaves_the_chain_alone_when_nothing_is_being_cancelled() {
        let registry = MockRegistry::default();
        let chain = live_chain();

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.clock_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancels_a_live_accepted_agreement_and_records_the_cancel() {
        let registry = registry_with_one(true);
        let chain = live_chain();

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 1);
        assert_eq!(registry.marked_cancelled.lock().unwrap().len(), 1);
        let tx = B256::repeat_byte(0xcd).to_string();
        assert_eq!(*registry.audits.lock().unwrap(), vec![Some(tx)]);
    }

    #[tokio::test]
    async fn records_the_cancel_before_marking_the_agreement_ended() {
        // The end is announced once the mark lands; recorded after, the announcement could
        // go out without its transaction and never be sent again.
        let registry = registry_with_one(true);

        retry(&registry, &live_chain(), 0).await;

        assert_eq!(
            *registry.writes.lock().unwrap(),
            vec!["cancel recorded", "ended"]
        );
    }

    #[tokio::test]
    async fn counts_an_agreement_the_listener_marked_ended_first_as_ended() {
        // Not a failure: there is nothing left to retry.
        let registry = MockRegistry {
            listener_ended_it: true,
            ..registry_with_one(true)
        };

        retry(&registry, &live_chain(), 0).await;

        assert_eq!(registry.checks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn leaves_an_accepted_agreement_that_already_ended_to_the_listener() {
        // The indexer may have ended it, or an earlier cancel whose result went unread;
        // the chain listener reads which, and records when and in which transaction.
        let registry = registry_with_one(true);
        let chain = MockChain::default();

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 0);
        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
        assert!(registry.audits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn marks_an_ended_accepted_agreement_itself_once_the_listener_has_had_long_enough() {
        // In case the listener never reads its end, it would otherwise stay cancelling.
        let mut registry = registry_with_one(true);
        registry.cancelling[0].ended_seen_at =
            Some(time::OffsetDateTime::now_utc() - LISTENER_GRACE - time::Duration::MINUTE);
        let chain = MockChain::default();

        retry(&registry, &chain, 0).await;

        assert_eq!(registry.marked_cancelled.lock().unwrap().len(), 1);
        assert!(registry.audits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn gives_the_listener_its_hour_from_when_the_end_is_first_seen() {
        // Cancelling for hours, as while the manager was paused, mustn't count towards it.
        let mut registry = registry_with_one(true);
        registry.cancelling[0].agreement.updated_at =
            time::OffsetDateTime::now_utc() - LISTENER_GRACE * 3;
        let chain = MockChain::default();

        retry(&registry, &chain, 0).await;

        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
        assert_eq!(*registry.found_ended.lock().unwrap(), vec![Some(true)]);
    }

    #[tokio::test]
    async fn notes_a_live_agreement_as_not_ended_and_an_unread_one_as_unknown() {
        let registry = registry_with_one(true);
        let chain = MockChain {
            cancel_has_no_effect: true,
            ..live_chain()
        };
        retry(&registry, &chain, 0).await;

        let unread = MockChain {
            read_fails: true,
            ..MockChain::default()
        };
        retry(&registry, &unread, 0).await;

        assert_eq!(
            *registry.found_ended.lock().unwrap(),
            vec![Some(false), None]
        );
    }

    #[tokio::test]
    async fn leaves_an_end_by_the_indexer_to_the_listener_for_a_while() {
        // The listener records when and in which transaction. An accepted agreement can lack
        // an accept time, if accepted before accepts were recorded, so both kinds are checked.
        for accepted_on_chain in [true, false] {
            let registry = registry_with_one(accepted_on_chain);
            let chain = MockChain {
                ended_by_indexer: true,
                ..MockChain::default()
            };

            retry(&registry, &chain, DEADLINE + 1).await;

            assert!(registry.marked_cancelled.lock().unwrap().is_empty());
            assert!(registry.marked_by_indexer.lock().unwrap().is_empty());
            assert_eq!(registry.checks.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn marks_an_end_by_the_indexer_as_theirs_once_the_listener_has_had_long_enough() {
        for accepted_on_chain in [true, false] {
            let mut registry = registry_with_one(accepted_on_chain);
            registry.cancelling[0].ended_seen_at =
                Some(time::OffsetDateTime::now_utc() - LISTENER_GRACE - time::Duration::MINUTE);
            let chain = MockChain {
                ended_by_indexer: true,
                ..MockChain::default()
            };

            retry(&registry, &chain, DEADLINE + 1).await;

            assert!(registry.marked_cancelled.lock().unwrap().is_empty());
            assert_eq!(registry.marked_by_indexer.lock().unwrap().len(), 1);
            let indexer = registry.cancelling[0].agreement.indexer.id.to_string();
            assert_eq!(*registry.audited_by.lock().unwrap(), vec![indexer]);
            assert_eq!(*registry.audits.lock().unwrap(), vec![None]);
        }
    }

    #[tokio::test]
    async fn reads_an_ended_agreement_once_to_learn_who_ended_it() {
        let mut registry = registry_with_one(true);
        registry.cancelling[0].ended_seen_at =
            Some(time::OffsetDateTime::now_utc() - LISTENER_GRACE - time::Duration::MINUTE);
        let chain = MockChain {
            ended_by_indexer: true,
            ..MockChain::default()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.reads.load(Ordering::SeqCst), 1);
        assert_eq!(registry.marked_by_indexer.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn leaves_an_end_the_indexer_beat_dipper_to_as_theirs() {
        // Dipper's cancel mined as a no-op after the indexer's; recording it as dipper's
        // would announce the wrong canceller, and the listener's details would be ignored.
        let registry = registry_with_one(true);
        let chain = MockChain {
            indexer_ends_it_first: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 1);
        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
        assert!(registry.audits.lock().unwrap().is_empty());
        assert_eq!(registry.attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn neither_counts_nor_confirms_a_mined_cancel_it_could_not_read_back() {
        // It may well have worked; the next check reads the agreement again.
        let registry = registry_with_one(true);
        let chain = MockChain {
            read_back_fails: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 1);
        assert_eq!(registry.attempts.load(Ordering::SeqCst), 0);
        assert_eq!(registry.checks.load(Ordering::SeqCst), 1);
        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn keeps_an_unaccepted_agreement_cancelling_until_its_deadline() {
        // An offer still in flight could land and be accepted until then.
        let registry = registry_with_one(false);
        let chain = MockChain::default();

        retry(&registry, &chain, DEADLINE).await;
        assert!(registry.marked_cancelled.lock().unwrap().is_empty());

        retry(&registry, &chain, DEADLINE + 1).await;
        assert_eq!(registry.marked_cancelled.lock().unwrap().len(), 1);
        assert!(registry.audits.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn withdraws_a_live_offer_before_its_deadline_and_keeps_watching() {
        let registry = registry_with_one(false);
        let chain = live_chain();

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 1);
        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn notes_each_check_that_leaves_an_agreement_cancelling() {
        // So the next sweep starts with the agreements checked longest ago.
        let registry = registry_with_one(false);
        let chain = MockChain::default();

        retry(&registry, &chain, DEADLINE).await;

        assert_eq!(registry.checks.load(Ordering::SeqCst), 1);
        assert_eq!(registry.attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn never_confirms_an_agreement_it_could_not_read() {
        // Past its deadline but unread, it may be live: an accept the listener hasn't
        // recorded, or one from before accepts were recorded.
        let registry = registry_with_one(false);
        let chain = MockChain {
            read_fails: true,
            ..live_chain()
        };

        retry(&registry, &chain, DEADLINE + 1).await;

        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
        assert_eq!(registry.checks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_unreachable_chain_neither_sends_nor_counts_an_attempt() {
        let registry = registry_with_one(true);
        let chain = MockChain {
            read_fails: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 0);
        assert_eq!(registry.attempts.load(Ordering::SeqCst), 0);
        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn counts_a_cancel_that_could_not_be_sent() {
        // The chain was just read, so a send that fails every time (gas over the cap, signer
        // out of funds) is no outage, and only counting it reaches the alert.
        let refusals: [fn() -> ChainClientError; 2] = [
            || ChainClientError::SubmitFailed(anyhow::anyhow!("Gas price exceeds maximum")),
            || {
                ChainClientError::RpcError(anyhow::anyhow!(
                    "Gas estimation failed: insufficient funds"
                ))
            },
        ];
        for err in refusals {
            let registry = registry_with_one(true);
            let chain = MockChain {
                send_error: Some(err),
                ..live_chain()
            };

            retry(&registry, &chain, 0).await;

            assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 0);
            assert_eq!(registry.attempts.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn gives_up_at_once_on_an_agreement_it_can_never_cancel() {
        // Without a stored terms hash no cancel can be sent, so retrying only delays the alert.
        let mut registry = registry_with_one(true);
        registry.cancelling[0].agreement.terms_version_hash = None;
        let chain = live_chain();

        retry(&registry, &chain, 0).await;

        assert_eq!(
            registry.attempts.load(Ordering::SeqCst),
            MAX_CANCEL_ATTEMPTS
        );
        assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn counts_a_cancel_that_is_mined_and_reverts() {
        // Each one costs gas, so it can't be retried without limit.
        let registry = registry_with_one(true);
        let chain = MockChain {
            mined_cancel_reverts: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(registry.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn counts_a_cancel_that_never_mines() {
        // Otherwise one that keeps being dropped is sent every sweep for ever, never alerting.
        let registry = registry_with_one(true);
        let chain = MockChain {
            never_mines: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(registry.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_count_a_cancel_whose_receipt_could_not_be_checked() {
        // Every receipt check failing is an outage, which may hide a cancel that mined.
        let registry = registry_with_one(true);
        let chain = MockChain {
            receipt_unreadable: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(registry.attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn counts_a_cancel_the_contract_refuses_before_it_is_sent() {
        // Otherwise one that always reverts is retried, and alerted on, for ever.
        let registry = registry_with_one(true);
        let chain = MockChain {
            reverts_before_sending: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(registry.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn counts_a_cancel_that_mines_without_ending_the_agreement() {
        let registry = registry_with_one(true);
        let chain = MockChain {
            cancel_has_no_effect: true,
            ..live_chain()
        };

        retry(&registry, &chain, 0).await;

        assert_eq!(registry.attempts.load(Ordering::SeqCst), 1);
        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
    }
}
