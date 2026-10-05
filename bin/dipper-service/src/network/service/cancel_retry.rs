//! Finishes the cancels dipper starts. An agreement dipper wants ended is marked
//! `Cancelling` before its on-chain cancel goes out; this sweep re-sends the cancel while
//! the chain shows it live, and marks it ended once it can no longer be: `CanceledByRequester`,
//! or `AbandonedByIndexer` for one dipper ended because its indexer stopped serving it.

use dipper_core::time::now_secs;
use thegraph_core::alloy::primitives::B256;

use crate::{
    cancel_dispatch::{
        CancelReason, LiveCancel, cancel_if_live, confirm_cancelled, log_unconfirmed,
    },
    chain_client::{ChainClient, ChainClientError},
    config::IndexingAgreementConfig,
    registry::{AgreementRegistry, CancelKind, CancellingAgreement, IndexingAgreement},
};

/// Failed cancels before dipper alerts an operator and retries the agreement only hourly, so a
/// paused manager recovers once unpaused. Outages don't count (see `failed_attempts`).
pub const MAX_CANCEL_ATTEMPTS: u32 = 10;

/// Agreements a sweep takes on, those that may be paying an indexer first; the time budget
/// below decides how many it gets through.
const BATCH_SIZE: i64 = 50;

/// Time a sweep may take before leaving the rest to the next one: it holds up the chain
/// listener while it runs, and each cancel can wait up to 15 s to be mined.
const SWEEP_BUDGET: std::time::Duration = std::time::Duration::from_secs(30);

/// Minutes an agreement stays out of the retry after it is marked, so the cancel sent
/// when it was marked can be mined first instead of being sent again. One moved back to
/// cancelling had none sent, so it doesn't wait.
const SETTLE_MINUTES: i32 = 2;

/// How long the chain listener gets, from when a check first finds an agreement ended, to
/// record when and in which transaction it ended, before the retry marks it without them.
const LISTENER_GRACE: time::Duration = time::Duration::HOUR;

/// Retry the cancel of agreements still `Cancelling`. The chain's own latest block time
/// decides when an offer that was never accepted no longer can be, so a subgraph that has
/// fallen behind doesn't hold that up.
pub async fn retry_cancelling_agreements<R, T>(
    registry: &R,
    chain_client: &T,
    config: &IndexingAgreementConfig,
) where
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
            return;
        }
    };
    if cancelling.is_empty() {
        return;
    }
    let Some(chain_now) = chain_time(chain_client).await else {
        return;
    };
    let started = std::time::Instant::now();
    for (done, row) in cancelling.iter().enumerate() {
        if started.elapsed() >= SWEEP_BUDGET {
            tracing::info!(
                left = cancelling.len() - done,
                "Cancel retry ran out of time; the rest wait for the next sweep"
            );
            break;
        }
        retry_cancel(registry, chain_client, config, row, chain_now).await;
    }
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

async fn retry_cancel<R, T>(
    registry: &R,
    chain_client: &T,
    config: &IndexingAgreementConfig,
    row: &CancellingAgreement,
    chain_now: u64,
) where
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
                return note_check(registry, row, None, None).await;
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
                return note_check(registry, row, None, None).await;
            }
        };
    if failure.is_none()
        && confirm_if_over(registry, config, row, tx_hash, by_indexer, chain_now).await
    {
        return;
    }
    // A cancel that failed found it live; otherwise it is over, or withdrawn until its deadline.
    note_check(registry, row, failure.as_ref(), Some(failure.is_none())).await;
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
                log_failed_cancel(agreement, attempts, failed_attempts, err);
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
    agreement: &IndexingAgreement,
    attempts: u32,
    failed: u32,
    err: &ChainClientError,
) {
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
        attempts,
        error = %err,
        "Cancelling an agreement keeps failing; it may still be live. Dipper now retries it hourly"
    );
}

/// How many of an agreement's cancel attempts a failure uses up. A cancel the contract
/// refused, before sending or once mined, that mined without ending the agreement, or that an
/// endpoint took but never mined counts, and one that can never be sent uses them all. An
/// unreachable chain is retried freely.
fn failed_attempts(err: &ChainClientError) -> u32 {
    match err {
        ChainClientError::CancelNotConfirmed { .. }
        | ChainClientError::TxReverted { .. }
        | ChainClientError::TxDropped { .. }
        | ChainClientError::ContractRevert { .. } => 1,
        ChainClientError::MissingTermsVersionHash { .. } => MAX_CANCEL_ATTEMPTS,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    };

    use async_trait::async_trait;
    use dipper_core::ids::IndexingAgreementId;
    use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
    use thegraph_core::alloy::primitives::Address;

    use super::*;
    use crate::{
        cancel_dispatch::tests::agreement,
        chain_client::AgreementOnChain,
        registry::{IndexingAgreementStatus, StubAgreementRegistry},
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
        send_fails: bool,
        mined_cancel_reverts: bool,
        never_mines: bool,
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
            if self.send_fails {
                return Err(ChainClientError::RpcError(anyhow::anyhow!("rpc down")));
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
            if self.never_mines {
                return Err(ChainClientError::TxDropped {
                    tx_hash: B256::repeat_byte(0xdd),
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
        for chain in [
            MockChain {
                read_fails: true,
                ..live_chain()
            },
            MockChain {
                send_fails: true,
                ..live_chain()
            },
        ] {
            let registry = registry_with_one(true);

            retry(&registry, &chain, 0).await;

            assert_eq!(chain.cancels_sent.load(Ordering::SeqCst), 0);
            assert_eq!(registry.attempts.load(Ordering::SeqCst), 0);
            assert!(registry.marked_cancelled.lock().unwrap().is_empty());
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
