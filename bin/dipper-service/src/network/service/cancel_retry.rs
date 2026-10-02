//! Finishes the cancels dipper starts. An agreement dipper wants ended is marked
//! `Cancelling` before its on-chain cancel goes out; this sweep re-sends the cancel while
//! the chain shows it live, and marks it `CanceledByRequester` once it can no longer be.

use dipper_core::time::now_secs;
use thegraph_core::alloy::primitives::B256;

use crate::{
    cancel_dispatch::{LiveCancel, cancel_if_live, confirm_cancelled},
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
/// when it was marked can be mined first instead of being sent again.
const SETTLE_MINUTES: i32 = 2;

/// How long the chain listener gets to record when, and in which transaction, an accepted
/// agreement ended, before the retry marks it ended without those details.
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
    let Some(chain_now) = chain_time(chain_client).await else {
        return;
    };
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
    let (tx_hash, failure) = match cancel_if_live(chain_client, &row.agreement, config).await {
        LiveCancel::ReadFailed(err) => {
            tracing::warn!(
                %agreement_id,
                error = %err,
                "Failed to read a cancelling agreement on-chain, will retry"
            );
            // Unread, it may still be live, so it can't be confirmed ended.
            return note_check(registry, row, None).await;
        }
        LiveCancel::NotLive => (None, None),
        LiveCancel::Ended(tx_hash) => {
            tracing::info!(
                %agreement_id,
                tx_hash = ?tx_hash,
                "Cancelled an agreement still live on-chain"
            );
            (tx_hash, None)
        }
        LiveCancel::CancelFailed(err) => (None, Some(err)),
    };
    if failure.is_none()
        && confirm_if_over(registry, chain_client, config, row, tx_hash, chain_now).await
    {
        return;
    }
    note_check(registry, row, failure.as_ref()).await;
}

/// Mark the agreement `CanceledByRequester` once it can't go live again: this sweep's cancel
/// ended it, or nobody accepted its offer before the deadline to. One ended otherwise is left
/// to the chain listener for a while; one the indexer ended then becomes `CanceledByIndexer`.
async fn confirm_if_over<R, T>(
    registry: &R,
    chain_client: &T,
    config: &IndexingAgreementConfig,
    row: &CancellingAgreement,
    tx_hash: Option<B256>,
    chain_now: u64,
) -> bool
where
    R: AgreementRegistry + Sync,
    T: ChainClient,
{
    let agreement = &row.agreement;
    let past_grace = agreement.updated_at < time::OffsetDateTime::now_utc() - LISTENER_GRACE;
    let can_confirm = if row.accepted_on_chain {
        tx_hash.is_some() || past_grace
    } else {
        chain_now > agreement.terms.deadline
    };
    if !can_confirm {
        return false;
    }
    if tx_hash.is_none() {
        match ended_by_indexer(chain_client, agreement).await {
            None => return false,
            Some(true) => return past_grace && record_end_by_indexer(registry, agreement).await,
            Some(false) => {}
        }
    }
    confirm_cancelled(registry, agreement, tx_hash, config).await
}

/// Whether the chain shows the indexer ended the agreement, or `None` when it can't be read,
/// so an end is never wrongly put down to dipper.
async fn ended_by_indexer<T: ChainClient>(
    chain_client: &T,
    agreement: &IndexingAgreement,
) -> Option<bool> {
    match chain_client
        .agreement_ended_by_indexer(agreement.id.as_bytes())
        .await
    {
        Ok(by_indexer) => {
            if by_indexer {
                tracing::info!(
                    agreement_id = %agreement.id,
                    "The indexer ended an agreement dipper was cancelling"
                );
            }
            Some(by_indexer)
        }
        Err(err) => {
            tracing::warn!(
                agreement_id = %agreement.id,
                error = %err,
                "Failed to read who ended a cancelling agreement, will retry"
            );
            None
        }
    }
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

/// Record that the agreement was checked and is still cancelling, counting a cancel the
/// chain answered without ending it; past the limit, dipper gives up with an ERROR.
async fn note_check<R: AgreementRegistry + Sync>(
    registry: &R,
    row: &CancellingAgreement,
    failure: Option<&ChainClientError>,
) {
    let agreement = &row.agreement;
    let failed_attempts = failure.map_or(0, failed_attempts);
    if let Some(err) = failure
        && failed_attempts == 0
    {
        log_uncounted_failure(agreement, err);
    }
    match registry
        .record_cancel_check(&agreement.id, failed_attempts)
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
            "Cancel did not end the agreement, will retry"
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
/// refused, before sending or once mined, or that mined without ending the agreement counts,
/// and one that can never be sent uses them all. An unreachable chain is retried freely.
fn failed_attempts(err: &ChainClientError) -> u32 {
    match err {
        ChainClientError::CancelNotConfirmed { .. }
        | ChainClientError::TxReverted { .. }
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
        ) -> crate::registry::Result<u32> {
            self.checks.fetch_add(1, Ordering::SeqCst);
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
        reverts_before_sending: bool,
        cancel_has_no_effect: bool,
        clock_fails: bool,
        now: AtomicU64,
        ended_by_indexer: bool,
        who_read_fails: bool,
        cancels_sent: AtomicU32,
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
            self.cancels_sent.fetch_add(1, Ordering::SeqCst);
            if !self.cancel_has_no_effect {
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
        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            if self.read_fails {
                return Err(ChainClientError::RpcError(anyhow::anyhow!("rpc down")));
            }
            Ok(self.live.load(Ordering::SeqCst))
        }
        async fn agreement_ended_by_indexer(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            if self.who_read_fails {
                return Err(ChainClientError::RpcError(anyhow::anyhow!("rpc down")));
            }
            Ok(self.ended_by_indexer)
        }
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
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
        registry.cancelling[0].agreement.updated_at =
            time::OffsetDateTime::now_utc() - LISTENER_GRACE - time::Duration::MINUTE;
        let chain = MockChain::default();

        retry(&registry, &chain, 0).await;

        assert_eq!(registry.marked_cancelled.lock().unwrap().len(), 1);
        assert!(registry.audits.lock().unwrap().is_empty());
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
            registry.cancelling[0].agreement.updated_at =
                time::OffsetDateTime::now_utc() - LISTENER_GRACE - time::Duration::MINUTE;
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
    async fn does_not_confirm_an_end_when_who_ended_it_cannot_be_read() {
        let mut registry = registry_with_one(true);
        registry.cancelling[0].agreement.updated_at =
            time::OffsetDateTime::now_utc() - LISTENER_GRACE - time::Duration::MINUTE;
        let chain = MockChain {
            who_read_fails: true,
            ..MockChain::default()
        };

        retry(&registry, &chain, 0).await;

        assert!(registry.marked_cancelled.lock().unwrap().is_empty());
        assert!(registry.marked_by_indexer.lock().unwrap().is_empty());
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
