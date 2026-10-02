//! Finishes the cancels dipper starts. An agreement dipper wants ended is marked
//! `Cancelling` before its on-chain cancel goes out; this sweep re-sends the cancel while
//! the chain shows it live, and marks it `CanceledByRequester` once it can no longer be.

use thegraph_core::alloy::primitives::B256;

use crate::{
    cancel_dispatch::{LiveCancel, cancel_if_live, record_cancel},
    chain_client::{ChainClient, ChainClientError},
    config::IndexingAgreementConfig,
    registry::{AgreementRegistry, CancellingAgreement, IndexingAgreement},
};

/// Retried cancels mined without ending an agreement before dipper stops retrying it and
/// leaves it to an operator. Other failures don't count (see `failed_attempts`).
pub const MAX_CANCEL_ATTEMPTS: u32 = 10;

/// Agreements checked per sweep, those checked longest ago first. Each can wait up to
/// 15 s for a cancel to be mined, holding up the chain listener meanwhile.
const BATCH_SIZE: i64 = 10;

/// Minutes an agreement stays out of the retry after it is marked, so the cancel sent
/// when it was marked can be mined first instead of being sent again.
const SETTLE_MINUTES: i32 = 2;

/// Retry the cancel of agreements still `Cancelling`. `chain_now`, in chain seconds,
/// decides when an offer that was never accepted no longer can be.
pub async fn retry_cancelling_agreements<R, T>(
    registry: &R,
    chain_client: &T,
    config: &IndexingAgreementConfig,
    chain_now: u64,
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
    for row in &cancelling {
        retry_cancel(registry, chain_client, config, row, chain_now).await;
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
            (None, None)
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
    if failure.is_none() && confirm_if_over(registry, config, row, tx_hash, chain_now).await {
        return;
    }
    note_check(registry, row, failure.as_ref()).await;
}

/// Mark the agreement `CanceledByRequester` once it can't go live again: this sweep's cancel
/// ended it, or nobody accepted its offer before the deadline to. One accepted that ended
/// otherwise is left to the chain listener, which reads who ended it and when.
async fn confirm_if_over<R: AgreementRegistry + Sync>(
    registry: &R,
    config: &IndexingAgreementConfig,
    row: &CancellingAgreement,
    tx_hash: Option<B256>,
    chain_now: u64,
) -> bool {
    let agreement = &row.agreement;
    let can_confirm = if row.accepted_on_chain {
        tx_hash.is_some()
    } else {
        chain_now > agreement.terms.deadline
    };
    if !can_confirm {
        return false;
    }
    if let Err(err) = registry
        .mark_indexing_agreement_as_canceled_by_requester(&agreement.id)
        .await
    {
        tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "Failed to mark an ended agreement cancelled, will retry"
        );
        return false;
    }
    tracing::info!(
        agreement_id = %agreement.id,
        indexing_request_id = %agreement.indexing_request_id,
        old_status = "CANCELLING",
        new_status = "CANCELED_BY_REQUESTER",
        reason = "cancel_confirmed_on_chain",
        "agreement state transition"
    );
    if row.accepted_on_chain {
        record_cancel(registry, agreement, tx_hash, config).await;
    }
    true
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
        tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "Failed to send the cancel of an agreement, will retry"
        );
    }
    match registry
        .record_cancel_check(&agreement.id, failed_attempts)
        .await
    {
        Ok(attempts) => {
            if let Some(err) = failure.filter(|_| failed_attempts > 0) {
                log_failed_cancel(agreement, attempts, err);
            }
        }
        Err(err) => tracing::warn!(
            agreement_id = %agreement.id,
            error = %err,
            "Failed to record a check of a cancelling agreement"
        ),
    }
}

fn log_failed_cancel(agreement: &IndexingAgreement, attempts: u32, err: &ChainClientError) {
    if attempts < MAX_CANCEL_ATTEMPTS {
        tracing::warn!(
            agreement_id = %agreement.id,
            attempts,
            error = %err,
            "Cancel did not end the agreement, will retry"
        );
        return;
    }
    tracing::error!(
        event = "agreement_cancel_abandoned",
        agreement_id = %agreement.id,
        indexer_id = %agreement.indexer.id,
        indexing_request_id = %agreement.indexing_request_id,
        attempts,
        error = %err,
        "Gave up cancelling an agreement on-chain; it may still be live"
    );
}

/// How many of an agreement's cancel attempts a failure uses up. Only a cancel mined without
/// ending it counts, and one that can never be sent uses them all. An unreachable chain or a
/// misconfigured or paused manager isn't the agreement's doing, so it is retried freely.
fn failed_attempts(err: &ChainClientError) -> u32 {
    match err {
        ChainClientError::CancelNotConfirmed { .. } => 1,
        ChainClientError::MissingTermsVersionHash { .. } => MAX_CANCEL_ATTEMPTS,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
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
        audits: Mutex<Vec<Option<String>>>,
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
            _canceled_by: &str,
            canceled_tx: Option<&str>,
        ) -> crate::registry::Result<()> {
            self.audits
                .lock()
                .unwrap()
                .push(canceled_tx.map(str::to_owned));
            Ok(())
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
        cancel_has_no_effect: bool,
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
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            unimplemented!()
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
        retry_cancelling_agreements(registry, chain, &config, chain_now).await;
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
