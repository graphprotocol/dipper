//! AlloyChainClient: the production implementation of the `ChainClient` trait, using
//! alloy for Ethereum interactions.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
use thegraph_core::alloy::{
    eips::{BlockNumberOrTag, eip2718::Encodable2718},
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, B256, FixedBytes},
    providers::Provider,
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol_types::{SolCall, SolValue},
    transports::TransportError,
};
use tokio::sync::Mutex;

use super::{
    abi::{IRecurringAgreementManager, IRecurringCollector},
    gas::{GasEstimator, calculate_max_fee, exceeds_max_gas_price, get_gas_prices},
    rpc_provider::RpcProviderPool,
};
use crate::{
    chain_client::{ChainClient, ChainClientError},
    config::ChainClientConfig,
    worker::service::PROCESS_JOB_TIMEOUT,
};

/// OFFER_TYPE_NEW from `RecurringCollector.sol`, used when submitting a new agreement
/// offer on-chain. The contract defines NONE=0, NEW=1, UPDATE=2; passing 0 reverts with
/// RecurringCollectorInvalidOfferType(0).
const OFFER_TYPE_NEW: u8 = 1;

/// Time to wait for a tx receipt before declaring the tx dropped from the mempool: ~15
/// blocks on hardhat at 1s each, 60 confirmations on Arbitrum at 0.25s. Short enough that
/// the pgmq retry budget recovers inside the RCA deadline `deadline_seconds` sets.
const RECEIPT_POLL_TIMEOUT: Duration = Duration::from_secs(15);

/// Interval between `eth_getTransactionReceipt` polls while waiting for a
/// tx to mine. Tight enough to respond quickly on sub-second block times,
/// loose enough to avoid hammering the RPC.
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// VERSION_CURRENT index from `IAgreementCollector.sol`: the active (or
/// pre-acceptance) terms. `getAgreementDetails(id, 0)` reports their state.
const VERSION_CURRENT: u64 = 0;

/// `AgreementDetails.state` flags from `IAgreementCollector.sol` (ACCEPTED=2,
/// NOTICE_GIVEN=4). `getAgreementDetails` keeps ACCEPTED set on a canceled
/// agreement and ORs in NOTICE_GIVEN, so a cancel must clear it, not just lack it.
const STATE_ACCEPTED: u16 = 2;
const STATE_NOTICE_GIVEN: u16 = 4;

/// Error patterns that indicate a nonce-related issue.
///
/// These errors can be resolved by refreshing the nonce and retrying.
const NONCE_ERROR_PATTERNS: &[&str] = &[
    "nonce too low",
    "nonce is too low",
    "invalid nonce",
    "replacement transaction underpriced",
    // A backstop only. `is_already_broadcast` claims this wording first and reports the
    // broadcast as the success it is, so a send never reaches here saying it.
    "already known",
];

/// Check if an error message indicates a nonce problem.
fn is_nonce_error(error: &str) -> bool {
    let lower = error.to_lowercase();
    NONCE_ERROR_PATTERNS.iter().any(|p| lower.contains(p))
}

/// Number of attempts `sign_and_send` makes at finding a nonce the chain will take.
const MAX_NONCE_RETRIES: u32 = 2;

/// How long one submission may hold `submit_lock` before giving up, derived from the retry
/// schedule so the cut-off can never pre-empt a retry the config allows: one full walk of
/// the endpoint ring to read the nonce and another to send. A second nonce attempt is not
/// budgeted separately: it only follows an endpoint answering with a nonce rejection, and
/// an endpoint that answers is not one that spends its whole retry schedule hanging.
fn derive_submit_deadline(pool: &RpcProviderPool) -> Duration {
    let budget = pool.worst_case_walk() * 2;
    // The rest of the worker job's budget stays reserved for what follows the broadcast:
    // the receipt poll and the nonce-gap fill.
    let cap = PROCESS_JOB_TIMEOUT / 5 * 4;
    if budget > cap {
        tracing::warn!(
            budget_secs = budget.as_secs(),
            cap_secs = cap.as_secs(),
            "retry schedule wants more time than a worker job allows; submissions may give \
             up before trying every endpoint"
        );
        return cap;
    }
    budget
}

/// Run a submission under `deadline`, reporting a failed submission if it runs over.
/// Giving up releases the lock, and the caller re-runs the job rather than losing the work.
async fn under_submit_deadline<F>(
    deadline: Duration,
    work: F,
) -> Result<SubmittedTx, ChainClientError>
where
    F: Future<Output = Result<SubmittedTx, ChainClientError>>,
{
    tokio::time::timeout(deadline, work)
        .await
        .unwrap_or_else(|_| {
            Err(ChainClientError::SubmitFailed(anyhow::anyhow!(
                "gave up submitting after {}s holding the submission lock",
                deadline.as_secs()
            )))
        })
}

/// How nodes say they are already holding the transaction being offered to them.
const ALREADY_BROADCAST_PATTERNS: &[&str] =
    &["already known", "already imported", "known transaction"];

/// Whether a rejection means this exact transaction is already in a mempool. Offering the
/// same signed bytes to a second endpoint is expected to land here, and it means the
/// broadcast succeeded, so it must not be mistaken for a reason to send another.
fn is_already_broadcast(error: &TransportError) -> bool {
    let lower = error.to_string().to_lowercase();
    ALREADY_BROADCAST_PATTERNS.iter().any(|p| lower.contains(p))
}

/// Classify the outcome of a nonce-gap fill submission. Pure so the
/// swallow rule (`is_nonce_error` → `Ok(())`) is unit-testable without
/// an RPC mock.
fn classify_fill_nonce_gap_outcome(
    nonce: u64,
    submission: Result<B256, ChainClientError>,
) -> Result<(), ChainClientError> {
    match submission {
        Ok(tx_hash) => {
            tracing::info!(
                event = "nonce_gap_fill_submitted",
                nonce,
                fill_tx_hash = %tx_hash,
                "Submitted noop self-transfer to fill mempool nonce gap"
            );
            Ok(())
        }
        Err(e) if is_nonce_error(&e.to_string()) => {
            tracing::info!(
                event = "nonce_gap_fill_nonce_rejected",
                nonce,
                error = %e,
                "Nonce-gap fill rejected; original still in flight or gap already filled"
            );
            Ok(())
        }
        Err(e) => {
            tracing::warn!(
                event = "nonce_gap_fill_failed",
                nonce,
                error = %e,
                "Nonce-gap fill submission failed; wallet may stay wedged until the original tx clears or another fill succeeds"
            );
            Err(e)
        }
    }
}

/// Production implementation of `ChainClient` using alloy. `Clone` via internal `Arc`
/// wrapping, so it can be shared across async task contexts.
#[derive(Clone)]
pub struct AlloyChainClient {
    /// Inner state wrapped in Arc for Clone support
    inner: Arc<AlloyChainClientInner>,
}

/// Sentinel value indicating the nonce has not been fetched from chain yet.
const NONCE_UNINITIALIZED: u64 = u64::MAX;

/// Tx submission outcome carrying both the hash and the reserved nonce, so
/// callers that need to recover from an evicted tx (`fill_nonce_gap`) know
/// which slot to fill without re-querying the RPC.
#[derive(Debug, Clone, Copy)]
struct SubmittedTx {
    hash: B256,
    nonce: u64,
}

/// Inner state for AlloyChainClient
struct AlloyChainClientInner {
    /// RPC provider pool with failover
    rpc_pool: RpcProviderPool,
    /// Gas estimator with bounds
    gas_estimator: GasEstimator,
    /// Transaction signer
    signer: PrivateKeySigner,
    /// RecurringCollector contract address (for RCA offer submission)
    recurring_collector_address: Address,
    /// RecurringAgreementManager address: the on-chain payer the manager-routed
    /// offer/cancel paths drive.
    recurring_agreement_manager_address: Address,
    /// Chain ID
    chain_id: u64,
    /// Gas price multiplier
    gas_price_multiplier: f64,
    /// Maximum gas price in gwei
    max_gas_price_gwei: u64,
    /// The next nonce a submission should use. Read under `submit_lock` and advanced only
    /// once a broadcast succeeds, so a failed submission cannot skip a slot the chain
    /// would then sit waiting on while later transactions queue behind the gap.
    nonce: AtomicU64,
    /// Serializes reading the nonce counter through committing it after the mempool
    /// submission, so two submissions can never take the same slot. Released before
    /// receipt polling so multiple confirmations pipeline concurrently.
    submit_lock: Mutex<()>,
    /// How long one submission may hold `submit_lock`; see `derive_submit_deadline`.
    submit_deadline: Duration,
}

impl AlloyChainClient {
    /// Create a new AlloyChainClient from configuration. Errors if no RPC providers are
    /// configured or the signer cannot be constructed.
    pub fn new(
        config: &ChainClientConfig,
        chain_id: u64,
        recurring_collector: Address,
        recurring_agreement_manager: Address,
        secret_key: &[u8; 32],
    ) -> Result<Self, ChainClientError> {
        let signer = PrivateKeySigner::from_bytes(&FixedBytes::from(*secret_key))
            .map_err(|e| ChainClientError::ConfigError(format!("Invalid signing key: {e}")))?;

        let rpc_pool = RpcProviderPool::new(
            config.providers.clone(),
            config.request_timeout,
            config.max_retries,
        )?;

        let gas_estimator = GasEstimator::new(
            config.gas_buffer_multiplier,
            config.gas_floor,
            config.gas_max_addition,
        );

        tracing::info!(
            signer_address = %signer.address(),
            recurring_collector = %recurring_collector,
            recurring_agreement_manager = %recurring_agreement_manager,
            chain_id,
            "AlloyChainClient initialized"
        );

        let submit_deadline = derive_submit_deadline(&rpc_pool);

        Ok(Self {
            inner: Arc::new(AlloyChainClientInner {
                rpc_pool,
                gas_estimator,
                signer,
                recurring_collector_address: recurring_collector,
                recurring_agreement_manager_address: recurring_agreement_manager,
                chain_id,
                gas_price_multiplier: config.gas_price_multiplier,
                max_gas_price_gwei: config.max_gas_price_gwei,
                nonce: AtomicU64::new(NONCE_UNINITIALIZED),
                submit_lock: Mutex::new(()),
                submit_deadline,
            }),
        })
    }

    /// Build, gas-estimate, and send a call to any contract. Shared entry point for the
    /// manager-routed offer and cancel calls; `log_agreement_id` is only for logging.
    async fn build_and_send_call(
        &self,
        to: Address,
        calldata: Vec<u8>,
        log_agreement_id: &[u8; 16],
    ) -> Result<SubmittedTx, ChainClientError> {
        // 1. Build initial transaction request
        let tx = TransactionRequest::default()
            .from(self.inner.signer.address())
            .to(to)
            .input(calldata.into());

        // 2. Estimate gas with safety bounds. The estimator may surface a structured
        // contract revert (e.g. an already-canceled agreement); box it through alloy's
        // Custom transport variant so the pool hands back the selector and payload intact.
        let gas_limit = self
            .inner
            .rpc_pool
            .execute("estimate_gas", |provider| {
                let tx = tx.clone();
                let estimator = self.inner.gas_estimator.clone();
                async move {
                    estimator.estimate(&provider, &tx).await.map_err(|e| {
                        thegraph_core::alloy::transports::TransportErrorKind::custom(e)
                    })
                }
            })
            .await?;

        // 3. Get gas prices
        let (base_fee, priority_fee) = self
            .inner
            .rpc_pool
            .execute("get_gas_prices", |provider| async move {
                get_gas_prices(&provider).await.map_err(|e| {
                    thegraph_core::alloy::transports::TransportError::local_usage_str(
                        &e.to_string(),
                    )
                })
            })
            .await?;

        // 4. Calculate max fee with multiplier
        let max_fee_per_gas =
            calculate_max_fee(base_fee, priority_fee, self.inner.gas_price_multiplier);

        // 5. Check gas price limit
        if exceeds_max_gas_price(max_fee_per_gas, self.inner.max_gas_price_gwei) {
            return Err(ChainClientError::SubmitFailed(anyhow::anyhow!(
                "Gas price {} gwei exceeds maximum {} gwei",
                max_fee_per_gas / 1_000_000_000,
                self.inner.max_gas_price_gwei
            )));
        }

        // 6. Build final transaction
        let tx = tx
            .with_gas_limit(gas_limit)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(priority_fee)
            .with_chain_id(self.inner.chain_id);

        tracing::debug!(
            agreement_id = %format_args!("0x{}", log_agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            to = %to,
            gas_limit,
            base_fee_gwei = base_fee / 1_000_000_000,
            priority_fee_gwei = priority_fee / 1_000_000_000,
            max_fee_gwei = max_fee_per_gas / 1_000_000_000,
            "Transaction parameters"
        );

        // 7. Sign and send with nonce handling
        self.sign_and_send(tx, log_agreement_id).await
    }

    /// The slot the next submission should use, read from the chain on first call. Only
    /// `commit_nonce` moves the counter on, so a submission that never got a transaction
    /// out leaves its slot to the next caller instead of leaving the chain waiting on it.
    async fn next_nonce(&self) -> Result<u64, ChainClientError> {
        let current = self.inner.nonce.load(Ordering::SeqCst);
        if current != NONCE_UNINITIALIZED {
            return Ok(current);
        }
        let chain_nonce = self.fetch_chain_nonce().await?;
        self.inner.nonce.store(chain_nonce, Ordering::SeqCst);
        Ok(chain_nonce)
    }

    /// Record that `nonce` carried a successful broadcast, so the next submission moves on
    /// to the following slot.
    fn commit_nonce(&self, nonce: u64) {
        self.inner.nonce.fetch_max(nonce + 1, Ordering::SeqCst);
    }

    /// Ratchet the in-memory counter up to the next slot the chain will accept. Never
    /// decreases it, so an endpoint reporting a stale pending count cannot wind the
    /// counter back onto a slot a broadcast has already spent.
    async fn resync_nonce(&self) -> Result<(), ChainClientError> {
        let chain_nonce = self.fetch_chain_nonce().await?;
        self.inner.nonce.fetch_max(chain_nonce, Ordering::SeqCst);
        Ok(())
    }

    /// Fetch the pending transaction count from chain. The "pending" tag counts our own
    /// mempool transactions too; "latest" would miss them and reuse an in-flight nonce.
    async fn fetch_chain_nonce(&self) -> Result<u64, ChainClientError> {
        self.inner
            .rpc_pool
            .execute("get_nonce", |provider| {
                let addr = self.inner.signer.address();
                async move { provider.get_transaction_count(addr).pending().await }
            })
            .await
    }

    /// Sign and send a transaction, re-syncing the nonce from chain and retrying on nonce
    /// errors. `submit_lock` spans the retry loop so two submissions can never interleave;
    /// released before receipt polling so confirmations pipeline concurrently.
    async fn sign_and_send(
        &self,
        tx: TransactionRequest,
        agreement_id: &[u8; 16],
    ) -> Result<SubmittedTx, ChainClientError> {
        let _submit_guard = self.inner.submit_lock.lock().await;
        under_submit_deadline(
            self.inner.submit_deadline,
            self.sign_and_send_locked(tx, agreement_id),
        )
        .await
    }

    /// The work `sign_and_send` does while holding `submit_lock`, split out so the deadline
    /// wraps the work rather than the wait for the lock: a caller queueing politely behind
    /// someone else should not be charged for the time it spent waiting.
    async fn sign_and_send_locked(
        &self,
        mut tx: TransactionRequest,
        agreement_id: &[u8; 16],
    ) -> Result<SubmittedTx, ChainClientError> {
        for attempt in 0..MAX_NONCE_RETRIES {
            if attempt > 0 {
                self.resync_nonce().await?;
            }
            let nonce = self.next_nonce().await?;

            tx = tx.with_nonce(nonce);

            let result = self.send_transaction(&tx).await;

            match result {
                Ok(tx_hash) => {
                    self.commit_nonce(nonce);
                    tracing::info!(
                        agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                        tx_hash = %tx_hash,
                        nonce,
                        "Transaction sent successfully"
                    );
                    return Ok(SubmittedTx {
                        hash: tx_hash,
                        nonce,
                    });
                }
                Err(e) if is_nonce_error(&e.to_string()) && attempt + 1 < MAX_NONCE_RETRIES => {
                    tracing::warn!(
                        agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                        attempt = attempt + 1,
                        nonce,
                        error = %e,
                        "Nonce error, re-syncing from chain and retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(ChainClientError::SubmitFailed(anyhow::anyhow!(
            "Failed to send transaction after {} nonce retries",
            MAX_NONCE_RETRIES
        )))
    }

    /// Submit a self-transfer of 0 wei at `nonce` so the chain has something to mine in a
    /// slot left empty by an evicted tx, releasing higher-nonce txs stuck behind the gap.
    /// Best-effort: an `is_nonce_error` rejection means the slot is spoken for, so success.
    async fn fill_nonce_gap(&self, nonce: u64) -> Result<(), ChainClientError> {
        let _submit_guard = self.inner.submit_lock.lock().await;

        let signer_addr = self.inner.signer.address();
        let (base_fee, priority_fee) = self
            .inner
            .rpc_pool
            .execute("get_gas_prices", |provider| async move {
                get_gas_prices(&provider).await.map_err(|e| {
                    thegraph_core::alloy::transports::TransportError::local_usage_str(
                        &e.to_string(),
                    )
                })
            })
            .await?;
        let max_fee_per_gas =
            calculate_max_fee(base_fee, priority_fee, self.inner.gas_price_multiplier);

        let tx = TransactionRequest::default()
            .from(signer_addr)
            .to(signer_addr)
            .value(thegraph_core::alloy::primitives::U256::ZERO)
            .with_gas_limit(21_000)
            .with_max_fee_per_gas(max_fee_per_gas)
            .with_max_priority_fee_per_gas(priority_fee)
            .with_chain_id(self.inner.chain_id)
            .with_nonce(nonce);

        classify_fill_nonce_gap_outcome(nonce, self.send_transaction(&tx).await)
    }

    /// Broadcast a transaction, retrying and rotating endpoints the way every read call
    /// already does. Signing happens once, up front, so every endpoint is offered the same
    /// bytes under one hash and the hash is known before anyone is asked to accept them.
    async fn send_transaction(&self, tx: &TransactionRequest) -> Result<B256, ChainClientError> {
        // Nothing fills a field in on this path any more, and a request that names no chain is
        // signed for chain 1 rather than refused, so check before the signature exists. Not a
        // `ConfigError`: the cancel path reads that as the chain client being switched off.
        if tx.chain_id() != Some(self.inner.chain_id) {
            return Err(ChainClientError::SubmitFailed(anyhow::anyhow!(
                "refusing to sign for chain {:?} while configured for chain {}",
                tx.chain_id(),
                self.inner.chain_id
            )));
        }

        let wallet = EthereumWallet::from(self.inner.signer.clone());
        // A caller leaving out a field fails here rather than having it filled in, so this is
        // as likely to be an incomplete request as a signing fault. Alloy's own wording names
        // which, so leave the reason to it.
        let signed = tx.clone().build(&wallet).await.map_err(|e| {
            ChainClientError::SubmitFailed(anyhow::anyhow!("Transaction not ready to send: {e}"))
        })?;
        let signed_hash = *signed.tx_hash();
        let raw = signed.encoded_2718();

        self.inner
            .rpc_pool
            .execute("send_transaction", |provider| {
                let raw = raw.clone();
                async move {
                    match provider.send_raw_transaction(&raw).await {
                        // The hash follows from the bytes, so report the one we signed
                        // instead of what this endpoint echoed back. A wrong hash sends the
                        // receipt poll after a transaction that never mines.
                        Ok(_) => Ok(signed_hash),
                        // This endpoint already holds these exact bytes, which is the
                        // outcome we were after. Answer with the hash we signed rather
                        // than reporting a failure that would send a second transaction.
                        Err(e) if is_already_broadcast(&e) => Ok(signed_hash),
                        Err(e) => Err(e),
                    }
                }
            })
            .await
            .map_err(|e| match e {
                // A submission that got nowhere is a failed submission, whatever the
                // endpoints happened to say. Anything the pool could name precisely, such
                // as a rejection from the contract, keeps the name it already has.
                ChainClientError::RpcError(cause) => ChainClientError::SubmitFailed(cause),
                other => other,
            })
    }

    /// Poll `eth_getTransactionReceipt` until the tx has mined or the timeout elapses.
    /// `Ok(Some(status))` reports the receipt's success flag; `Ok(None)` says the tx never
    /// appeared in time (dropped from the mempool). Transient RPC errors keep polling.
    async fn wait_for_receipt(
        &self,
        tx_hash: B256,
        timeout: Duration,
    ) -> Result<Option<bool>, ChainClientError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let receipt = self
                .inner
                .rpc_pool
                .execute("get_transaction_receipt", |provider| async move {
                    provider.get_transaction_receipt(tx_hash).await
                })
                .await;

            match receipt {
                Ok(Some(r)) => return Ok(Some(r.status())),
                Ok(None) => {} // not mined yet
                Err(e) => {
                    // Transient RPC error: log and keep polling. If it persists, the outer
                    // handler sees the timeout as `Ok(None)` and resubmits, the safe default.
                    tracing::debug!(
                        tx_hash = %tx_hash,
                        error = %e,
                        "RPC error polling tx receipt, will retry until timeout"
                    );
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
        }
    }
}

#[async_trait]
impl ChainClient for AlloyChainClient {
    async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
        let block = self
            .inner
            .rpc_pool
            .execute("get_latest_block", |provider| async move {
                provider.get_block_by_number(BlockNumberOrTag::Latest).await
            })
            .await?
            .ok_or_else(|| {
                ChainClientError::RpcError(anyhow::anyhow!("no latest block returned"))
            })?;
        Ok(block.header.timestamp)
    }

    async fn offer_via_manager(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError> {
        let manager = self.inner.recurring_agreement_manager_address;

        let agreement_id = dipper_rpc::indexer::derive_agreement_id(rca);

        // The manager is the payer; dipper is just the operator submitting the
        // tx, so encode offerAgreement(collector, NEW, abi(rca)).
        let calldata = IRecurringAgreementManager::offerAgreementCall {
            collector: self.inner.recurring_collector_address,
            offerType: OFFER_TYPE_NEW,
            offerData: rca.abi_encode().into(),
        }
        .abi_encode();

        tracing::info!(
            agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            manager = %manager,
            collector = %self.inner.recurring_collector_address,
            "Submitting RCA offer via RecurringAgreementManager"
        );

        let submitted = self
            .build_and_send_call(manager, calldata, &agreement_id)
            .await?;

        let SubmittedTx {
            hash: tx_hash,
            nonce: dropped_nonce,
        } = submitted;
        match self.wait_for_receipt(tx_hash, RECEIPT_POLL_TIMEOUT).await? {
            Some(true) => Ok(Some(tx_hash)),
            Some(false) => Err(ChainClientError::TxReverted { tx_hash }),
            None => {
                tracing::warn!(
                    agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                    tx_hash = %tx_hash,
                    nonce = dropped_nonce,
                    "Manager offer tx did not mine within receipt-poll window; treating as dropped"
                );
                if let Err(err) = self.fill_nonce_gap(dropped_nonce).await {
                    tracing::warn!(nonce = dropped_nonce, error = %err, "Failed to fill mempool nonce gap");
                }
                Err(ChainClientError::TxDropped { tx_hash })
            }
        }
    }

    async fn cancel_via_manager(
        &self,
        collector: Address,
        agreement_id: &[u8; 16],
        version_hash: B256,
        options: u16,
    ) -> Result<Option<B256>, ChainClientError> {
        let manager = self.inner.recurring_agreement_manager_address;

        let calldata = IRecurringAgreementManager::cancelAgreementCall {
            collector,
            agreementId: FixedBytes::<16>::from_slice(agreement_id),
            versionHash: version_hash,
            options,
        }
        .abi_encode();

        tracing::info!(
            agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
            manager = %manager,
            options,
            "Canceling agreement via RecurringAgreementManager"
        );

        let submitted = self
            .build_and_send_call(manager, calldata, agreement_id)
            .await?;

        // Wait for the receipt so a returned Ok means the cancel mined, not just
        // that it entered the mempool. The dispatch layer then re-reads on-chain
        // to catch a mined-but-no-op cancel (stale hash, unknown id, terminal).
        let SubmittedTx {
            hash: tx_hash,
            nonce: dropped_nonce,
        } = submitted;
        match self.wait_for_receipt(tx_hash, RECEIPT_POLL_TIMEOUT).await? {
            Some(true) => Ok(Some(tx_hash)),
            Some(false) => Err(ChainClientError::TxReverted { tx_hash }),
            None => {
                tracing::warn!(
                    agreement_id = %format_args!("0x{}", agreement_id.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                    tx_hash = %tx_hash,
                    nonce = dropped_nonce,
                    "Manager cancel tx did not mine within receipt-poll window; treating as dropped"
                );
                if let Err(err) = self.fill_nonce_gap(dropped_nonce).await {
                    tracing::warn!(nonce = dropped_nonce, error = %err, "Failed to fill mempool nonce gap");
                }
                Err(ChainClientError::TxDropped { tx_hash })
            }
        }
    }

    async fn agreement_still_active(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<bool, ChainClientError> {
        let calldata = IRecurringCollector::getAgreementDetailsCall {
            agreementId: FixedBytes::<16>::from_slice(agreement_id),
            index: thegraph_core::alloy::primitives::U256::from(VERSION_CURRENT),
        }
        .abi_encode();

        let collector = self.inner.recurring_collector_address;
        let output = self
            .inner
            .rpc_pool
            .execute("get_agreement_details", |provider| {
                let calldata = calldata.clone();
                async move {
                    let tx = TransactionRequest::default()
                        .to(collector)
                        .input(calldata.into());
                    provider.call(tx).await
                }
            })
            .await?;

        let details = IRecurringCollector::getAgreementDetailsCall::abi_decode_returns(&output)
            .map_err(|err| {
                ChainClientError::RpcError(anyhow::anyhow!(
                    "undecodable getAgreementDetails from {collector}: {err}"
                ))
            })?;

        // Live iff the terms are accepted and no cancellation notice exists.
        // A cancel sets NOTICE_GIVEN while ACCEPTED stays set, so checking the
        // notice bit is what tells a still-live agreement from a cancelled one.
        let state = details.state;
        Ok(state & STATE_ACCEPTED != 0 && state & STATE_NOTICE_GIVEN == 0)
    }

    async fn reconcile_provider(
        &self,
        collector: Address,
        provider: Address,
    ) -> Result<Option<B256>, ChainClientError> {
        let manager = self.inner.recurring_agreement_manager_address;

        let calldata = IRecurringAgreementManager::reconcileProviderCall {
            collector,
            provider,
        }
        .abi_encode();

        tracing::info!(
            manager = %manager,
            collector = %collector,
            provider = %provider,
            "Reconciling provider escrow via RecurringAgreementManager"
        );

        // No agreement context here; pass a zero id for the shared call's
        // logging field only. The call target is the manager.
        let tx = self
            .build_and_send_call(manager, calldata, &[0u8; 16])
            .await?;
        Ok(Some(tx.hash))
    }
}

#[cfg(test)]
mod tests {
    use url::Url;
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate, matchers::method};

    use super::*;

    /// Answers a send with a fixed transaction hash, echoing the request id so alloy's
    /// transport accepts the response. Any other call is a mistake in the test rather than
    /// something to answer with a hash, so say so instead of returning nonsense.
    struct SendResponder {
        tx_hash: B256,
    }

    impl Respond for SendResponder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("JSON-RPC request body");
            let method = body["method"].as_str().unwrap_or_default();
            assert!(
                method.starts_with("eth_send"),
                "this mock only answers sends, got {method}"
            );
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": format!("{tx_hash:#x}", tx_hash = self.tx_hash),
            }))
        }
    }

    async fn server_answering_with(tx_hash: B256) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(SendResponder { tx_hash })
            .mount(&server)
            .await;
        server
    }

    async fn server_answering_500() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string(
                r#"{"id":0,"jsonrpc":"2.0","error":{"message":"Temporary internal error. Please retry","code":19}}"#,
            ))
            .mount(&server)
            .await;
        server
    }

    /// Answers HTTP 200 carrying a JSON-RPC error, which is how a chain reports a rejection
    /// such as a stale nonce and how some providers report being overloaded.
    async fn server_answering_rpc_error(code: i64, message: &str) -> MockServer {
        let server = MockServer::start().await;
        let body = serde_json::json!({
            "id": 0,
            "jsonrpc": "2.0",
            "error": { "code": code, "message": message },
        });
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    fn client_over(providers: Vec<Url>) -> AlloyChainClient {
        client_over_retrying(providers, 0)
    }

    fn client_over_retrying(providers: Vec<Url>, max_retries: u32) -> AlloyChainClient {
        let config = ChainClientConfig {
            enabled: true,
            providers,
            request_timeout: Duration::from_secs(5),
            max_retries,
            domain_refresh_interval: Duration::from_secs(3600),
            gas_price_multiplier: 1.2,
            max_gas_price_gwei: 100,
            gas_buffer_multiplier: 2.0,
            gas_floor: 100_000,
            gas_max_addition: 200_000,
        };

        AlloyChainClient::new(
            &config,
            1337,
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            &[0x42; 32],
        )
        .expect("chain client")
    }

    /// A transaction with everything filled, so no filler needs to reach the network and
    /// the bytes signed are identical whichever provider receives them.
    fn ready_to_send_tx(from: Address) -> TransactionRequest {
        TransactionRequest::default()
            .from(from)
            .to(Address::repeat_byte(0x33))
            .value(thegraph_core::alloy::primitives::U256::ZERO)
            .with_gas_limit(21_000)
            .with_max_fee_per_gas(2_000_000_000)
            .with_max_priority_fee_per_gas(1_000_000_000)
            .with_chain_id(1337)
            .with_nonce(7)
    }

    /// The hash the signed bytes carry, which is what a send reports.
    async fn signed_hash_of(client: &AlloyChainClient, tx: &TransactionRequest) -> B256 {
        let wallet = EthereumWallet::from(client.inner.signer.clone());
        *tx.clone()
            .build(&wallet)
            .await
            .expect("sign the transaction")
            .tx_hash()
    }

    /// Submitting a transaction must fail over to the next provider, exactly as every read
    /// call does. On 2026-07-29 the send bypassed the pool, so one endpoint answering 500
    /// stranded an accepted agreement while a healthy second endpoint was never tried.
    #[tokio::test]
    async fn send_transaction_rotates_to_the_next_provider_on_server_fault() {
        let sick = server_answering_500().await;
        let healthy = server_answering_with(B256::repeat_byte(0xab)).await;

        let client = client_over(vec![
            sick.uri().parse().expect("sick provider URL"),
            healthy.uri().parse().expect("healthy provider URL"),
        ]);
        let tx = ready_to_send_tx(client.inner.signer.address());

        let tx_hash = client
            .send_transaction(&tx)
            .await
            .expect("send should succeed on the second provider");

        assert_eq!(
            tx_hash,
            signed_hash_of(&client, &tx).await,
            "the hash reported must be the one we signed, not the one the endpoint made up"
        );
        assert!(
            !sick
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty(),
            "the failing provider should have been tried first"
        );
        assert!(
            !healthy
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty(),
            "the healthy provider should have been tried after rotation"
        );
    }

    /// With a single provider there is nowhere to rotate to, so the fault must surface rather
    /// than be retried forever or silently swallowed. It surfaces as a failed submission rather
    /// than the pool's generic RPC fault, which is what says the transaction went nowhere.
    #[tokio::test]
    async fn send_transaction_reports_failure_when_every_provider_is_sick() {
        let sick = server_answering_500().await;
        let client = client_over(vec![sick.uri().parse().expect("sick provider URL")]);
        let tx = ready_to_send_tx(client.inner.signer.address());

        let err = client
            .send_transaction(&tx)
            .await
            .expect_err("a 500 from the only provider must error");

        assert!(
            matches!(err, ChainClientError::SubmitFailed(_)),
            "got {err}"
        );
    }

    /// What the chain said has to survive the pool, because `sign_and_send` reads this text to
    /// recognise a stale nonce and resync from the chain. Routing the send through the pool
    /// once replaced the reason with a generic summary, which silently disabled that recovery.
    #[tokio::test]
    async fn send_transaction_surfaces_what_the_provider_said() {
        let rejecting = server_answering_rpc_error(-32000, "nonce too low: next nonce 12").await;
        let client = client_over(vec![rejecting.uri().parse().expect("provider URL")]);
        let tx = ready_to_send_tx(client.inner.signer.address());

        let err = client
            .send_transaction(&tx)
            .await
            .expect_err("a rejected transaction must error");

        let text = err.to_string();
        assert!(
            is_nonce_error(&text),
            "the nonce reason must still be recognisable, got: {text}"
        );
    }

    /// Answers the nonce lookup that precedes a send, then reports every send as already
    /// held. Counting the sends is how a duplicate submission shows up.
    struct AlreadyHeldResponder {
        pending_nonce: u64,
    }

    impl Respond for AlreadyHeldResponder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("JSON-RPC request body");
            match body["method"].as_str().unwrap_or_default() {
                "eth_getTransactionCount" => {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "result": format!("{:#x}", self.pending_nonce),
                    }))
                }
                "eth_sendRawTransaction" => {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "error": { "code": -32000, "message": "already known" },
                    }))
                }
                other => panic!("unexpected method {other}"),
            }
        }
    }

    fn sends_among(requests: &[wiremock::Request]) -> usize {
        requests
            .iter()
            .filter(|r| {
                let body: serde_json::Value =
                    serde_json::from_slice(&r.body).expect("JSON-RPC request body");
                body["method"] == "eth_sendRawTransaction"
            })
            .count()
    }

    /// The raw transaction each endpoint was asked to accept.
    fn broadcast_bytes(requests: &[wiremock::Request]) -> Vec<String> {
        requests
            .iter()
            .filter_map(|r| {
                let body: serde_json::Value =
                    serde_json::from_slice(&r.body).expect("JSON-RPC request body");
                (body["method"] == "eth_sendRawTransaction")
                    .then(|| body["params"][0].as_str().expect("raw tx").to_string())
            })
            .collect()
    }

    /// Rotating between endpoints is only safe while they are all offered the same bytes: two
    /// different transactions would mean two chances of both being mined and paid for. This
    /// is what makes a rebroadcast a retry of one transaction rather than a second one.
    #[tokio::test]
    async fn every_endpoint_is_offered_the_same_bytes() {
        let sick = server_answering_500().await;
        let healthy = server_answering_with(B256::repeat_byte(0xab)).await;
        let client = client_over(vec![
            sick.uri().parse().expect("sick provider URL"),
            healthy.uri().parse().expect("healthy provider URL"),
        ]);
        let tx = ready_to_send_tx(client.inner.signer.address());

        client.send_transaction(&tx).await.expect("send");

        let offered = [
            broadcast_bytes(&sick.received_requests().await.unwrap_or_default()),
            broadcast_bytes(&healthy.received_requests().await.unwrap_or_default()),
        ]
        .concat();
        assert_eq!(offered.len(), 2, "both endpoints should have been asked");
        assert_eq!(
            offered[0], offered[1],
            "the two endpoints were offered different transactions"
        );
    }

    /// Recovering from a stale nonce has to land on the slot the chain will actually take.
    /// Aiming one past it leaves that slot empty, and a transaction behind an empty slot never
    /// gets mined, so a wallet could stay stuck until something else happened to fill it.
    #[tokio::test]
    async fn resync_lands_on_the_next_slot_the_chain_will_accept() {
        let pending = Arc::new(AtomicU64::new(3));
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(PendingNonceResponder {
                pending: pending.clone(),
            })
            .mount(&server)
            .await;
        let client = client_over(vec![server.uri().parse().expect("provider URL")]);

        assert_eq!(
            client.next_nonce().await.expect("first reservation"),
            3,
            "the first reservation takes the slot the chain reports"
        );

        // Something else spent slots 3 to 9, so 10 is now the next one free.
        pending.store(10, Ordering::SeqCst);
        client.resync_nonce().await.expect("resync");

        assert_eq!(
            client.next_nonce().await.expect("reservation after resync"),
            10,
            "the reservation after a resync must not skip the free slot"
        );
    }

    /// Reports whatever the pending count currently is, so a test can move the chain on.
    struct PendingNonceResponder {
        pending: Arc<AtomicU64>,
    }

    impl Respond for PendingNonceResponder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("JSON-RPC request body");
            assert_eq!(
                body["method"], "eth_getTransactionCount",
                "this mock only answers nonce lookups"
            );
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": body["id"],
                "result": format!("{:#x}", self.pending.load(Ordering::SeqCst)),
            }))
        }
    }

    /// The first endpoint takes the bytes but its reply is lost, so the same bytes go to the
    /// second, which already holds them. That is the outcome we wanted, so it has to be
    /// reported with the hash rather than as a failure.
    #[tokio::test]
    async fn send_transaction_accepts_a_transaction_already_in_a_mempool() {
        let sick = server_answering_500().await;
        let holding = server_answering_rpc_error(-32000, "already known").await;
        let client = client_over(vec![
            sick.uri().parse().expect("sick provider URL"),
            holding.uri().parse().expect("holding provider URL"),
        ]);
        let tx = ready_to_send_tx(client.inner.signer.address());

        let tx_hash = client
            .send_transaction(&tx)
            .await
            .expect("a transaction already in a mempool is a successful broadcast");

        assert_eq!(
            tx_hash,
            signed_hash_of(&client, &tx).await,
            "the hash reported must be the one we signed"
        );
    }

    /// Treating "we already have it" as a stale nonce made the caller resync and send a
    /// second transaction at a different nonce, which for an agreement offer means paying
    /// twice for one offer. One broadcast has to stay one broadcast.
    #[tokio::test]
    async fn sign_and_send_does_not_resend_a_transaction_already_in_a_mempool() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(AlreadyHeldResponder { pending_nonce: 6 })
            .mount(&server)
            .await;
        let client = client_over(vec![server.uri().parse().expect("provider URL")]);

        client
            .sign_and_send(ready_to_send_tx(client.inner.signer.address()), &[0u8; 16])
            .await
            .expect("a transaction already in a mempool is a successful broadcast");

        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            sends_among(&requests),
            1,
            "the transaction must be broadcast once, not resent at a new nonce"
        );
    }

    /// Nothing fills a field in on the send path, and a request naming no chain is signed for
    /// chain 1 rather than refused, so a transaction has to say which chain it is for. It has
    /// to read as a failed submission: a config fault means "chain client off" to the caller.
    #[tokio::test]
    async fn send_transaction_refuses_a_transaction_that_names_another_chain() {
        let server = server_answering_with(B256::repeat_byte(0xab)).await;
        let client = client_over(vec![server.uri().parse().expect("provider URL")]);
        let from = client.inner.signer.address();

        let mut names_no_chain = ready_to_send_tx(from);
        names_no_chain.chain_id = None;

        for tx in [ready_to_send_tx(from).with_chain_id(1), names_no_chain] {
            let err = client
                .send_transaction(&tx)
                .await
                .expect_err("a transaction for another chain must not be signed");
            assert!(
                matches!(err, ChainClientError::SubmitFailed(_)),
                "got {err}"
            );
        }

        assert!(
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty(),
            "no endpoint should have been asked to accept either transaction"
        );
    }

    /// A field a caller leaves out is no longer filled in for them, so the send stops rather
    /// than putting an incomplete transaction on the wire. What it says has to name the field,
    /// because a caller reading only "signing failed" would go looking at the wrong thing.
    #[tokio::test]
    async fn send_transaction_refuses_a_transaction_missing_a_field() {
        let server = server_answering_with(B256::repeat_byte(0xab)).await;
        let client = client_over(vec![server.uri().parse().expect("provider URL")]);

        let mut no_gas_limit = ready_to_send_tx(client.inner.signer.address());
        no_gas_limit.gas = None;

        let err = client
            .send_transaction(&no_gas_limit)
            .await
            .expect_err("a transaction with no gas limit must not be sent");

        let text = err.to_string();
        assert!(
            text.contains("gas_limit"),
            "the failure should name the missing field, got: {text}"
        );
        assert!(
            server
                .received_requests()
                .await
                .unwrap_or_default()
                .is_empty(),
            "no endpoint should have been asked to accept it"
        );
    }

    /// The nonce reason has to survive a second provider failing a different way. Reporting
    /// only the last provider's reason hid the rejection behind a transport fault, and
    /// `sign_and_send` then skipped the resync that would have unstuck the wallet.
    #[tokio::test]
    async fn send_transaction_surfaces_a_nonce_reason_from_any_provider() {
        let rejecting = server_answering_rpc_error(-32000, "nonce too low: next nonce 12").await;
        let sick = server_answering_500().await;
        let client = client_over(vec![
            rejecting.uri().parse().expect("rejecting provider URL"),
            sick.uri().parse().expect("sick provider URL"),
        ]);
        let tx = ready_to_send_tx(client.inner.signer.address());

        let err = client
            .send_transaction(&tx)
            .await
            .expect_err("both providers refused, so the send must error");

        let text = err.to_string();
        assert!(
            is_nonce_error(&text),
            "the nonce reason must survive the later transport fault, got: {text}"
        );
    }

    /// An endpoint saying it is overloaded is asked again before the submission gives up on
    /// it, because that complaint often clears on its own and rotating away costs a provider.
    /// One retry rather than the usual several, to keep the backoff this waits out short.
    #[tokio::test]
    async fn send_transaction_retries_a_struggling_endpoint_before_rotating() {
        let overloaded =
            server_answering_rpc_error(-32005, "project ID request rate exceeded").await;
        let healthy = server_answering_with(B256::repeat_byte(0xcd)).await;
        let client = client_over_retrying(
            vec![
                overloaded.uri().parse().expect("overloaded provider URL"),
                healthy.uri().parse().expect("healthy provider URL"),
            ],
            1,
        );
        let tx = ready_to_send_tx(client.inner.signer.address());

        client
            .send_transaction(&tx)
            .await
            .expect("send should succeed on the healthy provider");

        assert_eq!(
            overloaded
                .received_requests()
                .await
                .unwrap_or_default()
                .len(),
            2,
            "the struggling endpoint should get its retry before the rotation"
        );
        assert_eq!(
            healthy.received_requests().await.unwrap_or_default().len(),
            1,
            "the healthy endpoint should answer on the first ask"
        );
    }

    /// Every other submission queues behind the one holding the lock, so a submission that
    /// never finishes has to be cut off rather than waited out. Paused time so the deadline
    /// is reached without the test spending it.
    #[tokio::test(start_paused = true)]
    async fn a_submission_that_never_finishes_is_given_up_on() {
        let err = under_submit_deadline(Duration::from_secs(60), std::future::pending())
            .await
            .expect_err("a submission that never finishes must not be waited out");

        assert!(
            matches!(err, ChainClientError::SubmitFailed(_)),
            "got {err}"
        );
        assert!(
            err.to_string().contains("60"),
            "the failure should say how long it waited, got: {err}"
        );
    }

    /// A submission that finishes inside the deadline is left alone, so the cap only ever
    /// catches the case it is there for.
    #[tokio::test(start_paused = true)]
    async fn a_submission_that_finishes_in_time_is_left_alone() {
        let submitted = under_submit_deadline(Duration::from_secs(60), async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(SubmittedTx {
                hash: B256::repeat_byte(0x77),
                nonce: 3,
            })
        })
        .await
        .expect("a submission inside the deadline should stand");

        assert_eq!(submitted.hash, B256::repeat_byte(0x77));
    }

    /// The deadline exists to stop one submission starving the queue, not to cut off retries
    /// the config asks for, so it is derived from the schedule: a submission walks the whole
    /// ring twice, once reading the chain's nonce and once broadcasting.
    #[test]
    fn the_submit_deadline_covers_the_retry_schedule() {
        let client = client_over_retrying(
            vec![
                "http://one.invalid".parse().expect("first URL"),
                "http://two.invalid".parse().expect("second URL"),
            ],
            1,
        );

        // Per endpoint: 2 attempts of 5s plus 1s of backoff; 2 endpoints make one walk of
        // 22s; one walk to read the nonce and one to send.
        assert_eq!(client.inner.submit_deadline, Duration::from_secs(44));
    }

    /// The shape a real deployment has, 3 providers at the config defaults (10s timeout,
    /// 3 retries), must fit under the cap with its whole schedule intact, otherwise every
    /// production start would log the warning and lose retries the config asked for.
    #[test]
    fn three_providers_at_the_defaults_fit_inside_a_worker_job() {
        let providers = (0..3)
            .map(|i| {
                format!("http://rpc{i}.invalid")
                    .parse()
                    .expect("provider URL")
            })
            .collect();
        let config = ChainClientConfig {
            enabled: true,
            providers,
            request_timeout: crate::config::default_chain_client_request_timeout(),
            max_retries: crate::config::default_chain_client_max_retries(),
            domain_refresh_interval: Duration::from_secs(3600),
            gas_price_multiplier: 1.2,
            max_gas_price_gwei: 100,
            gas_buffer_multiplier: 2.0,
            gas_floor: 100_000,
            gas_max_addition: 200_000,
        };
        let client = AlloyChainClient::new(
            &config,
            1337,
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            &[0x42; 32],
        )
        .expect("chain client");

        // Per endpoint: 4 attempts of 10s plus 1+2+4s of backoff; 3 endpoints make one walk
        // of 141s; two walks come to 282s, inside the 336s the job leaves for a submission.
        let two_walks = Duration::from_secs(282);
        assert_eq!(client.inner.submit_deadline, two_walks);
        assert!(two_walks < PROCESS_JOB_TIMEOUT / 5 * 4);
    }

    /// A schedule that wants more time than a worker job has is capped rather than obeyed,
    /// leaving room after the broadcast for the receipt poll and the nonce-gap fill.
    #[test]
    fn the_submit_deadline_stays_inside_a_worker_job() {
        let providers = (0..20)
            .map(|i| {
                format!("http://rpc{i}.invalid")
                    .parse()
                    .expect("provider URL")
            })
            .collect();
        let client = client_over_retrying(providers, 3);

        assert_eq!(client.inner.submit_deadline, PROCESS_JOB_TIMEOUT / 5 * 4);
    }

    /// Offering the same bytes twice is what makes a retry safe: an endpoint that took them
    /// and then failed to say so recognises them the second time, and reports the broadcast
    /// that already happened rather than accepting a second transaction.
    #[tokio::test]
    async fn a_retry_that_lands_on_bytes_already_held_is_a_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(AlreadyHeldResponder { pending_nonce: 6 })
            .mount(&server)
            .await;
        let client = client_over_retrying(vec![server.uri().parse().expect("provider URL")], 1);
        let tx = ready_to_send_tx(client.inner.signer.address());

        let tx_hash = client
            .send_transaction(&tx)
            .await
            .expect("bytes already held are a successful broadcast");

        assert_eq!(tx_hash, signed_hash_of(&client, &tx).await);
        assert_eq!(
            sends_among(&server.received_requests().await.unwrap_or_default()),
            1,
            "an accepted broadcast should not be offered again"
        );
    }

    /// A chain rejection is the chain's answer, not one endpoint's, so asking the same
    /// endpoint again would only collect the same refusal at the cost of the delay.
    #[tokio::test]
    async fn send_transaction_does_not_repeat_a_chain_rejection() {
        let rejecting = server_answering_rpc_error(-32000, "nonce too low: next nonce 12").await;
        let spare = server_answering_with(B256::repeat_byte(0xef)).await;
        let client = client_over_retrying(
            vec![
                rejecting.uri().parse().expect("rejecting provider URL"),
                spare.uri().parse().expect("spare provider URL"),
            ],
            3,
        );
        let tx = ready_to_send_tx(client.inner.signer.address());

        client
            .send_transaction(&tx)
            .await
            .expect("the spare endpoint accepts, so the send succeeds");

        assert_eq!(
            rejecting
                .received_requests()
                .await
                .unwrap_or_default()
                .len(),
            1,
            "a chain rejection should not be retried against the same endpoint"
        );
    }

    #[test]
    fn nonce_rejections_are_told_apart_from_other_refusals() {
        // Nonce errors
        assert!(is_nonce_error("nonce too low"));
        assert!(is_nonce_error("Nonce Too Low for account"));
        assert!(is_nonce_error("invalid nonce: expected 5, got 3"));
        assert!(is_nonce_error("replacement transaction underpriced"));
        // A backstop, not a live path: a send reports this as the successful broadcast it is.
        assert!(is_nonce_error("transaction already known"));

        // Non-nonce errors
        assert!(!is_nonce_error("insufficient funds"));
        assert!(!is_nonce_error("execution reverted"));
        assert!(!is_nonce_error("gas limit exceeded"));
        assert!(!is_nonce_error("connection timeout"));
    }

    #[test]
    fn a_nonce_gap_fill_that_is_accepted_is_a_success() {
        let result = classify_fill_nonce_gap_outcome(42, Ok(B256::ZERO));
        assert!(result.is_ok());
    }

    #[test]
    fn a_nonce_gap_fill_refused_on_the_nonce_is_still_a_success() {
        // Each of these strings flips `is_nonce_error` to true; the gap fill must treat
        // them as success because they all mean the slot is already spoken for.
        for msg in [
            "nonce too low",
            "replacement transaction underpriced",
            "already known",
            "invalid nonce",
        ] {
            let err = ChainClientError::SubmitFailed(anyhow::anyhow!("{msg}"));
            let result = classify_fill_nonce_gap_outcome(99, Err(err));
            assert!(
                result.is_ok(),
                "fill_nonce_gap must swallow {msg:?} so a still-live original tx is not treated as a hard failure"
            );
        }
    }

    #[test]
    fn a_nonce_gap_fill_that_fails_for_another_reason_is_reported() {
        // Errors that don't match `is_nonce_error` mean the noop tx itself
        // failed for a real reason (RPC down, gas estimation broken, etc.),
        // so the wallet may stay wedged. Surface to the caller.
        let err = ChainClientError::SubmitFailed(anyhow::anyhow!("connection timeout"));
        let result = classify_fill_nonce_gap_outcome(99, Err(err));
        assert!(
            result.is_err(),
            "non-nonce errors must propagate so the wedged-wallet path is observable"
        );
    }

    /// Answers nonce lookups with a fixed pending count, refuses the first send with a
    /// server fault, and accepts every send after it: the shape of a submission failing
    /// outright and the job being re-run.
    struct FailsFirstSendResponder {
        pending_nonce: u64,
        sends: AtomicU64,
    }

    impl Respond for FailsFirstSendResponder {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("JSON-RPC request body");
            match body["method"].as_str().unwrap_or_default() {
                "eth_getTransactionCount" => {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "result": format!("{:#x}", self.pending_nonce),
                    }))
                }
                "eth_sendRawTransaction" if self.sends.fetch_add(1, Ordering::SeqCst) == 0 => {
                    ResponseTemplate::new(500).set_body_string("Temporary internal error")
                }
                "eth_sendRawTransaction" => {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "result": format!("{:#x}", B256::repeat_byte(0xaa)),
                    }))
                }
                other => panic!("unexpected method {other}"),
            }
        }
    }

    /// A submission that never got a transaction out must leave its nonce for the next one.
    /// Spending it anyway left a slot the chain kept waiting on: every later transaction
    /// queued behind the empty slot, and nothing filled it short of a restart.
    #[tokio::test]
    async fn a_failed_submission_leaves_its_nonce_to_the_next() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(FailsFirstSendResponder {
                pending_nonce: 5,
                sends: AtomicU64::new(0),
            })
            .mount(&server)
            .await;
        let client = client_over(vec![server.uri().parse().expect("provider URL")]);
        let tx = ready_to_send_tx(client.inner.signer.address());

        client
            .sign_and_send(tx.clone(), &[0u8; 16])
            .await
            .expect_err("the only endpoint refused, so the submission fails");

        let retry = client
            .sign_and_send(tx.clone(), &[0u8; 16])
            .await
            .expect("the endpoint accepts the re-run");
        assert_eq!(
            retry.nonce, 5,
            "the re-run must take the slot the failure never spent"
        );

        let next = client
            .sign_and_send(tx, &[0u8; 16])
            .await
            .expect("a further submission succeeds");
        assert_eq!(next.nonce, 6, "a successful broadcast spends its slot");
    }
}
