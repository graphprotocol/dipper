//! Chain client for sending on-chain transactions
//!
//! This module provides the interface for interacting with the blockchain
//! to manage indexing agreements on-chain.
//!
//! ## Implementation
//!
//! The production implementation uses alloy for Ethereum interactions and
//! includes:
//! - RPC provider pool with automatic failover
//! - Exponential backoff retry logic
//! - Gas estimation with safety bounds
//! - Nonce management and error handling
//!
//! See [`AlloyChainClient`] for the production implementation.

mod abi;
mod client;
mod eip5267;
mod gas;
mod manager_role;
mod rpc_provider;

use std::sync::Arc;

pub(crate) use abi::decode_revert_reason;
use async_trait::async_trait;
pub use client::AlloyChainClient;
use dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement;
pub use eip5267::{fetch_rca_eip712_domain, refresh_rca_eip712_domain};
pub use manager_role::verify_signer_has_agreement_manager_role;
use thegraph_core::alloy::primitives::{B256, Bytes};

/// Error type for chain client operations
#[derive(Debug, thiserror::Error)]
pub enum ChainClientError {
    /// Transaction failed to submit
    #[error("failed to submit transaction: {0}")]
    SubmitFailed(#[source] anyhow::Error),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigError(String),

    /// The agreement has no 32-byte `terms_version_hash`, so it cannot be
    /// canceled via the RecurringAgreementManager. Permanent and per-agreement
    /// — distinct from a globally-disabled chain client; never retry or abandon.
    #[error("agreement {agreement_id} has no 32-byte terms_version_hash for manager cancel")]
    MissingTermsVersionHash { agreement_id: String },

    /// A manager-routed cancel tx mined, but a follow-up read shows the
    /// agreement is still active on-chain (stale/wrong hash, unknown id, or
    /// already-terminal made the cancel a silent no-op). Callers retry.
    #[error("manager cancel for agreement {agreement_id} mined but the agreement is still active")]
    CancelNotConfirmed { agreement_id: String },

    /// RPC error
    #[error("RPC error: {0}")]
    RpcError(#[source] anyhow::Error),

    /// Tx was accepted by the RPC (returned a hash) but no receipt appeared
    /// within the poll window.
    ///
    /// In practice the tx was evicted from the mempool — typically a same-sender
    /// tx claimed the nonce with a higher fee. Callers re-sync the nonce and
    /// resubmit; there is no idempotency guard, so a replay re-sends the offer.
    #[error("tx {tx_hash} did not mine within the receipt-poll window")]
    TxDropped { tx_hash: B256 },

    /// Tx was mined but reverted on-chain (receipt status = 0).
    #[error("tx {tx_hash} reverted on-chain")]
    TxReverted { tx_hash: B256 },

    /// A contract call reverted with structured revert data during gas
    /// estimation (eth_estimateGas simulated the call and the EVM reverted
    /// before tx submission). The 4-byte selector and full revert payload
    /// are preserved so callers can decode the specific error variant and
    /// decide whether to treat it as fatal or as a known idempotent no-op.
    #[error("contract reverted with selector 0x{:02x}{:02x}{:02x}{:02x}", selector[0], selector[1], selector[2], selector[3])]
    ContractRevert { selector: [u8; 4], data: Bytes },
}

/// Trait for sending on-chain transactions related to indexing agreements
#[async_trait]
pub trait ChainClient {
    /// Offer an RCA via `RecurringAgreementManager.offerAgreement` (manager as
    /// payer); returns the tx hash once mined. No crash-recovery idempotency,
    /// so a re-run re-sends — pending validation of the contract's re-offer path.
    async fn offer_via_manager(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError>;

    /// Cancel an RCA via `RecurringAgreementManager.cancelAgreement`. Returns
    /// the tx hash when a transaction was submitted, or `Ok(None)` when the
    /// agreement is already canceled on-chain.
    async fn cancel_via_manager(
        &self,
        collector: thegraph_core::alloy::primitives::Address,
        agreement_id: &[u8; 16],
        version_hash: B256,
        options: u16,
    ) -> Result<Option<B256>, ChainClientError>;

    /// Reconcile a provider's escrow via the RecurringAgreementManager
    /// (`AgreementManager` mode) by calling `reconcileProvider(collector,
    /// provider)`. Permissionless and idempotent; `Ok(Some(tx_hash))` on submit.
    async fn reconcile_provider(
        &self,
        collector: thegraph_core::alloy::primitives::Address,
        provider: thegraph_core::alloy::primitives::Address,
    ) -> Result<Option<B256>, ChainClientError>;

    /// Read whether the agreement is still live on-chain (terms accepted and no
    /// cancellation notice given) via the RecurringCollector's
    /// `getAgreementDetails(id, VERSION_CURRENT)`.
    async fn agreement_still_active(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<bool, ChainClientError>;

    /// Read the authoritative `versionHash` the RecurringCollector stored for
    /// this agreement via `getAgreementDetails(id, VERSION_CURRENT)`. Returns
    /// `None` if the contract has no hash on record (a zero `versionHash` —
    /// the agreement was never offered, or the id is unknown), `Some(hash)`
    /// otherwise. Used to recover a `terms_version_hash` missing locally
    /// (e.g. a row from before the column existed) instead of leaving the
    /// agreement permanently uncancelable.
    async fn fetch_agreement_version_hash(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<Option<B256>, ChainClientError>;

    /// Read the latest block's unix timestamp from the chain. Lets agreement
    /// deadlines be stamped from live chain time when the chain-clock bypass is
    /// on, instead of a cached listener timestamp that can lag a fast chain.
    async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError>;
}

/// Blanket impl for Arc-wrapped trait objects.
///
/// This allows using `Arc<dyn ChainClient + Send + Sync>` as a Clone-able
/// chain client, enabling runtime selection between implementations.
#[async_trait]
impl<T: ChainClient + Send + Sync + ?Sized> ChainClient for Arc<T> {
    async fn offer_via_manager(
        &self,
        rca: &RecurringCollectionAgreement,
    ) -> Result<Option<B256>, ChainClientError> {
        (**self).offer_via_manager(rca).await
    }

    async fn cancel_via_manager(
        &self,
        collector: thegraph_core::alloy::primitives::Address,
        agreement_id: &[u8; 16],
        version_hash: B256,
        options: u16,
    ) -> Result<Option<B256>, ChainClientError> {
        (**self)
            .cancel_via_manager(collector, agreement_id, version_hash, options)
            .await
    }

    async fn reconcile_provider(
        &self,
        collector: thegraph_core::alloy::primitives::Address,
        provider: thegraph_core::alloy::primitives::Address,
    ) -> Result<Option<B256>, ChainClientError> {
        (**self).reconcile_provider(collector, provider).await
    }

    async fn agreement_still_active(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<bool, ChainClientError> {
        (**self).agreement_still_active(agreement_id).await
    }

    async fn fetch_agreement_version_hash(
        &self,
        agreement_id: &[u8; 16],
    ) -> Result<Option<B256>, ChainClientError> {
        (**self).fetch_agreement_version_hash(agreement_id).await
    }

    async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
        (**self).latest_block_timestamp().await
    }
}
