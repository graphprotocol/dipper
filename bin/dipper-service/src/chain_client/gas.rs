//! Gas estimation with safety bounds.
//!
//! Ported from `rewards-eligibility-oracle/blockchain_client.py`.

use thegraph_core::alloy::{
    eips::BlockNumberOrTag,
    providers::Provider,
    rpc::types::TransactionRequest,
    transports::{RpcError, TransportErrorKind},
};

use super::rpc_provider::HttpProvider;
use crate::chain_client::ChainClientError;

/// Convert an alloy transport error from `eth_estimateGas` into the dipper
/// chain-client error type, preserving structured revert data when present.
///
/// `eth_estimateGas` simulates the call inside the EVM. When the call reverts,
/// the node returns the revert payload in the JSON-RPC error's `data` field.
/// If we can extract that payload and it carries a 4-byte selector, surface it
/// as `ContractRevert` so callers can decode the specific error and decide
/// whether to swallow it (e.g. `IndexingAgreementNotActive` on a cancel retry
/// is an idempotent success). Otherwise fall back to the generic `RpcError`.
fn classify_estimate_error(err: RpcError<TransportErrorKind>) -> ChainClientError {
    if let RpcError::ErrorResp(ref payload) = err
        && let Some(data) = payload.as_revert_data()
        && data.len() >= 4
    {
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&data[..4]);
        return ChainClientError::ContractRevert { selector, data };
    }
    ChainClientError::RpcError(anyhow::anyhow!("Gas estimation failed: {err}"))
}

/// Gas estimator with configurable safety bounds.
///
/// Applies a buffer to the estimated gas, bounded by a floor (minimum)
/// and ceiling (maximum addition above estimate).
#[derive(Debug, Clone)]
pub struct GasEstimator {
    /// Multiplier applied to estimated gas (e.g., 2.0 = 100% buffer)
    buffer_multiplier: f64,
    /// Minimum gas limit (floor)
    floor: u64,
    /// Maximum addition above estimate (ceiling = estimate + max_addition)
    max_addition: u64,
}

/// Outcome of `compute_bounds`: the bounded gas limit plus the intermediate
/// values and which bound produced it, for transparent gas logging.
struct GasBounds {
    gas_limit: u64,
    with_buffer: u64,
    ceiling: u64,
    applied_bound: &'static str,
}

impl GasEstimator {
    /// Create a new gas estimator with the specified bounds.
    pub fn new(buffer_multiplier: f64, floor: u64, max_addition: u64) -> Self {
        Self {
            buffer_multiplier,
            floor,
            max_addition,
        }
    }

    /// Estimate gas for a transaction and apply safety bounds.
    ///
    /// The final gas limit is calculated as:
    /// ```text
    /// with_buffer = estimate * buffer_multiplier
    /// ceiling = estimate + max_addition
    /// gas_limit = max(floor, min(with_buffer, ceiling))
    /// ```
    ///
    /// This ensures:
    /// - Gas limit is never below the floor
    /// - Buffer doesn't grow unbounded for expensive transactions
    /// - Small transactions get adequate buffer
    pub async fn estimate(
        &self,
        provider: &HttpProvider,
        tx: &TransactionRequest,
    ) -> Result<u64, ChainClientError> {
        let estimated = provider
            .estimate_gas(tx.clone())
            .await
            .map_err(classify_estimate_error)?;

        let bounds = self.compute_bounds(estimated);

        tracing::debug!(
            estimated,
            buffer_multiplier = self.buffer_multiplier,
            floor = self.floor,
            max_addition = self.max_addition,
            with_buffer = bounds.with_buffer,
            ceiling = bounds.ceiling,
            applied_bound = bounds.applied_bound,
            gas_limit = bounds.gas_limit,
            "Gas estimation with bounds"
        );

        Ok(bounds.gas_limit)
    }

    /// Apply the bounds and report which one set the final limit, so the gas
    /// log can show whether the buffer, the ceiling, or the floor won instead
    /// of implying `buffer_multiplier` always drives the result.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "predates this lint; fix when next touched"
    )]
    fn compute_bounds(&self, estimated: u64) -> GasBounds {
        let with_buffer = (estimated as f64 * self.buffer_multiplier) as u64;
        let ceiling = estimated.saturating_add(self.max_addition);

        // gas_limit = floor.max(min(with_buffer, ceiling)); track the winner.
        let (capped, capped_bound) = if ceiling < with_buffer {
            (ceiling, "ceiling")
        } else {
            (with_buffer, "buffer")
        };
        let (gas_limit, applied_bound) = if self.floor > capped {
            (self.floor, "floor")
        } else {
            (capped, capped_bound)
        };

        GasBounds {
            gas_limit,
            with_buffer,
            ceiling,
            applied_bound,
        }
    }
}

impl Default for GasEstimator {
    fn default() -> Self {
        Self {
            buffer_multiplier: 2.0,
            floor: 100_000,
            max_addition: 200_000,
        }
    }
}

/// Get gas prices from the provider.
///
/// Returns (base_fee, priority_fee) for EIP-1559 transactions.
pub async fn get_gas_prices(provider: &HttpProvider) -> Result<(u128, u128), ChainClientError> {
    // Get the latest block for base fee
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Latest)
        .await
        .map_err(|e| ChainClientError::RpcError(anyhow::anyhow!("Failed to get block: {e}")))?
        .ok_or_else(|| ChainClientError::RpcError(anyhow::anyhow!("No latest block found")))?;

    let base_fee = block.header.base_fee_per_gas.ok_or_else(|| {
        ChainClientError::RpcError(anyhow::anyhow!("Block has no base fee (pre-EIP-1559?)"))
    })?;

    // Get priority fee estimate
    let priority_fee = provider.get_max_priority_fee_per_gas().await.map_err(|e| {
        ChainClientError::RpcError(anyhow::anyhow!("Failed to get priority fee: {e}"))
    })?;

    Ok((base_fee as u128, priority_fee))
}

/// Calculate max fee per gas with a multiplier.
///
/// Formula: (base_fee * multiplier) + priority_fee
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "predates this lint; fix when next touched"
)]
pub fn calculate_max_fee(base_fee: u128, priority_fee: u128, multiplier: f64) -> u128 {
    let adjusted_base = (base_fee as f64 * multiplier) as u128;
    adjusted_base.saturating_add(priority_fee)
}

/// Check if a gas price exceeds the maximum allowed.
pub fn exceeds_max_gas_price(max_fee_per_gas: u128, max_gwei: u64) -> bool {
    let max_wei = (max_gwei as u128) * 1_000_000_000;
    max_fee_per_gas > max_wei
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gas_estimation_applies_buffer() {
        let estimator = GasEstimator::new(2.0, 100_000, 200_000);

        // Normal case: buffer applied
        // estimated = 50,000, with_buffer = 100,000, ceiling = 250,000
        // result = max(100_000, min(100_000, 250_000)) = 100,000
        let bounds = estimator.compute_bounds(50_000);
        assert_eq!(bounds.gas_limit, 100_000);
        assert_eq!(bounds.applied_bound, "buffer");
    }

    #[test]
    fn test_gas_floor_enforced() {
        let estimator = GasEstimator::new(2.0, 100_000, 200_000);

        // Small estimate should use floor
        // estimated = 10,000, with_buffer = 20,000, ceiling = 210,000
        // result = max(100_000, min(20_000, 210_000)) = 100,000
        let bounds = estimator.compute_bounds(10_000);
        assert_eq!(bounds.gas_limit, 100_000);
        assert_eq!(bounds.applied_bound, "floor");
    }

    #[test]
    fn test_gas_ceiling_enforced() {
        let estimator = GasEstimator::new(2.0, 100_000, 200_000);

        // Large estimate should be capped
        // estimated = 500,000, with_buffer = 1,000,000, ceiling = 700,000
        // result = max(100_000, min(1_000_000, 700_000)) = 700,000
        let bounds = estimator.compute_bounds(500_000);
        assert_eq!(bounds.gas_limit, 700_000);
        assert_eq!(bounds.applied_bound, "ceiling");
    }

    #[test]
    fn test_gas_buffer_within_ceiling() {
        let estimator = GasEstimator::new(1.5, 100_000, 200_000);

        // Buffer fits within ceiling
        // estimated = 200,000, with_buffer = 300,000, ceiling = 400,000
        // result = max(100_000, min(300_000, 400_000)) = 300,000
        let bounds = estimator.compute_bounds(200_000);
        assert_eq!(bounds.gas_limit, 300_000);
        assert_eq!(bounds.applied_bound, "buffer");
    }

    #[test]
    fn test_calculate_max_fee() {
        let base_fee: u128 = 1_000_000_000; // 1 gwei
        let priority_fee: u128 = 100_000_000; // 0.1 gwei
        let multiplier = 1.2;

        // (1 gwei * 1.2) + 0.1 gwei = 1.3 gwei
        let max_fee = calculate_max_fee(base_fee, priority_fee, multiplier);
        assert_eq!(max_fee, 1_300_000_000);
    }

    #[test]
    fn test_exceeds_max_gas_price() {
        let max_gwei = 100;

        // 99 gwei should not exceed
        assert!(!exceeds_max_gas_price(99_000_000_000, max_gwei));

        // 100 gwei should not exceed
        assert!(!exceeds_max_gas_price(100_000_000_000, max_gwei));

        // 101 gwei should exceed
        assert!(exceeds_max_gas_price(101_000_000_000, max_gwei));
    }
}
