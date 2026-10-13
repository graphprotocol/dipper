//! Shared selection helpers for indexing-request handlers.

use std::collections::BTreeMap;

use dipper_iisa::SelectedIndexer;
use graph_networks_registry::NetworksRegistry;
use thegraph_core::alloy::primitives::{ChainId, U256};

use crate::config::IndexingAgreementChainPrices;

/// Seconds in 30 days (30 * 24 * 60 * 60).
const SECONDS_PER_30_DAYS: u128 = 2_592_000;

/// 1 GRT = 10^18 wei.
const WEI_PER_GRT: u128 = 1_000_000_000_000_000_000;

/// Convert GRT per 30 days to wei per second (ceiling division to protect indexers).
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "predates this lint; fix when next touched"
)]
pub(crate) fn grt_per_30_days_to_wei_per_second(grt: f64) -> U256 {
    // Convert to integer wei, then divide by seconds using ceiling division.
    // The ceiling protects indexers from rounding losses.
    let total_wei = (grt * WEI_PER_GRT as f64) as u128;
    let wei_per_second = total_wei.div_ceil(SECONDS_PER_30_DAYS);
    U256::from(wei_per_second)
}

/// Convert GRT per billion entities per 30 days to wei per entity per second.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "predates this lint; fix when next touched"
)]
fn grt_per_billion_entities_per_30_days_to_wei_per_entity_per_second(grt: f64) -> U256 {
    // 1 billion entities = 1_000_000_000
    let total_wei = (grt * WEI_PER_GRT as f64 / 1_000_000_000.0) as u128;
    let wei_per_second = total_wei.div_ceil(SECONDS_PER_30_DAYS);
    U256::from(wei_per_second)
}

/// Resolve pricing for a selected indexer.
///
/// Uses per-indexer pricing from IISA when available, otherwise falls back to
/// the static pricing_table config. Returns `None` if neither source has pricing.
pub(crate) fn resolve_pricing(
    selected: &SelectedIndexer,
    fallback_prices: Option<&IndexingAgreementChainPrices>,
    chain_id: &ChainId,
) -> Option<(U256, U256)> {
    // Prefer IISA-reported per-indexer prices
    if let Some(grt_per_30d) = selected.min_grt_per_30_days {
        let tokens_per_second = grt_per_30_days_to_wei_per_second(grt_per_30d);
        let tokens_per_entity_per_second = selected
            .min_grt_per_billion_entities_per_30_days
            .map(grt_per_billion_entities_per_30_days_to_wei_per_entity_per_second)
            .unwrap_or(U256::ZERO);
        return Some((tokens_per_second, tokens_per_entity_per_second));
    }

    // Fall back to static pricing_table
    if let Some(prices) = fallback_prices {
        tracing::warn!(
            indexer_id=%selected.id,
            chain_id=%chain_id,
            tokens_per_second=%prices.tokens_per_second,
            tokens_per_entity_per_second=%prices.tokens_per_entity_per_second,
            "IISA returned no per-indexer pricing, falling back to static pricing_table"
        );
        return Some((
            prices.tokens_per_second,
            prices.tokens_per_entity_per_second,
        ));
    }

    tracing::warn!(
        indexer_id=%selected.id,
        chain_id=%chain_id,
        "No pricing from IISA and no fallback in pricing_table"
    );
    None
}

/// Resolve a numeric chain ID to the canonical network name used by The Graph ecosystem.
///
/// Looks up the chain in the official graph-networks-registry first (using CAIP-2 format
/// `eip155:{chain_id}`), then falls back to the `additional_networks` config map for
/// dev/test chains not in the registry (e.g. `1337` -> `"hardhat"`).
pub(crate) fn resolve_chain_name(
    chain_id: ChainId,
    registry: &NetworksRegistry,
    additional_networks: &BTreeMap<ChainId, String>,
) -> Option<String> {
    let caip2 = format!("eip155:{chain_id}");
    if let Some(network) = registry.get_network_by_caip2_id(&caip2) {
        return Some(network.id.clone());
    }
    if let Some(name) = additional_networks.get(&chain_id) {
        return Some(name.clone());
    }
    tracing::debug!(chain_id=%chain_id, "No network name found in registry or additional_networks");
    None
}
