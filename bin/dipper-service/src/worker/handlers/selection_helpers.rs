//! Shared selection helpers for indexing-request handlers.

use std::collections::BTreeMap;

use dipper_iisa::{PriceCeilings, SelectedIndexer};
use graph_networks_registry::NetworksRegistry;
use thegraph_core::alloy::primitives::{ChainId, U256};

use crate::config::{IndexingAgreementChainPrices, IndexingAgreementConfig};

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

/// Resolve pricing for a selected indexer: IISA's per-indexer price when it has one, else the
/// static pricing_table. `None` when neither has a price, or IISA's is outside `ceilings`.
pub(crate) fn resolve_pricing(
    selected: &SelectedIndexer,
    ceilings: &PriceCeilings,
    fallback_prices: Option<&IndexingAgreementChainPrices>,
    chain_id: &ChainId,
) -> Option<(U256, U256)> {
    // Prefer IISA-reported per-indexer prices
    if let Some(grt_per_30d) = selected.min_grt_per_30_days {
        let grt_per_billion = selected.min_grt_per_billion_entities_per_30_days;
        // IISA should have dropped this indexer, but an older, rolled-back or faulty one may
        // not have, and the wei conversion below turns a negative or NaN price into 0.
        if !within_cap(grt_per_30d, ceilings.max_grt_per_30_days)
            || grt_per_billion.is_some_and(|price| {
                !within_cap(price, ceilings.max_grt_per_billion_entities_per_30_days)
            })
        {
            tracing::warn!(
                indexer_id=%selected.id,
                chain_id=%chain_id,
                min_grt_per_30_days=grt_per_30d,
                max_grt_per_30_days=?ceilings.max_grt_per_30_days,
                min_grt_per_billion_entities_per_30_days=?grt_per_billion,
                max_grt_per_billion_entities_per_30_days=?ceilings.max_grt_per_billion_entities_per_30_days,
                "IISA priced the indexer above dipper's cap, below zero or not as a number"
            );
            return None;
        }
        let tokens_per_second = grt_per_30_days_to_wei_per_second(grt_per_30d);
        let tokens_per_entity_per_second = grt_per_billion
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

/// A note for each `pricing_table` price above the ceiling an IISA price is held to there.
/// Dipper offers a table price as it stands to an indexer IISA has no price for, so such a
/// price is a way to pay more than the ceilings allow. A chain with no name gets the fallback.
pub(crate) fn table_prices_above_ceilings(
    agreement_conf: &IndexingAgreementConfig,
    table: &BTreeMap<ChainId, IndexingAgreementChainPrices>,
    chain_name: impl Fn(ChainId) -> Option<String>,
) -> Vec<String> {
    const OFFERED_ANYWAY: &str =
        "an indexer IISA has no price for is offered the table price, which no ceiling limits";
    let entity_ceiling = agreement_conf.max_grt_per_billion_entities_per_30_days();
    let entity_ceiling_wei =
        grt_per_billion_entities_per_30_days_to_wei_per_entity_per_second(entity_ceiling);
    let mut notes = Vec::new();
    for (chain_id, prices) in table {
        let name = chain_name(*chain_id);
        let chain = match &name {
            Some(name) => format!("chain {chain_id} ({name})"),
            None => format!("chain {chain_id}"),
        };
        let ceiling = agreement_conf.max_grt_per_30_days_for(name.as_deref().unwrap_or_default());
        if prices.tokens_per_second > grt_per_30_days_to_wei_per_second(ceiling) {
            let worth = grt_from_wei(prices.tokens_per_second, SECONDS_PER_30_DAYS);
            notes.push(format!(
                "pricing_table tokens_per_second for {chain} is worth {worth} GRT per 30 days, \
                 above that chain's base-price ceiling of {ceiling}: {OFFERED_ANYWAY}"
            ));
        }
        if prices.tokens_per_entity_per_second > entity_ceiling_wei {
            let per_billion = SECONDS_PER_30_DAYS * 1_000_000_000;
            let worth = grt_from_wei(prices.tokens_per_entity_per_second, per_billion);
            notes.push(format!(
                "pricing_table tokens_per_entity_per_second for {chain} is worth {worth} GRT per \
                 billion entities per 30 days, above the per-entity ceiling of {entity_ceiling}: \
                 {OFFERED_ANYWAY}"
            ));
        }
    }
    notes
}

/// `wei` times `times`, in GRT, to 2 decimal places at the most, for a message.
fn grt_from_wei(wei: U256, times: u128) -> String {
    let grt = wei.saturating_to::<u128>() as f64 * times as f64 / WEI_PER_GRT as f64;
    let rounded = format!("{grt:.2}");
    let trimmed = rounded.trim_end_matches('0').trim_end_matches('.');
    trimmed.to_string()
}

/// Whether dipper may offer an IISA price: finite, not below zero, and at most `cap`
/// (`None` is no cap). A price equal to the cap passes, as it does in IISA.
fn within_cap(price: f64, cap: Option<f64>) -> bool {
    price.is_finite() && price >= 0.0 && cap.is_none_or(|cap| price <= cap)
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

#[cfg(test)]
mod tests {
    use thegraph_core::{IndexerId, alloy::primitives::Address};

    use super::*;

    const CHAIN: ChainId = 42161;

    fn priced(base: Option<f64>, per_entity: Option<f64>) -> SelectedIndexer {
        SelectedIndexer {
            id: IndexerId::from(Address::repeat_byte(1)),
            min_grt_per_30_days: base,
            min_grt_per_billion_entities_per_30_days: per_entity,
        }
    }

    fn caps(base: Option<f64>, per_entity: Option<f64>) -> PriceCeilings {
        PriceCeilings {
            max_grt_per_30_days: base,
            max_grt_per_billion_entities_per_30_days: per_entity,
        }
    }

    /// A table price is what dipper offers an indexer that IISA has no price for, whatever
    /// the ceilings are. Start-up says so for each one above its ceiling, and still starts.
    #[test]
    fn notes_each_table_price_above_the_ceiling_for_its_chain() {
        let conf = IndexingAgreementConfig {
            max_grt_per_30_days: BTreeMap::from([("arbitrum-one".to_string(), 1_800.0)]),
            fallback_max_grt_per_30_days: 900.0,
            max_grt_per_billion_entities_per_30_days: 800.0,
            ..IndexingAgreementConfig::for_tests()
        };
        let prices = |base: f64, per_entity: f64| IndexingAgreementChainPrices {
            tokens_per_second: grt_per_30_days_to_wei_per_second(base),
            tokens_per_entity_per_second:
                grt_per_billion_entities_per_30_days_to_wei_per_entity_per_second(per_entity),
        };
        let table = BTreeMap::from([
            // Both parts above their ceilings of 1,800 and 800.
            (42161, prices(3_000.0, 2_000.0)),
            // At its chain's ceiling, the fallback of 900, and at the per-entity one.
            (1, prices(900.0, 800.0)),
            // A chain with no name is held to the fallback.
            (1337, prices(1_000.0, 0.0)),
        ]);
        let chain_name = |chain: ChainId| match chain {
            42161 => Some("arbitrum-one".to_string()),
            1 => Some("mainnet".to_string()),
            _ => None,
        };

        let notes = table_prices_above_ceilings(&conf, &table, chain_name);

        assert_eq!(notes.len(), 3, "{notes:#?}");
        let base = &notes[1];
        assert!(base.contains("chain 42161 (arbitrum-one)"), "{base}");
        assert!(
            base.contains("3000 GRT per 30 days") && base.contains("ceiling of 1800"),
            "it names both figures: {base}"
        );
        let per_entity = &notes[2];
        assert!(
            per_entity.contains("chain 42161 (arbitrum-one)"),
            "{per_entity}"
        );
        assert!(
            per_entity.contains("2000 GRT per billion entities per 30 days")
                && per_entity.contains("ceiling of 800"),
            "{per_entity}"
        );
        let unnamed = &notes[0];
        assert!(
            unnamed.contains("chain 1337")
                && unnamed.contains("1000 GRT per 30 days")
                && unnamed.contains("ceiling of 900"),
            "{unnamed}"
        );
    }

    #[test]
    fn within_cap_takes_any_finite_price_from_0_up_to_the_cap() {
        assert!(within_cap(0.0, Some(450.0)));
        assert!(
            within_cap(450.0, Some(450.0)),
            "a price equal to the cap passes"
        );
        assert!(!within_cap(450.01, Some(450.0)));
        assert!(within_cap(1e9, None), "no cap takes any finite price");
        for price in [-1.0, f64::NAN, f64::INFINITY] {
            assert!(!within_cap(price, None), "{price}");
            assert!(!within_cap(price, Some(450.0)), "{price}");
        }
    }

    #[test]
    fn prices_an_indexer_within_both_caps() {
        let pricing = resolve_pricing(
            &priced(Some(450.0), Some(2000.0)),
            &caps(Some(450.0), Some(2000.0)),
            None,
            &CHAIN,
        );
        assert_eq!(
            pricing,
            Some((
                grt_per_30_days_to_wei_per_second(450.0),
                grt_per_billion_entities_per_30_days_to_wei_per_entity_per_second(2000.0),
            ))
        );
    }

    #[test]
    fn prices_a_base_price_without_a_per_entity_price() {
        let pricing = resolve_pricing(
            &priced(Some(100.0), None),
            &caps(Some(450.0), Some(2000.0)),
            None,
            &CHAIN,
        );
        assert_eq!(
            pricing,
            Some((grt_per_30_days_to_wei_per_second(100.0), U256::ZERO))
        );
    }

    #[test]
    fn skips_an_indexer_priced_over_either_cap() {
        let ceilings = caps(Some(450.0), Some(2000.0));
        assert_eq!(
            resolve_pricing(&priced(Some(451.0), Some(1.0)), &ceilings, None, &CHAIN),
            None
        );
        assert_eq!(
            resolve_pricing(&priced(Some(1.0), Some(2001.0)), &ceilings, None, &CHAIN),
            None
        );
    }

    #[test]
    fn a_missing_cap_checks_only_that_the_price_is_usable() {
        let no_caps = caps(None, None);
        assert!(resolve_pricing(&priced(Some(1e6), Some(1e6)), &no_caps, None, &CHAIN).is_some());
        assert_eq!(
            resolve_pricing(&priced(Some(-1.0), None), &no_caps, None, &CHAIN),
            None
        );
        assert_eq!(
            resolve_pricing(&priced(Some(1.0), Some(f64::NAN)), &no_caps, None, &CHAIN),
            None
        );
    }

    #[test]
    fn falls_back_to_the_pricing_table_only_when_iisa_has_no_price() {
        let ceilings = caps(Some(450.0), Some(2000.0));
        let table = IndexingAgreementChainPrices {
            tokens_per_second: U256::from(7),
            tokens_per_entity_per_second: U256::from(3),
        };
        assert_eq!(
            resolve_pricing(&priced(None, None), &ceilings, Some(&table), &CHAIN),
            Some((U256::from(7), U256::from(3)))
        );
        assert_eq!(
            resolve_pricing(&priced(None, None), &ceilings, None, &CHAIN),
            None
        );
    }
}
