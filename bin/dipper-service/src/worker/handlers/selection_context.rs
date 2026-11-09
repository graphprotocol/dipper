//! Shared utilities for gathering IISA selection context.

use std::collections::{HashMap, HashSet};

use dipper_iisa::SelectionContext;
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::ChainId};

use crate::{
    network::service::entity_count_cache::EntityCountCache,
    registry::{
        AgreementRegistry, IndexerDenylistRegistry, IndexingAgreement, IndexingAgreementStatus,
        WithheldIndexers,
    },
    worker::result::{JobError, JobResult},
};

/// Seconds in 28 days (aligned with IISA's Redpanda replay window).
const SECONDS_PER_28_DAYS: f64 = 86400.0 * 28.0;

/// 1 GRT = 10^18 wei.
const WEI_PER_GRT: f64 = 1e18;

/// Fewest indexers held back at once that can point at a fault on dipper's side.
const MIN_WITHHELD_FOR_DIPPER_FAULT: usize = 2;

/// Gather load balancing context for IISA selection.
///
/// This function queries the registry to build context about:
/// - Which indexers already have active agreements for this deployment
/// - What pending agreements exist across all deployments
/// - Which indexers have recently declined agreements (within lookback windows)
/// - Which indexers have stopped accepting offers, and get none until their test offer is due
/// - Which indexers are on the denylist and should be excluded entirely
/// - Optimistic DIPs fees from accepted agreement vouchers, enriched with
///   entity counts from the shared cache when available
#[allow(clippy::too_many_arguments)]
pub async fn gather_selection_context<R>(
    registry: &R,
    deployment_id: &DeploymentId,
    declined_indexer_lookback_days: i32,
    price_rejection_lookback_days: i32,
    transient_rejection_lookback_minutes: i32,
    uncertain_rejection_lookback_days: i32,
    unresponsive_indexer_lookback_days: i32,
    non_accepting_test_interval_minutes: i32,
    deployment_chain_id: ChainId,
    entity_count_cache: &EntityCountCache,
) -> JobResult<(SelectionContext, Vec<IndexerId>)>
where
    R: AgreementRegistry + IndexerDenylistRegistry,
{
    // Get indexers that already have active agreements for this deployment
    let agreements = registry
        .get_indexing_agreements_by_deployment_id(deployment_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;
    let existing_indexers = agreements
        .iter()
        .filter(|a| is_active_agreement(&a.status))
        .map(|a| a.indexer.id)
        .collect::<Vec<_>>();

    // Get pending agreements across all deployments
    let pending_agreements = registry
        .get_pending_agreement_indexers_by_deployment(&existing_indexers)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Get indexers that declined within their respective lookback periods
    let mut declined_indexers = registry
        .get_declined_indexers_by_deployment(
            declined_indexer_lookback_days,
            price_rejection_lookback_days,
            transient_rejection_lookback_minutes,
            uncertain_rejection_lookback_days,
        )
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;
    exclude_cancelling_indexers(&mut declined_indexers, *deployment_id, &agreements);

    // Get denied indexers that should be excluded from selection
    let indexer_denylist = registry
        .get_indexer_denylist()
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    // Recently-unresponsive indexers for this chain, deduped against the base denylist.
    // Returned to the caller (not merged here) so the per-chain breaker can apply this
    // exclusion or suppress it during a dipper-side outage.
    let already_denied: std::collections::HashSet<_> = indexer_denylist.iter().copied().collect();
    let unresponsive_indexers: Vec<IndexerId> = registry
        .get_unresponsive_indexers(unresponsive_indexer_lookback_days, deployment_chain_id)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?
        .into_iter()
        .filter(|id| !already_denied.contains(id))
        .collect();

    // Sent as declined, not denylisted: IISA would drop a denylisted group member, cancelling an
    // agreement it may still serve, when it has only stopped taking new ones.
    let withheld = registry
        .get_indexers_to_withhold_offers_from(non_accepting_test_interval_minutes)
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;
    let withheld: Vec<IndexerId> = withheld_unless_dipper_fault(withheld)
        .into_iter()
        .filter(|id| !already_denied.contains(id))
        .collect();
    if !withheld.is_empty() {
        tracing::info!(
            deployment_id = %deployment_id,
            withheld = withheld.len(),
            indexers = ?withheld,
            "withholding new offers from indexers that have stopped accepting them"
        );
        add_declined(&mut declined_indexers, *deployment_id, &withheld);
    }

    // Compute optimistic DIPs fees from active agreements, enriched with
    // entity counts from the shared cache when available.
    let optimistic_dips_fees = compute_optimistic_dips_fees(registry, entity_count_cache).await?;

    Ok((
        SelectionContext {
            existing_indexers,
            pending_agreements,
            declined_indexers,
            indexer_denylist,
            optimistic_dips_fees,
            ..Default::default()
        },
        unresponsive_indexers,
    ))
}

/// Compute optimistic DIPs fees per indexer in GRT per 28 days.
///
/// For each active agreement, computes the expected fee rate:
/// - If entity counts are available in the cache:
///   `fee_rate = base_rate + entity_rate * entities`
/// - Otherwise: `fee_rate = base_rate` (base rate only)
///
/// Sums per indexer and converts wei/second to GRT/28d.
async fn compute_optimistic_dips_fees<R>(
    registry: &R,
    entity_count_cache: &EntityCountCache,
) -> JobResult<HashMap<IndexerId, f64>>
where
    R: AgreementRegistry,
{
    let rates = registry
        .get_agreement_fee_rates()
        .await
        .map_err(|err| JobError::Fatal(err.into()))?;

    let cache = entity_count_cache.read().await;
    let optimistic_dips_fees = sum_fee_rates(&rates, &cache);
    let enriched = rates
        .iter()
        .filter(|r| cache.contains_key(&(r.indexer_id, r.deployment_id)))
        .count();
    drop(cache);

    if !optimistic_dips_fees.is_empty() {
        tracing::debug!(
            indexer_count = optimistic_dips_fees.len(),
            agreement_count = rates.len(),
            enriched_with_entities = enriched,
            "computed optimistic DIPs fees for IISA"
        );
    }

    Ok(optimistic_dips_fees)
}

/// Sum fee rates per indexer and convert to GRT per 28 days.
///
/// When the cache has entity counts for an (indexer, deployment) pair,
/// includes the entity component:
/// `fee_rate = base_rate + entity_rate * claimed_entities`.
/// Otherwise uses base rate only.
fn sum_fee_rates(
    rates: &[crate::registry::AgreementFeeRate],
    entity_counts: &HashMap<(IndexerId, DeploymentId), u64>,
) -> HashMap<IndexerId, f64> {
    let mut fees: HashMap<IndexerId, f64> = HashMap::new();
    for rate in rates {
        let fee_rate =
            if let Some(&entities) = entity_counts.get(&(rate.indexer_id, rate.deployment_id)) {
                rate.tokens_per_second + rate.tokens_per_entity_per_second * entities as f64
            } else {
                rate.tokens_per_second
            };
        *fees.entry(rate.indexer_id).or_default() += fee_rate;
    }
    fees.into_iter()
        .map(|(id, wei_per_sec)| (id, wei_per_second_to_grt_per_28d(wei_per_sec)))
        .collect()
}

/// Convert wei/second to GRT per 28 days.
fn wei_per_second_to_grt_per_28d(wei_per_second: f64) -> f64 {
    wei_per_second * SECONDS_PER_28_DAYS / WEI_PER_GRT
}

/// Add to the deployment's declined list the indexers whose agreement dipper is still
/// cancelling. That agreement may still be live and paid on-chain, so its indexer must not
/// be picked again, but it no longer counts towards the group IISA sizes.
fn exclude_cancelling_indexers(
    declined: &mut HashMap<DeploymentId, Vec<IndexerId>>,
    deployment_id: DeploymentId,
    agreements: &[IndexingAgreement],
) {
    let cancelling = agreements
        .iter()
        .filter(|a| a.status == IndexingAgreementStatus::Cancelling)
        .map(|a| a.indexer.id)
        .collect::<Vec<_>>();
    add_declined(declined, deployment_id, &cancelling);
}

/// The indexers to hold back from new offers, or none when at least half of those offered to in
/// the last 24 hours (and at least 2) would be: that many at once points at a fault on dipper's
/// side, such as a short escrow or a stalled chain reader, rather than at the indexers.
pub(crate) fn withheld_unless_dipper_fault(withheld: WithheldIndexers) -> Vec<IndexerId> {
    let count = withheld.indexers.len();
    if count >= MIN_WITHHELD_FOR_DIPPER_FAULT && count.saturating_mul(2) >= withheld.offered {
        tracing::warn!(
            withheld = count,
            offered = withheld.offered,
            "at least half of the indexers offered to in the last 24 hours look like they stopped \
             accepting, which points at dipper; holding back none of them"
        );
        return Vec::new();
    }
    withheld.indexers
}

/// Add `indexers` to the deployment's declined list, each once, adding no entry for none.
pub(crate) fn add_declined(
    declined: &mut HashMap<DeploymentId, Vec<IndexerId>>,
    deployment_id: DeploymentId,
    indexers: &[IndexerId],
) {
    if indexers.is_empty() {
        return;
    }
    let excluded = declined.entry(deployment_id).or_default();
    for indexer in indexers {
        if !excluded.contains(indexer) {
            excluded.push(*indexer);
        }
    }
}

/// Check if an agreement status represents an active agreement.
pub(crate) fn is_active_agreement(status: &IndexingAgreementStatus) -> bool {
    matches!(
        status,
        IndexingAgreementStatus::Created | IndexingAgreementStatus::AcceptedOnChain
    )
}

/// The indexers in IISA's DIPs `pool` that dipper's records leave free for a new offer on
/// `deployment_id`, in pool order: none holding or cancelling an agreement there, declined there,
/// denylisted, unresponsive or holding a slot. IISA's own scoring may still pass one over.
pub(crate) fn offerable_indexers(
    pool: &[IndexerId],
    deployment_id: DeploymentId,
    agreements: &[IndexingAgreement],
    declined: &HashMap<DeploymentId, Vec<IndexerId>>,
    denylist: &[IndexerId],
    unresponsive: &[IndexerId],
    held: &[IndexerId],
) -> Vec<IndexerId> {
    let mut excluded: HashSet<IndexerId> = agreements
        .iter()
        .filter(|a| {
            is_active_agreement(&a.status) || a.status == IndexingAgreementStatus::Cancelling
        })
        .map(|a| a.indexer.id)
        .collect();
    excluded.extend(declined.get(&deployment_id).into_iter().flatten());
    excluded.extend(denylist.iter().chain(unresponsive).chain(held));
    pool.iter()
        .copied()
        .filter(|id| !excluded.contains(id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cancel_dispatch::tests::agreement, registry::AgreementFeeRate};

    fn indexer(hex_digit: char) -> IndexerId {
        format!("0x{}", hex_digit.to_string().repeat(40))
            .parse()
            .unwrap()
    }

    fn on(indexer_id: IndexerId, status: IndexingAgreementStatus) -> IndexingAgreement {
        let mut a = agreement(status, None);
        a.indexer.id = indexer_id;
        a
    }

    #[test]
    fn offerable_indexers_leave_out_every_indexer_a_reassessment_would_rule_out() {
        let deployment: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let other: DeploymentId = "QmUzRg2HHMpbgf6Q4VHKNDbtBEJnyp5JWCh2gUX9AV6jXv"
            .parse()
            .unwrap();
        let pool: Vec<IndexerId> = "abcdef123".chars().map(indexer).collect();
        let agreements = [
            on(indexer('a'), IndexingAgreementStatus::AcceptedOnChain),
            on(indexer('b'), IndexingAgreementStatus::Created),
            on(indexer('c'), IndexingAgreementStatus::Cancelling),
            // Ended agreements don't keep their indexer from a new offer.
            on(indexer('3'), IndexingAgreementStatus::CanceledByRequester),
        ];
        let declined = HashMap::from([
            (deployment, vec![indexer('d')]),
            (other, vec![indexer('2')]),
        ]);

        let offerable = offerable_indexers(
            &pool,
            deployment,
            &agreements,
            &declined,
            &[indexer('e')],
            &[indexer('f')],
            &[indexer('1')],
        );

        assert_eq!(offerable, vec![indexer('2'), indexer('3')]);
    }

    #[test]
    fn no_indexer_is_offerable_when_every_pool_member_already_holds_the_deployment() {
        let deployment: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let agreements = [
            on(indexer('a'), IndexingAgreementStatus::AcceptedOnChain),
            on(indexer('b'), IndexingAgreementStatus::AcceptedOnChain),
        ];

        let offerable = offerable_indexers(
            &[indexer('a'), indexer('b')],
            deployment,
            &agreements,
            &HashMap::new(),
            &[],
            &[],
            &[],
        );

        assert!(offerable.is_empty(), "{offerable:?}");
    }

    #[test]
    fn an_indexer_still_being_cancelled_cannot_be_picked_again_for_the_deployment() {
        let deployment: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let mut cancelling = agreement(IndexingAgreementStatus::Cancelling, None);
        cancelling.indexer.id = indexer('b');
        let mut accepted = agreement(IndexingAgreementStatus::AcceptedOnChain, None);
        accepted.indexer.id = indexer('c');
        let mut declined = HashMap::from([(deployment, vec![indexer('a')])]);

        exclude_cancelling_indexers(&mut declined, deployment, &[cancelling, accepted]);

        assert_eq!(declined[&deployment], vec![indexer('a'), indexer('b')]);
    }

    #[test]
    fn a_cancelling_indexer_already_declined_is_listed_once_and_none_adds_no_entry() {
        let deployment: DeploymentId = "QmTXzATwNfgGVukV1fX2T6xw9f6LAYRVWpsdXyRWzUR2H9"
            .parse()
            .unwrap();
        let mut cancelling = agreement(IndexingAgreementStatus::Cancelling, None);
        cancelling.indexer.id = indexer('a');
        let mut declined = HashMap::from([(deployment, vec![indexer('a')])]);
        exclude_cancelling_indexers(&mut declined, deployment, &[cancelling]);
        assert_eq!(declined[&deployment], vec![indexer('a')]);

        let mut none_declined = HashMap::new();
        let accepted = agreement(IndexingAgreementStatus::AcceptedOnChain, None);
        exclude_cancelling_indexers(&mut none_declined, deployment, &[accepted]);
        assert!(none_declined.is_empty());
    }

    #[test]
    fn holds_back_no_one_once_half_of_those_offered_to_would_be_held_back() {
        let held_back = |count: usize, offered: usize| {
            withheld_unless_dipper_fault(WithheldIndexers {
                indexers: "abcdef".chars().take(count).map(indexer).collect(),
                offered,
            })
            .len()
        };

        assert_eq!(held_back(2, 4), 0, "half");
        assert_eq!(held_back(3, 4), 0, "over half");
        assert_eq!(held_back(2, 5), 2, "under half");
        assert_eq!(held_back(1, 1), 1, "1 is never enough to point at dipper");
        assert_eq!(held_back(0, 0), 0, "none");
    }

    #[test]
    fn test_wei_per_second_to_grt_per_28d() {
        let one_grt_per_sec = 1e18;
        let result = wei_per_second_to_grt_per_28d(one_grt_per_sec);
        assert!((result - 2_419_200.0).abs() < 0.01);

        let wei_per_sec = 10.0 * 1e18 / (86400.0 * 28.0);
        let result = wei_per_second_to_grt_per_28d(wei_per_sec);
        assert!((result - 10.0).abs() < 1e-6);

        assert_eq!(wei_per_second_to_grt_per_28d(0.0), 0.0);
    }

    #[test]
    fn test_sum_fee_rates_base_only() {
        let indexer_a: IndexerId = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let indexer_b: IndexerId = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .parse()
            .unwrap();
        let deployment: DeploymentId =
            "0x0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .unwrap();

        let rates = vec![
            AgreementFeeRate {
                indexer_id: indexer_a,
                deployment_id: deployment,
                tokens_per_second: 1e18,
                tokens_per_entity_per_second: 5e14,
            },
            AgreementFeeRate {
                indexer_id: indexer_a,
                deployment_id: deployment,
                tokens_per_second: 2e18,
                tokens_per_entity_per_second: 0.0,
            },
            AgreementFeeRate {
                indexer_id: indexer_b,
                deployment_id: deployment,
                tokens_per_second: 0.5e18,
                tokens_per_entity_per_second: 1e15,
            },
        ];

        let fees = sum_fee_rates(&rates, &HashMap::new());

        // indexer_a: (1 + 2) GRT/sec * 2,419,200 = 7,257,600 GRT/28d
        assert!((fees[&indexer_a] - 7_257_600.0).abs() < 1.0);
        // indexer_b: 0.5 GRT/sec * 2,419,200 = 1,209,600 GRT/28d
        assert!((fees[&indexer_b] - 1_209_600.0).abs() < 1.0);
    }

    #[test]
    fn test_sum_fee_rates_with_entity_counts() {
        let indexer_a: IndexerId = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap();
        let deployment: DeploymentId =
            "0x0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .unwrap();

        let rates = vec![AgreementFeeRate {
            indexer_id: indexer_a,
            deployment_id: deployment,
            tokens_per_second: 1e18,
            tokens_per_entity_per_second: 1e15,
        }];

        let mut entity_counts = HashMap::new();
        entity_counts.insert((indexer_a, deployment), 1000u64);

        let fees = sum_fee_rates(&rates, &entity_counts);

        // fee_rate = 1e18 + 1e15 * 1000 = 2e18
        // 2 GRT/sec * 2,419,200 = 4,838,400 GRT/28d
        assert!((fees[&indexer_a] - 4_838_400.0).abs() < 1.0);
    }

    #[test]
    fn test_sum_fee_rates_empty() {
        let fees = sum_fee_rates(&[], &HashMap::new());
        assert!(fees.is_empty());
    }
}
