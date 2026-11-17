//! The most dipper will pay as a base price on each chain, in GRT per 30 days. An indexer that
//! asks more is left out of selection. A ceiling is what the operator set or, left to its
//! default, a multiple of the minimum price indexer-rs suggests for the chain.

use std::{
    collections::BTreeMap,
    sync::{Arc, PoisonError, RwLock},
};

use crate::config::PAYMENT_CEILING_MULTIPLIER;

/// A suggested minimum base price for each chain, by chain name, in GRT per 30 days.
pub type Minimums = BTreeMap<String, f64>;

/// The base-price ceilings in force. What the operator set stays exactly as set. What was left
/// to its default is worked out from the suggested minimums in use at the time it is asked for.
#[derive(Debug)]
pub struct BasePriceCeilings {
    /// The operator's `max_grt_per_30_days`, which replaces every default chain ceiling.
    chains: Option<BTreeMap<String, f64>>,
    /// The operator's `fallback_max_grt_per_30_days`.
    fallback: Option<f64>,
    /// The suggested minimums the default ceilings are a multiple of.
    minimums: RwLock<Arc<Minimums>>,
}

impl BasePriceCeilings {
    /// The ceilings as configured: `None` for a setting the operator left out, whose default
    /// is then worked out from `minimums`.
    pub fn new(
        chains: Option<BTreeMap<String, f64>>,
        fallback: Option<f64>,
        minimums: Minimums,
    ) -> Self {
        Self {
            chains,
            fallback,
            minimums: RwLock::new(Arc::new(minimums)),
        }
    }

    /// Ceilings an operator set in full, for tests: nothing is left to a default.
    #[cfg(test)]
    pub fn set(chains: BTreeMap<String, f64>, fallback: f64) -> Self {
        Self::new(Some(chains), Some(fallback), Minimums::new())
    }

    /// The ceiling for a chain: its own, or the fallback when it has none.
    pub fn for_chain(&self, chain: &str) -> f64 {
        let minimums = self.minimums();
        let own = match &self.chains {
            Some(chains) => chains.get(chain).copied(),
            None => minimums.get(chain).map(|minimum| default_ceiling(*minimum)),
        };
        own.unwrap_or_else(|| self.fallback_over(&minimums))
    }

    /// The ceiling for a chain that has none of its own.
    pub fn fallback(&self) -> f64 {
        self.fallback_over(&self.minimums())
    }

    /// The suggested minimums in use.
    pub fn minimums(&self) -> Arc<Minimums> {
        let minimums = self.minimums.read().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(&minimums)
    }

    /// The operator's fallback, or by default the highest of the default chain ceilings,
    /// whatever chain ceilings the operator set.
    fn fallback_over(&self, minimums: &Minimums) -> f64 {
        self.fallback
            .unwrap_or_else(|| default_fallback_ceiling(minimums))
    }
}

/// The default ceiling for a chain whose suggested minimum is `minimum`.
pub fn default_ceiling(minimum: f64) -> f64 {
    minimum * PAYMENT_CEILING_MULTIPLIER
}

/// The default ceiling for a chain with no suggested minimum: the highest default chain ceiling.
pub fn default_fallback_ceiling(minimums: &Minimums) -> f64 {
    minimums
        .values()
        .map(|minimum| default_ceiling(*minimum))
        .fold(0.0, f64::max)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimums(entries: &[(&str, f64)]) -> Minimums {
        entries
            .iter()
            .map(|(chain, minimum)| ((*chain).to_string(), *minimum))
            .collect()
    }

    #[test]
    fn ceilings_left_to_their_defaults_are_4_times_the_suggested_minimums() {
        let ceilings =
            BasePriceCeilings::new(None, None, minimums(&[("mainnet", 45.0), ("base", 80.0)]));

        assert_eq!(ceilings.for_chain("mainnet"), 180.0);
        assert_eq!(ceilings.for_chain("base"), 320.0);
        assert_eq!(
            ceilings.for_chain("gnosis"),
            320.0,
            "a chain with no minimum gets the highest default ceiling"
        );
        assert_eq!(ceilings.fallback(), 320.0);
    }

    /// `max_grt_per_30_days` replaces the whole default table, so a chain it leaves out gets
    /// the fallback and not the default ceiling it would otherwise have had.
    #[test]
    fn chain_ceilings_the_operator_set_replace_every_default_chain_ceiling() {
        let set = BTreeMap::from([("mainnet".to_string(), 50.0)]);
        let ceilings = BasePriceCeilings::new(
            Some(set),
            None,
            minimums(&[("mainnet", 45.0), ("base", 80.0)]),
        );

        assert_eq!(ceilings.for_chain("mainnet"), 50.0);
        assert_eq!(
            ceilings.for_chain("base"),
            320.0,
            "left out of the operator's table, it gets the fallback, still the default one"
        );
    }

    #[test]
    fn a_fallback_the_operator_set_is_used_beside_default_chain_ceilings() {
        let ceilings = BasePriceCeilings::new(
            None,
            Some(1_000.0),
            minimums(&[("mainnet", 45.0), ("base", 80.0)]),
        );

        assert_eq!(ceilings.for_chain("mainnet"), 180.0);
        assert_eq!(ceilings.for_chain("gnosis"), 1_000.0);
        assert_eq!(ceilings.fallback(), 1_000.0);
    }
}
