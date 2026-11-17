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
        self.ceiling_over(chain, &self.minimums())
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

    /// Whether a ceiling is left to its default, and so follows the suggested minimums.
    pub fn follows_minimums(&self) -> bool {
        self.chains.is_none() || self.fallback.is_none()
    }

    /// Takes `fetched` as the suggested minimums in use, if it passes every check against
    /// the ones in use now, and says which ceilings that changed.
    pub fn follow(
        &self,
        fetched: Minimums,
        max_agreement_grt_per_30_days: f64,
    ) -> Result<Vec<CeilingChange>, Refusal> {
        let mut in_use = self
            .minimums
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        checked(&fetched, &in_use, max_agreement_grt_per_30_days)?;
        // No chain is ever dropped, so the fetched table names every chain that can change.
        let changes = fetched
            .keys()
            .filter_map(|chain| {
                let old = self.ceiling_over(chain, &in_use);
                let new = self.ceiling_over(chain, &fetched);
                // With the operator's own chain ceilings, a chain only moves with the fallback.
                (self.chains.is_none() && old != new).then(|| CeilingChange {
                    chain: Some(chain.clone()),
                    old,
                    new,
                })
            })
            .chain(self.fallback_change(&in_use, &fetched))
            .collect();
        *in_use = Arc::new(fetched);
        Ok(changes)
    }

    /// How the fallback moves from 1 table of minimums to the next, if it does.
    fn fallback_change(&self, in_use: &Minimums, fetched: &Minimums) -> Option<CeilingChange> {
        let old = self.fallback_over(in_use);
        let new = self.fallback_over(fetched);
        (old != new).then_some(CeilingChange {
            chain: None,
            old,
            new,
        })
    }

    /// The ceiling for a chain when the suggested minimums are `minimums`.
    fn ceiling_over(&self, chain: &str, minimums: &Minimums) -> f64 {
        let own = match &self.chains {
            Some(chains) => chains.get(chain).copied(),
            None => minimums.get(chain).map(|minimum| default_ceiling(*minimum)),
        };
        own.unwrap_or_else(|| self.fallback_over(minimums))
    }

    /// The operator's fallback, or by default the highest of the default chain ceilings,
    /// whatever chain ceilings the operator set.
    fn fallback_over(&self, minimums: &Minimums) -> f64 {
        self.fallback
            .unwrap_or_else(|| default_fallback_ceiling(minimums))
    }
}

/// A ceiling that changed when a new table of suggested minimums was taken.
#[derive(Debug, Clone, PartialEq)]
pub struct CeilingChange {
    /// The chain, or `None` for the fallback.
    pub chain: Option<String>,
    pub old: f64,
    pub new: f64,
}

/// The most a suggested minimum may move, up or down, between the table in use and the next.
pub const MAX_CHANGE_FACTOR: f64 = 10.0;

/// Why a table of suggested minimums is not taken, and the one in use is kept.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum Refusal {
    #[error("it cannot be read: {0}")]
    Unreadable(String),
    #[error("it lists no chain")]
    Empty,
    #[error("its minimum for {chain} ({value}) is not a positive number")]
    NotPositive { chain: String, value: f64 },
    #[error("it leaves out {chain}, which the table in use has")]
    ChainDropped { chain: String },
    #[error(
        "it moves the minimum for {chain} from {from} to {to}, more than {MAX_CHANGE_FACTOR} times"
    )]
    MovedTooFar { chain: String, from: f64, to: f64 },
    #[error(
        "its minimum for {chain} ({minimum}) puts that ceiling at {ceiling}, above \
         max_agreement_grt_per_30_days ({cap})"
    )]
    AboveAgreementCap {
        chain: String,
        minimum: f64,
        ceiling: f64,
        cap: f64,
    },
}

/// Reads the table as indexer-rs publishes it: a JSON object whose `min_grt_per_30_days` gives
/// a number for each chain name. Anything else in the object is ignored.
pub fn parse_minimums(body: &[u8]) -> Result<Minimums, Refusal> {
    #[derive(serde::Deserialize)]
    struct Published {
        min_grt_per_30_days: ChainMinimums,
    }
    let published: Published =
        serde_json::from_slice(body).map_err(|error| Refusal::Unreadable(error.to_string()))?;
    Ok(published.min_grt_per_30_days.0)
}

/// The published table, read strictly: a chain may appear once, and must have a name.
struct ChainMinimums(Minimums);

impl<'de> serde::Deserialize<'de> for ChainMinimums {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Chains;
        impl<'de> serde::de::Visitor<'de> for Chains {
            type Value = ChainMinimums;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a minimum price for each chain name")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut entries: A,
            ) -> Result<Self::Value, A::Error> {
                use serde::de::Error as _;
                let mut minimums = Minimums::new();
                while let Some((chain, minimum)) = entries.next_entry::<String, f64>()? {
                    if chain.is_empty() {
                        return Err(A::Error::custom("a chain has no name"));
                    }
                    if minimums.insert(chain.clone(), minimum).is_some() {
                        return Err(A::Error::custom(format!("{chain} is listed twice")));
                    }
                }
                Ok(ChainMinimums(minimums))
            }
        }
        deserializer.deserialize_map(Chains)
    }
}

/// Checks a table against the one in use and the most an agreement may pay.
fn checked(
    fetched: &Minimums,
    in_use: &Minimums,
    max_agreement_grt_per_30_days: f64,
) -> Result<(), Refusal> {
    if fetched.is_empty() {
        return Err(Refusal::Empty);
    }
    for (chain, minimum) in fetched {
        if !(minimum.is_finite() && *minimum > 0.0) {
            return Err(Refusal::NotPositive {
                chain: chain.clone(),
                value: *minimum,
            });
        }
    }
    for (chain, from) in in_use {
        let Some(to) = fetched.get(chain).copied() else {
            return Err(Refusal::ChainDropped {
                chain: chain.clone(),
            });
        };
        if to > from * MAX_CHANGE_FACTOR || to < from / MAX_CHANGE_FACTOR {
            return Err(Refusal::MovedTooFar {
                chain: chain.clone(),
                from: *from,
                to,
            });
        }
    }
    // Compared in wei per second, rounded as dipper rounds an offer's price, since that is what
    // the contract holds an agreement to; the config's own ceilings are checked the same way.
    let cap = crate::worker::grt_per_30_days_to_wei_per_second(max_agreement_grt_per_30_days);
    for (chain, minimum) in fetched {
        let ceiling = default_ceiling(*minimum);
        if crate::worker::grt_per_30_days_to_wei_per_second(ceiling) > cap {
            return Err(Refusal::AboveAgreementCap {
                chain: chain.clone(),
                minimum: *minimum,
                ceiling,
                cap: max_agreement_grt_per_30_days,
            });
        }
    }
    Ok(())
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

    /// The table in use in these tests, and the most an agreement may pay.
    fn in_use() -> Minimums {
        minimums(&[("arbitrum-one", 450.0), ("mainnet", 45.0), ("sepolia", 5.0)])
    }
    const CAP: f64 = 20_000.0;

    #[test]
    fn reads_the_file_indexer_rs_publishes() {
        let body = br#"{
            "about": ["Suggested minimums.", "Dipper reads this file."],
            "min_grt_per_30_days": {"arbitrum-one": 450, "base": 80.5, "sepolia": 5}
        }"#;

        assert_eq!(
            parse_minimums(body),
            Ok(minimums(&[
                ("arbitrum-one", 450.0),
                ("base", 80.5),
                ("sepolia", 5.0)
            ]))
        );
    }

    #[test]
    fn refuses_a_file_it_cannot_read() {
        let unreadable: [&[u8]; 8] = [
            b"<html>404: Not Found</html>",
            b"",
            br#"{"about": []}"#,
            br#"{"min_grt_per_30_days": ["mainnet", 45]}"#,
            br#"{"min_grt_per_30_days": {"mainnet": "45"}}"#,
            br#"{"min_grt_per_30_days": {"mainnet": null}}"#,
            br#"{"min_grt_per_30_days": {"mainnet": 1e999}}"#,
            br#"{"min_grt_per_30_days": {"": 45}}"#,
        ];
        for body in unreadable {
            let refused = parse_minimums(body);
            assert!(
                matches!(refused, Err(Refusal::Unreadable(_))),
                "{}: {refused:?}",
                String::from_utf8_lossy(body)
            );
        }
    }

    /// JSON lets a name appear twice and most readers keep the last. For a table that sets
    /// what dipper will pay, 2 answers for 1 chain is a mistake to refuse, not to pick from.
    #[test]
    fn refuses_a_file_that_lists_a_chain_twice() {
        let body = br#"{"min_grt_per_30_days": {"mainnet": 45, "base": 80, "mainnet": 4500}}"#;

        let refused = parse_minimums(body).expect_err("a chain listed twice");

        assert!(
            refused.to_string().contains("mainnet"),
            "it names the chain: {refused}"
        );
    }

    #[test]
    fn refuses_an_empty_table() {
        assert_eq!(
            checked(&Minimums::new(), &in_use(), CAP),
            Err(Refusal::Empty)
        );
    }

    #[test]
    fn refuses_a_minimum_that_is_not_a_positive_number() {
        for value in [0.0, -45.0, f64::NAN, f64::INFINITY] {
            let mut fetched = in_use();
            fetched.insert("mainnet".to_string(), value);

            let refused = checked(&fetched, &in_use(), CAP);

            assert!(
                matches!(&refused, Err(Refusal::NotPositive { chain, .. }) if chain == "mainnet"),
                "{value}: {refused:?}"
            );
        }
    }

    #[test]
    fn refuses_a_table_that_drops_a_chain() {
        let mut fetched = in_use();
        fetched.remove("sepolia");

        assert_eq!(
            checked(&fetched, &in_use(), CAP),
            Err(Refusal::ChainDropped {
                chain: "sepolia".to_string()
            })
        );
    }

    #[test]
    fn refuses_a_minimum_that_moves_by_more_than_10_times() {
        for (to, taken) in [
            (450.0, true),
            (451.0, false),
            (4.5, true),
            (4.49, false),
            (46.0, true),
        ] {
            let mut fetched = in_use();
            fetched.insert("mainnet".to_string(), to);

            let outcome = checked(&fetched, &in_use(), CAP);

            let moved_too_far = Err(Refusal::MovedTooFar {
                chain: "mainnet".to_string(),
                from: 45.0,
                to,
            });
            assert_eq!(outcome, if taken { Ok(()) } else { moved_too_far }, "{to}");
        }
    }

    /// The contract refuses an agreement priced above the cap, so a ceiling above it would
    /// have dipper send offers that no indexer can accept.
    #[test]
    fn refuses_a_minimum_that_puts_a_ceiling_above_the_agreement_cap() {
        let mut fetched = in_use();
        fetched.insert("arbitrum-one".to_string(), 500.0);

        assert_eq!(checked(&fetched, &in_use(), 2_000.0), Ok(()), "at the cap");
        assert_eq!(
            checked(&fetched, &in_use(), 1_999.0),
            Err(Refusal::AboveAgreementCap {
                chain: "arbitrum-one".to_string(),
                minimum: 500.0,
                ceiling: 2_000.0,
                cap: 1_999.0,
            })
        );
    }

    #[test]
    fn takes_a_table_that_adds_a_chain_and_moves_others_within_bounds() {
        let mut fetched = in_use();
        fetched.insert("base".to_string(), 80.0);
        fetched.insert("mainnet".to_string(), 60.0);

        assert_eq!(checked(&fetched, &in_use(), CAP), Ok(()));
    }

    #[test]
    fn default_ceilings_follow_a_table_that_is_taken() {
        let ceilings = BasePriceCeilings::new(None, None, in_use());
        let mut fetched = in_use();
        fetched.insert("arbitrum-one".to_string(), 500.0);
        fetched.insert("mainnet".to_string(), 30.0);
        fetched.insert("base".to_string(), 80.0);

        let changes = ceilings.follow(fetched, CAP).expect("the table is taken");

        assert!(ceilings.follows_minimums());
        assert_eq!(ceilings.for_chain("arbitrum-one"), 2_000.0);
        assert_eq!(ceilings.for_chain("mainnet"), 120.0);
        assert_eq!(ceilings.for_chain("base"), 320.0);
        assert_eq!(ceilings.for_chain("sepolia"), 20.0, "unchanged");
        assert_eq!(
            ceilings.for_chain("gnosis"),
            2_000.0,
            "the fallback follows"
        );
        let change = |chain: Option<&str>, old, new| CeilingChange {
            chain: chain.map(str::to_string),
            old,
            new,
        };
        assert_eq!(
            changes,
            [
                change(Some("arbitrum-one"), 1_800.0, 2_000.0),
                // A chain new to the table had the fallback until now.
                change(Some("base"), 1_800.0, 320.0),
                change(Some("mainnet"), 180.0, 120.0),
                change(None, 1_800.0, 2_000.0),
            ]
        );
    }

    #[test]
    fn a_table_that_is_refused_leaves_the_one_in_use() {
        let ceilings = BasePriceCeilings::new(None, None, in_use());
        let mut fetched = in_use();
        fetched.insert("mainnet".to_string(), 4_500.0);

        let refused = ceilings.follow(fetched, CAP);

        assert!(
            matches!(refused, Err(Refusal::MovedTooFar { .. })),
            "{refused:?}"
        );
        assert_eq!(*ceilings.minimums(), in_use());
        assert_eq!(ceilings.for_chain("mainnet"), 180.0);
    }

    /// `max_grt_per_30_days` set by the operator stays exactly as set. The fallback, left
    /// out, is still the highest default chain ceiling, so it follows the table.
    #[test]
    fn chain_ceilings_the_operator_set_do_not_follow_but_a_default_fallback_does() {
        let set = BTreeMap::from([("mainnet".to_string(), 50.0)]);
        let ceilings = BasePriceCeilings::new(Some(set), None, in_use());
        let mut fetched = in_use();
        fetched.insert("arbitrum-one".to_string(), 500.0);
        fetched.insert("mainnet".to_string(), 60.0);

        let changes = ceilings.follow(fetched, CAP).expect("the table is taken");

        assert!(ceilings.follows_minimums());
        assert_eq!(
            ceilings.for_chain("mainnet"),
            50.0,
            "as the operator set it"
        );
        assert_eq!(ceilings.for_chain("arbitrum-one"), 2_000.0, "the fallback");
        assert_eq!(
            changes,
            [CeilingChange {
                chain: None,
                old: 1_800.0,
                new: 2_000.0
            }]
        );
    }

    #[test]
    fn a_fallback_the_operator_set_does_not_follow_but_default_chain_ceilings_do() {
        let ceilings = BasePriceCeilings::new(None, Some(1_000.0), in_use());
        let mut fetched = in_use();
        fetched.insert("arbitrum-one".to_string(), 500.0);

        let changes = ceilings.follow(fetched, CAP).expect("the table is taken");

        assert!(ceilings.follows_minimums());
        assert_eq!(
            ceilings.for_chain("gnosis"),
            1_000.0,
            "as the operator set it"
        );
        assert_eq!(
            changes,
            [CeilingChange {
                chain: Some("arbitrum-one".to_string()),
                old: 1_800.0,
                new: 2_000.0
            }]
        );
    }

    #[test]
    fn ceilings_the_operator_set_in_full_follow_nothing() {
        let set = BTreeMap::from([("mainnet".to_string(), 50.0)]);
        let ceilings = BasePriceCeilings::new(Some(set), Some(1_000.0), in_use());
        let mut fetched = in_use();
        fetched.insert("mainnet".to_string(), 60.0);

        let changes = ceilings.follow(fetched, CAP).expect("the table is taken");

        assert!(!ceilings.follows_minimums());
        assert_eq!(changes, []);
        assert_eq!(ceilings.for_chain("mainnet"), 50.0);
        assert_eq!(ceilings.for_chain("gnosis"), 1_000.0);
    }

    #[test]
    fn a_table_equal_to_the_one_in_use_changes_nothing() {
        let ceilings = BasePriceCeilings::new(None, None, in_use());

        assert_eq!(ceilings.follow(in_use(), CAP), Ok(Vec::new()));
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
