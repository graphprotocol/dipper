//! Mass-unresponsive circuit breaker: when a dipper-side outage makes proposals to
//! many indexers on a chain time out at once, suppress that chain's unresponsive
//! exclusion (a large unresponsive fraction signals a dipper-side cause, not indexers).

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use dipper_iisa::{CandidateSelection, DipsAcceptingSnapshot};
use thegraph_core::IndexerId;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Per-chain hysteresis state. In-memory map shared across all worker loops
/// (dipper is single-replica); a chain's entry being `true` means that chain's
/// unresponsive exclusion is currently suppressed.
#[derive(Default)]
pub struct UnresponsiveBreaker {
    tripped: Mutex<HashMap<String, bool>>,
}

impl UnresponsiveBreaker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide whether to suppress `chain`'s unresponsive exclusion this round, updating
    /// that chain's hysteresis (trip above `trip`, resume below `reset`, hold between).
    /// Fail-safe: a missing/stale/empty snapshot returns `false` without mutating state.
    #[expect(
        clippy::expect_used,
        reason = "predates this lint; fix when next touched"
    )]
    pub fn evaluate(
        &self,
        chain: &str,
        unresponsive: &[IndexerId],
        snapshot: Option<&DipsAcceptingSnapshot>,
        trip: f64,
        reset: f64,
        max_age_hours: i64,
    ) -> bool {
        let Some(snapshot) = snapshot else {
            return false;
        };
        let Some(computed_at) = snapshot.computed_at.as_deref() else {
            return false;
        };
        let Ok(computed_at) = OffsetDateTime::parse(computed_at, &Rfc3339) else {
            tracing::warn!(
                chain,
                "could not parse IISA snapshot timestamp; treating as stale"
            );
            return false;
        };
        if OffsetDateTime::now_utc() - computed_at > time::Duration::hours(max_age_hours) {
            return false;
        }
        let pool: HashSet<IndexerId> = snapshot.indexers.iter().copied().collect();
        if pool.is_empty() {
            return false;
        }

        let benched = unresponsive.iter().filter(|id| pool.contains(id)).count();
        let fraction = benched as f64 / pool.len() as f64;

        let mut state = self.tripped.lock().expect("breaker mutex poisoned");
        let tripped = state.get(chain).copied().unwrap_or(false);

        if !tripped && fraction > trip {
            state.insert(chain.to_string(), true);
            tracing::warn!(
                chain,
                benched,
                pool = pool.len(),
                fraction,
                trip,
                "mass-unresponsive breaker TRIPPED; suppressing this chain's unresponsive \
                 exclusions (likely a dipper-side outage, not indexer faults)"
            );
            true
        } else if tripped && fraction < reset {
            state.insert(chain.to_string(), false);
            tracing::warn!(
                chain,
                fraction,
                reset,
                "mass-unresponsive breaker RESET; resuming this chain's exclusions"
            );
            false
        } else {
            tripped
        }
    }

    #[cfg(test)]
    fn is_tripped(&self, chain: &str) -> bool {
        self.tripped
            .lock()
            .expect("breaker mutex poisoned")
            .get(chain)
            .copied()
            .unwrap_or(false)
    }
}

/// Cache keyed by chain name -> (fetch time, snapshot). The snapshot is shared via
/// `Arc` so a cache hit hands out a pointer instead of copying the indexer list.
type CacheStore = Arc<tokio::sync::Mutex<HashMap<String, (Instant, Arc<DipsAcceptingSnapshot>)>>>;

/// Short-TTL, per-chain cache of IISA's DIPs-accepting set, so a burst of
/// reassessments doesn't re-query the same daily snapshot once per request.
#[derive(Clone)]
pub struct DipsAcceptingCache {
    inner: CacheStore,
    ttl: Duration,
}

impl DipsAcceptingCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            ttl,
        }
    }

    /// Return a cached snapshot if it's within the TTL, otherwise fetch from IISA
    /// and cache it. A fetch error yields `None` (the breaker stays inactive this
    /// pass) rather than failing the job. The lock is never held across the await.
    pub async fn get_or_fetch<I: CandidateSelection + ?Sized>(
        &self,
        iisa: &I,
        chain: &str,
        max_grt_per_30_days: Option<f64>,
    ) -> Option<Arc<DipsAcceptingSnapshot>> {
        {
            let guard = self.inner.lock().await;
            if let Some((fetched_at, snapshot)) = guard.get(chain)
                && fetched_at.elapsed() < self.ttl
            {
                return Some(Arc::clone(snapshot));
            }
        }
        match iisa
            .dips_accepting_indexers(chain, max_grt_per_30_days)
            .await
        {
            Ok(snapshot) => {
                let snapshot = Arc::new(snapshot);
                let mut guard = self.inner.lock().await;
                guard.insert(chain.to_string(), (Instant::now(), Arc::clone(&snapshot)));
                Some(snapshot)
            }
            Err(err) => {
                tracing::warn!(error = %err, "DIPs-accepting fetch failed; breaker inactive this pass");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn indexer(byte: u8) -> IndexerId {
        IndexerId::from(thegraph_core::alloy::primitives::Address::repeat_byte(byte))
    }

    fn snapshot(pool: &[IndexerId], age_hours: i64) -> DipsAcceptingSnapshot {
        let ts = (OffsetDateTime::now_utc() - time::Duration::hours(age_hours))
            .format(&Rfc3339)
            .unwrap();
        DipsAcceptingSnapshot {
            computed_at: Some(ts),
            indexers: pool.to_vec(),
        }
    }

    #[test]
    fn trips_above_trip_and_resets_below_reset_with_deadband() {
        let pool: Vec<IndexerId> = (0..10).map(indexer).collect();
        let breaker = UnresponsiveBreaker::new();
        let fresh = snapshot(&pool, 0);

        // 6/10 = 0.6 > 0.5 -> trip (suppress).
        assert!(breaker.evaluate("c1", &pool[..6], Some(&fresh), 0.5, 0.25, 48));
        assert!(breaker.is_tripped("c1"));

        // 4/10 = 0.4 sits in the dead-band -> hold suppressed.
        assert!(breaker.evaluate("c1", &pool[..4], Some(&fresh), 0.5, 0.25, 48));
        assert!(breaker.is_tripped("c1"));

        // 2/10 = 0.2 < 0.25 -> reset (resume exclusions).
        assert!(!breaker.evaluate("c1", &pool[..2], Some(&fresh), 0.5, 0.25, 48));
        assert!(!breaker.is_tripped("c1"));
    }

    #[test]
    fn does_not_trip_below_trip_from_untripped() {
        let pool: Vec<IndexerId> = (0..10).map(indexer).collect();
        let breaker = UnresponsiveBreaker::new();
        // 4/10 = 0.4, still under 0.5 from an untripped start -> apply exclusions.
        assert!(!breaker.evaluate("c1", &pool[..4], Some(&snapshot(&pool, 0)), 0.5, 0.25, 48));
        assert!(!breaker.is_tripped("c1"));
    }

    #[test]
    fn per_chain_state_is_isolated() {
        let pool: Vec<IndexerId> = (0..10).map(indexer).collect();
        let breaker = UnresponsiveBreaker::new();
        let fresh = snapshot(&pool, 0);

        // Chain a: 6/10 = 0.6 > 0.5 -> trips a only.
        assert!(breaker.evaluate("a", &pool[..6], Some(&fresh), 0.5, 0.25, 48));
        assert!(breaker.is_tripped("a"));
        assert!(!breaker.is_tripped("b"));

        // Chain b: 1/10 = 0.1 -> stays untripped and must NOT reset a.
        assert!(!breaker.evaluate("b", &pool[..1], Some(&fresh), 0.5, 0.25, 48));
        assert!(!breaker.is_tripped("b"));
        assert!(breaker.is_tripped("a"));

        // Chain a dead-band (0.4) reads a's own state, not b's.
        assert!(breaker.evaluate("a", &pool[..4], Some(&fresh), 0.5, 0.25, 48));
        assert!(breaker.is_tripped("a"));
    }

    #[test]
    fn fail_safe_paths_do_not_suppress_or_mutate_state() {
        let pool: Vec<IndexerId> = (0..10).map(indexer).collect();
        let all = &pool[..]; // 10/10 = 1.0, would trip if the snapshot were usable
        let breaker = UnresponsiveBreaker::new();

        // No snapshot (IISA unreachable).
        assert!(!breaker.evaluate("c1", all, None, 0.5, 0.25, 48));
        assert!(!breaker.is_tripped("c1"));

        // No computed_at (IISA has no scores).
        let no_ts = DipsAcceptingSnapshot {
            computed_at: None,
            indexers: pool.clone(),
        };
        assert!(!breaker.evaluate("c1", all, Some(&no_ts), 0.5, 0.25, 48));
        assert!(!breaker.is_tripped("c1"));

        // Stale snapshot (older than max age).
        assert!(!breaker.evaluate("c1", all, Some(&snapshot(&pool, 72)), 0.5, 0.25, 48));
        assert!(!breaker.is_tripped("c1"));

        // Empty pool (denominator 0).
        let empty = snapshot(&[], 0);
        assert!(!breaker.evaluate("c1", all, Some(&empty), 0.5, 0.25, 48));
        assert!(!breaker.is_tripped("c1"));
    }

    #[test]
    fn numerator_only_counts_indexers_in_the_pool() {
        let pool: Vec<IndexerId> = (0..4).map(indexer).collect();
        let breaker = UnresponsiveBreaker::new();
        // Six unresponsive, but only the 4 pool members count -> 4/4 = 1.0 > 0.5.
        let unresponsive: Vec<IndexerId> = (0..6).map(indexer).collect();
        assert!(breaker.evaluate(
            "c1",
            &unresponsive,
            Some(&snapshot(&pool, 0)),
            0.5,
            0.25,
            48
        ));
    }
}
