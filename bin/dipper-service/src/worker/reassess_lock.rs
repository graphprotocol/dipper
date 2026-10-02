//! Keeps reassessments and offers apart: an offer sent just after a reassessment's
//! cancel would land after it and stay open. Offers hold this shared from their
//! status check until they land. In-process only (dipper is single-replica).

use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Longest a reassessment waits for offers already being sent to land before it
/// defers. An offer holds the lock through its receipt wait (15 s) and its turn
/// at the chain client's send, so a slow RPC can outlast this.
const OFFER_DRAIN_WAIT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
pub struct ReassessLock {
    /// Only one reassessment runs at a time across every worker loop, so two
    /// loops can't diff the same baseline and both create agreements.
    reassessment: Arc<Mutex<()>>,
    /// Offers hold it shared; the running reassessment holds it exclusively.
    offers: Arc<RwLock<()>>,
}

/// The running reassessment, before it has waited out the offers in flight.
pub struct Reassessment {
    reassessment: OwnedMutexGuard<()>,
    offers: Arc<RwLock<()>>,
}

/// Held for the rest of a reassessment once offers in flight have landed.
pub struct ReassessmentGuard {
    _reassessment: OwnedMutexGuard<()>,
    _offers: OwnedRwLockWriteGuard<()>,
}

impl ReassessLock {
    /// Start a reassessment, or `None` (defer) if another one is running.
    pub fn start_reassessment(&self) -> Option<Reassessment> {
        Some(Reassessment {
            reassessment: self.reassessment.clone().try_lock_owned().ok()?,
            offers: self.offers.clone(),
        })
    }

    /// Both steps at once.
    #[cfg(test)]
    pub async fn reassessment(&self) -> Option<ReassessmentGuard> {
        self.start_reassessment()?.wait_for_offers().await
    }

    /// Start an offer submission, or `None` (defer) while a reassessment is
    /// running or waiting to run. Offers never wait for each other.
    pub fn offer(&self) -> Option<OwnedRwLockReadGuard<()>> {
        self.offers.clone().try_read_owned().ok()
    }

    /// Whether a reassessment could start and take the lock right now.
    #[cfg(test)]
    pub fn reassessment_could_start_now(&self) -> bool {
        self.reassessment.try_lock().is_ok() && self.offers.try_write().is_ok()
    }
}

impl Reassessment {
    /// Wait for offers in flight to land, or `None` (defer) after
    /// `OFFER_DRAIN_WAIT`. Must come before any cancel this reassessment sends.
    /// Waiting blocks new offers, so a stream of them can't keep it out.
    pub async fn wait_for_offers(self) -> Option<ReassessmentGuard> {
        let Ok(offers) =
            tokio::time::timeout(OFFER_DRAIN_WAIT, self.offers.clone().write_owned()).await
        else {
            tracing::info!(
                wait_secs = OFFER_DRAIN_WAIT.as_secs(),
                "Offers in flight did not land in time; deferring the reassessment"
            );
            return None;
        };
        Some(ReassessmentGuard {
            _reassessment: self.reassessment,
            _offers: offers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn offers_run_alongside_each_other() {
        let lock = ReassessLock::default();
        let _first = lock.offer().expect("first offer starts");
        assert!(lock.offer().is_some(), "a second offer must not wait");
    }

    #[tokio::test]
    async fn no_offer_starts_while_a_reassessment_runs() {
        let lock = ReassessLock::default();
        let _reassessment = lock.reassessment().await.expect("lock is free");
        assert!(lock.offer().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_second_reassessment_defers_without_waiting() {
        let lock = ReassessLock::default();
        let _running = lock.reassessment().await.expect("lock is free");

        let started = tokio::time::Instant::now();
        assert!(lock.reassessment().await.is_none());
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "it must not park its loop"
        );
    }

    #[tokio::test]
    async fn a_reassessment_waits_for_an_offer_in_flight_and_blocks_new_ones() {
        let lock = ReassessLock::default();
        let in_flight = lock.offer().expect("offer starts");

        let waiting = tokio::spawn({
            let lock = lock.clone();
            async move { lock.reassessment().await.is_some() }
        });
        // Let the reassessment queue behind the offer in flight.
        while lock.reassessment.try_lock().is_ok() {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;

        assert!(
            lock.offer().is_none(),
            "a new offer must not start ahead of the waiting reassessment"
        );
        drop(in_flight);
        assert!(
            waiting.await.unwrap(),
            "the reassessment runs once the offer lands"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_reassessment_defers_when_offers_do_not_land_in_time() {
        let lock = ReassessLock::default();
        let _stuck = lock.offer().expect("offer starts");

        let started = tokio::time::Instant::now();
        assert!(lock.reassessment().await.is_none());
        assert_eq!(started.elapsed(), OFFER_DRAIN_WAIT);

        drop(_stuck);
        assert!(
            lock.reassessment_could_start_now(),
            "giving up must release both locks"
        );
    }
}
