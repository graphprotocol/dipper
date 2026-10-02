//! Keeps a reassessment's cancels and offers apart: an offer sent just after such
//! a cancel would land after it and stay open. Offers hold this shared from their
//! status check until they land. In-process only (dipper is single-replica).

use std::{sync::Arc, time::Duration};

use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Longest a reassessment waits for offers already being sent to land before it
/// gives up on its cancels. An offer holds the lock through its receipt wait
/// (15 s) and its turn at the chain client's send, so a slow RPC can outlast this.
const OFFER_DRAIN_WAIT: Duration = Duration::from_secs(30);

#[derive(Clone, Default)]
pub struct ReassessLock {
    /// Only one reassessment runs at a time across every worker loop, so two
    /// loops can't diff the same baseline and both create agreements.
    reassessment: Arc<Mutex<()>>,
    /// Offers hold it shared; a reassessment holds it exclusively while it
    /// cancels agreements whose offers may be in flight.
    offers: Arc<RwLock<()>>,
}

/// The running reassessment.
pub struct Reassessment {
    _running: OwnedMutexGuard<()>,
    offers: Arc<RwLock<()>>,
}

impl ReassessLock {
    /// Start a reassessment, or `None` (defer) if another one is running.
    pub fn start_reassessment(&self) -> Option<Reassessment> {
        Some(Reassessment {
            _running: self.reassessment.clone().try_lock_owned().ok()?,
            offers: self.offers.clone(),
        })
    }

    /// A reassessment holding offers back, as one does while it cancels.
    #[cfg(test)]
    pub async fn reassessment(&self) -> Option<(Reassessment, OwnedRwLockWriteGuard<()>)> {
        let reassessment = self.start_reassessment()?;
        let offers_held_back = reassessment.hold_back_offers().await?;
        Some((reassessment, offers_held_back))
    }

    /// Start an offer submission, or `None` (defer) while a reassessment is
    /// cancelling or waiting to. Offers never wait for each other.
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
    /// Wait for offers in flight to land and keep new ones from starting until
    /// the guard drops, or `None` after `OFFER_DRAIN_WAIT`. Waiting already holds
    /// new offers back, so a stream of them can't keep it out.
    pub async fn hold_back_offers(&self) -> Option<OwnedRwLockWriteGuard<()>> {
        let held = tokio::time::timeout(OFFER_DRAIN_WAIT, self.offers.clone().write_owned()).await;
        if held.is_err() {
            tracing::warn!(
                wait_secs = OFFER_DRAIN_WAIT.as_secs(),
                "Offers in flight did not land in time; skipping cancels that could race them"
            );
        }
        held.ok()
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
    async fn offers_run_while_a_reassessment_is_not_cancelling() {
        let lock = ReassessLock::default();
        let _running = lock.start_reassessment().expect("lock is free");
        assert!(lock.offer().is_some());
    }

    #[tokio::test]
    async fn no_offer_starts_while_a_reassessment_holds_them_back() {
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
    async fn holding_back_offers_gives_up_when_they_do_not_land_in_time() {
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
