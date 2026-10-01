//! Keeps the RecurringAgreementManager's escrow tidy. The manager only rebalances a
//! provider when one of its agreements changes or is collected, so this sweep withdraws
//! finished thaws and rebalances each provider periodically, and only where needed.

use std::{future::Future, time::Duration};

use thegraph_core::alloy::primitives::{Address, U256};
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    chain_client::{ChainClient, ChainClientError, EscrowAccount, ManagerEscrowReader},
    config::{EscrowReconcilerConfig, IndexingAgreementConfig},
};

/// Whether the reconciler should run: only when its config section is present and enabled.
pub fn should_run(
    config: Option<&EscrowReconcilerConfig>,
    _agreement_conf: &IndexingAgreementConfig,
) -> bool {
    config.is_some_and(|c| c.enabled)
}

/// Handle for controlling the escrow reconciler service lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the escrow reconciler service gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the escrow reconciler service
pub struct Ctx<T> {
    /// Chain client used to read the manager's escrow and call `reconcileProvider`
    pub chain_client: T,
    /// Service configuration
    pub config: EscrowReconcilerConfig,
    /// The RecurringCollector address passed as the manager's `collector` arg
    pub collector: Address,
}

/// Create a new escrow reconciler service. Returns a handle plus a future to spawn.
pub fn new<T>(ctx: Ctx<T>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    T: ChainClient + ManagerEscrowReader + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        chain_client,
        config,
        collector,
    } = ctx;

    let service = async move {
        tracing::info!(
            interval_secs = config.interval.as_secs(),
            rebalance_interval_secs = config.rebalance_interval.as_secs(),
            batch_size = config.batch_size,
            collector = %collector,
            "escrow reconciler service started"
        );

        // The first sweep waits a full interval, so a restart doesn't send a burst.
        let mut timer = tokio::time::interval_at(
            tokio::time::Instant::now() + config.interval,
            config.interval,
        );
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            let due = match providers_due(&chain_client, collector, &config).await {
                Ok(due) => due,
                Err(err) => {
                    tracing::error!(error = %err, "failed to read the manager's escrow for reconciliation");
                    continue;
                }
            };

            if due.is_empty() {
                tracing::debug!("escrow reconciliation: no provider needs a reconcile");
                continue;
            }

            match reconcile_providers(&chain_client, &mut rx_stop, collector, due).await {
                Outcome::Stopped => return Ok(()),
                Outcome::Done { ok, failed } => {
                    tracing::info!(
                        reconciled = ok,
                        failed,
                        "escrow reconciliation sweep completed"
                    );
                }
            }
        }

        tracing::debug!("escrow reconciler service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

/// Why a provider gets a reconcile this sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reason {
    /// A thaw has run its course, so the tokens can be withdrawn.
    ThawFinished,
    /// The provider's periodic rebalance is due.
    Rebalance,
}

/// Providers to reconcile this sweep: those whose thaw has finished first, then those
/// due a rebalance, capped at `batch_size` when it is positive. A provider whose escrow
/// can't be read is skipped until the next sweep rather than failing the rest.
async fn providers_due<T>(
    chain_client: &T,
    collector: Address,
    config: &EscrowReconcilerConfig,
) -> Result<Vec<(Address, Reason)>, ChainClientError>
where
    T: ChainClient + ManagerEscrowReader,
{
    let providers = chain_client.tracked_providers(collector).await?;
    let now = chain_client.latest_block_timestamp().await?;

    let mut due = Vec::new();
    let mut rebalances = Vec::new();
    for provider in providers {
        match chain_client.escrow_account(collector, provider).await {
            Ok(account) if thaw_finished(&account, now) => {
                due.push((provider, Reason::ThawFinished));
            }
            Ok(_) if rebalance_due(provider, now, config.interval, config.rebalance_interval) => {
                rebalances.push((provider, Reason::Rebalance));
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(%provider, error = %err, "failed to read provider escrow; skipping it this sweep");
            }
        }
    }
    due.extend(rebalances);

    if let Ok(limit) = usize::try_from(config.batch_size)
        && limit > 0
    {
        due.truncate(limit);
    }
    Ok(due)
}

/// Whether a thaw has finished, so `reconcileProvider` would withdraw it. Mirrors the
/// manager's own check (`thawEndTimestamp < block.timestamp`).
fn thaw_finished(account: &EscrowAccount, now: u64) -> bool {
    account.tokens_thawing > U256::ZERO && account.thaw_end_timestamp < U256::from(now)
}

/// Whether a rebalance for `provider` falls in this sweep. Each provider gets its own
/// offset into `rebalance` so the transactions spread out, and the slot follows chain
/// time rather than process memory, so a restart doesn't trigger a round of them.
fn rebalance_due(provider: Address, now: u64, sweep: Duration, rebalance: Duration) -> bool {
    let period = rebalance.as_secs();
    if period == 0 {
        return false;
    }
    let mut first_bytes = [0u8; 8];
    first_bytes.copy_from_slice(&provider.as_slice()[..8]);
    let offset = u64::from_be_bytes(first_bytes) % period;

    let this_sweep = now.saturating_add(offset) / period;
    let last_sweep = now.saturating_sub(sweep.as_secs()).saturating_add(offset) / period;
    this_sweep != last_sweep
}

/// Result of one sweep over the provider set.
enum Outcome {
    /// `rx_stop` fired mid-sweep; the caller should return.
    Stopped,
    /// The sweep finished; `ok`/`failed` count per-provider tx outcomes.
    Done { ok: u64, failed: u64 },
}

/// Call `reconcileProvider` once per distinct provider. One failed tx never aborts
/// the sweep: the next provider runs and the failed one is picked up again next sweep.
async fn reconcile_providers<T>(
    chain_client: &T,
    rx_stop: &mut mpsc::Receiver<()>,
    collector: Address,
    providers: Vec<(Address, Reason)>,
) -> Outcome
where
    T: ChainClient,
{
    let mut seen = std::collections::HashSet::new();
    let mut ok: u64 = 0;
    let mut failed: u64 = 0;

    for (provider, reason) in providers {
        if rx_stop.try_recv().is_ok() {
            tracing::debug!("escrow reconciler stopping mid-sweep");
            return Outcome::Stopped;
        }

        if !seen.insert(provider) {
            continue;
        }

        // A reconcile can wait seconds for its receipt; abandoning it on stop is safe
        // because reconciling is idempotent and the next run picks the provider up again.
        let result = tokio::select! {
            _ = rx_stop.recv() => {
                tracing::debug!("escrow reconciler stopping mid-reconcile");
                return Outcome::Stopped;
            }
            result = chain_client.reconcile_provider(collector, provider) => result,
        };

        match result {
            Ok(Some(tx_hash)) => {
                ok += 1;
                tracing::info!(
                    %provider,
                    ?reason,
                    %tx_hash,
                    "reconciled provider escrow"
                );
            }
            Ok(None) => {
                ok += 1;
                tracing::debug!(%provider, ?reason, "escrow reconciliation was a no-op for provider");
            }
            Err(err) => {
                failed += 1;
                tracing::warn!(
                    %provider,
                    ?reason,
                    error = %err,
                    "failed to reconcile provider escrow; will retry next sweep"
                );
            }
        }
    }

    Outcome::Done { ok, failed }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;
    use thegraph_core::alloy::primitives::B256;

    use super::*;

    const NOW: u64 = 1_800_000_000;

    /// A manager with fixed provider escrow, recording every `reconcile_provider` call.
    #[derive(Clone, Default)]
    struct FakeManager {
        escrow: Arc<HashMap<Address, Result<EscrowAccount, String>>>,
        order: Vec<Address>,
        reads: Arc<Mutex<u32>>,
        calls: Arc<Mutex<Vec<Address>>>,
        /// Never finish a reconcile, like one stuck waiting for its receipt.
        stall: bool,
    }

    impl FakeManager {
        fn with(providers: Vec<(Address, Result<EscrowAccount, String>)>) -> Self {
            Self {
                order: providers.iter().map(|(p, _)| *p).collect(),
                escrow: Arc::new(providers.into_iter().collect()),
                ..Default::default()
            }
        }

        fn calls(&self) -> Vec<Address> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ManagerEscrowReader for FakeManager {
        async fn tracked_providers(
            &self,
            _collector: Address,
        ) -> Result<Vec<Address>, ChainClientError> {
            *self.reads.lock().unwrap() += 1;
            Ok(self.order.clone())
        }

        async fn escrow_account(
            &self,
            _collector: Address,
            provider: Address,
        ) -> Result<EscrowAccount, ChainClientError> {
            self.escrow[&provider]
                .clone()
                .map_err(|err| ChainClientError::RpcError(anyhow::anyhow!(err)))
        }
    }

    #[async_trait]
    impl ChainClient for FakeManager {
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            Ok(NOW)
        }

        async fn offer_via_manager(
            &self,
            _rca: &dipper_rpc::indexer::indexer_client::sol::RecurringCollectionAgreement,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn cancel_via_manager(
            &self,
            _collector: Address,
            _agreement_id: &[u8; 16],
            _version_hash: B256,
            _options: u16,
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
        async fn reconcile_provider(
            &self,
            _collector: Address,
            provider: Address,
        ) -> Result<Option<B256>, ChainClientError> {
            self.calls.lock().unwrap().push(provider);
            if self.stall {
                std::future::pending::<()>().await;
            }
            Ok(Some(B256::ZERO))
        }
        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            unimplemented!()
        }
    }

    fn thawing_until(thaw_end: u64) -> Result<EscrowAccount, String> {
        Ok(EscrowAccount {
            balance: U256::from(100),
            tokens_thawing: U256::from(100),
            thaw_end_timestamp: U256::from(thaw_end),
        })
    }

    fn settled() -> Result<EscrowAccount, String> {
        Ok(EscrowAccount {
            balance: U256::from(100),
            tokens_thawing: U256::ZERO,
            thaw_end_timestamp: U256::ZERO,
        })
    }

    fn config(interval: Duration, rebalance: Duration, batch_size: i64) -> EscrowReconcilerConfig {
        EscrowReconcilerConfig {
            enabled: true,
            interval,
            batch_size,
            rebalance_interval: rebalance,
        }
    }

    fn no_rebalance() -> EscrowReconcilerConfig {
        config(Duration::from_secs(600), Duration::ZERO, 500)
    }

    fn agreement_conf() -> IndexingAgreementConfig {
        IndexingAgreementConfig {
            data_service: Address::ZERO,
            recurring_collector: Address::ZERO,
            recurring_agreement_manager: Address::ZERO,
            max_agreement_grt_per_30_days: 0.0,
            max_seconds_per_collection: 0,
            min_seconds_per_collection: 0,
            duration_seconds: 0,
            deadline_seconds: 0,
            max_grt_per_30_days: std::collections::BTreeMap::new(),
            max_grt_per_billion_entities_per_30_days: 0.0,
            declined_indexer_lookback_days: 0,
            price_rejection_lookback_days: 0,
            transient_rejection_lookback_minutes: 0,
            uncertain_rejection_lookback_days: 0,
            unresponsive_indexer_lookback_days: 0,
            mass_unresponsive_trip_fraction: 0.5,
            mass_unresponsive_reset_fraction: 0.25,
            dips_accepting_snapshot_max_age_hours: 48,
            dips_accepting_cache_ttl_seconds: 300,
            max_in_flight_offers_per_indexer: None,
            max_in_flight_offers_total: None,
        }
    }

    #[test]
    fn gate_runs_only_when_enabled() {
        assert!(should_run(Some(&no_rebalance()), &agreement_conf()));

        let disabled = EscrowReconcilerConfig {
            enabled: false,
            ..no_rebalance()
        };
        assert!(!should_run(Some(&disabled), &agreement_conf()));
        assert!(!should_run(None, &agreement_conf()));
    }

    #[tokio::test]
    async fn only_providers_whose_thaw_has_finished_are_reconciled() {
        //* Arrange
        let finished = Address::repeat_byte(0x11);
        let still_thawing = Address::repeat_byte(0x22);
        let nothing_thawing = Address::repeat_byte(0x33);
        let manager = FakeManager::with(vec![
            (finished, thawing_until(NOW - 1)),
            (still_thawing, thawing_until(NOW + 3_600)),
            (nothing_thawing, settled()),
        ]);

        //* Act
        let due = providers_due(&manager, Address::ZERO, &no_rebalance())
            .await
            .unwrap();

        //* Assert
        assert_eq!(due, vec![(finished, Reason::ThawFinished)]);
    }

    #[tokio::test]
    async fn an_unreadable_provider_does_not_stop_the_others() {
        let finished = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![
            (Address::repeat_byte(0x99), Err("rpc down".to_string())),
            (finished, thawing_until(NOW - 1)),
        ]);

        let due = providers_due(&manager, Address::ZERO, &no_rebalance())
            .await
            .unwrap();

        assert_eq!(due, vec![(finished, Reason::ThawFinished)]);
    }

    #[tokio::test]
    async fn finished_thaws_come_first_and_the_batch_caps_the_rest() {
        //* Arrange - rebalance every sweep, so every settled provider is due one
        let finished = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![
            (Address::repeat_byte(0x22), settled()),
            (Address::repeat_byte(0x33), settled()),
            (finished, thawing_until(NOW - 1)),
        ]);
        let every_sweep = config(Duration::from_secs(600), Duration::from_secs(60), 2);

        //* Act
        let due = providers_due(&manager, Address::ZERO, &every_sweep)
            .await
            .unwrap();

        //* Assert
        assert_eq!(due.len(), 2);
        assert_eq!(due[0], (finished, Reason::ThawFinished));
        assert_eq!(due[1].1, Reason::Rebalance);
    }

    #[test]
    fn each_provider_is_rebalanced_once_per_interval() {
        let sweep = Duration::from_secs(600);
        let day = Duration::from_secs(86_400);

        for provider in [Address::repeat_byte(0x01), Address::repeat_byte(0xfe)] {
            let sweeps_in_three_days = (0..3 * 144u64)
                .filter(|i| rebalance_due(provider, NOW + i * 600, sweep, day))
                .count();
            assert_eq!(sweeps_in_three_days, 3, "provider {provider}");
        }
        assert!(!rebalance_due(
            Address::repeat_byte(0x01),
            NOW,
            sweep,
            Duration::ZERO
        ));
    }

    #[tokio::test]
    async fn de_duplicates_providers_within_a_sweep() {
        let dup = Address::repeat_byte(0x44);
        let manager = FakeManager::default();
        let (_tx_stop, mut rx_stop) = mpsc::channel(1);

        let outcome = reconcile_providers(
            &manager,
            &mut rx_stop,
            Address::ZERO,
            vec![
                (dup, Reason::ThawFinished),
                (dup, Reason::Rebalance),
                (Address::repeat_byte(0x55), Reason::ThawFinished),
            ],
        )
        .await;

        assert!(matches!(outcome, Outcome::Done { ok: 2, failed: 0 }));
        assert_eq!(manager.calls().iter().filter(|c| **c == dup).count(), 1);
    }

    #[tokio::test]
    async fn a_stop_interrupts_a_reconcile_waiting_on_its_receipt() {
        //* Arrange
        let manager = FakeManager {
            stall: true,
            ..Default::default()
        };
        let (tx_stop, mut rx_stop) = mpsc::channel(1);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            tx_stop.send(()).await.unwrap();
        });

        //* Act - the stop arrives while the reconcile is in flight
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            reconcile_providers(
                &manager,
                &mut rx_stop,
                Address::ZERO,
                vec![(Address::repeat_byte(0x11), Reason::ThawFinished)],
            ),
        )
        .await;

        //* Assert - shutdown gives each service only a few seconds to stop
        assert!(matches!(outcome, Ok(Outcome::Stopped)));
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_sweep_waits_a_full_interval() {
        //* Arrange
        let finished = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![(finished, thawing_until(NOW - 1))]);
        let (handle, service) = new(Ctx {
            chain_client: manager.clone(),
            config: no_rebalance(),
            collector: Address::ZERO,
        });
        let service = tokio::spawn(service);

        //* Act + Assert - nothing at startup, a sweep once the interval has passed
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(manager.calls().is_empty(), "no sweep at startup");

        tokio::time::sleep(Duration::from_secs(600)).await;
        assert_eq!(manager.calls(), vec![finished]);

        handle.stop().await;
        service.await.unwrap().unwrap();
    }
}
