//! Reclaims orphaned protocol-managed escrow on-chain. In `AgreementManager`
//! mode the manager self-reconciles only on collection; this driver sweeps the
//! distinct providers it skips (ended/canceled/mid-thaw) into `reconcileProvider`.

use std::{future::Future, time::Duration};

use thegraph_core::alloy::primitives::Address;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    chain_client::ChainClient,
    config::{EscrowReconcilerConfig, IndexingAgreementConfig},
    registry::AgreementRegistry,
};

/// Whether the reconciler should run. The manager self-reconciles only on
/// collection, so this driver sweeps the providers it skips when enabled.
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
pub struct Ctx<R, T> {
    /// Registry for enumerating providers with reconcilable escrow
    pub registry: R,
    /// Chain client used to call `reconcileProvider` on the manager
    pub chain_client: T,
    /// Service configuration
    pub config: EscrowReconcilerConfig,
    /// The RecurringCollector address passed as the manager's `collector` arg
    pub collector: Address,
}

/// Create a new escrow reconciler service. Returns a handle plus a future to
/// spawn. Callers must only construct this in `AgreementManager` mode; the
/// service itself is mode-agnostic.
pub fn new<R, T>(ctx: Ctx<R, T>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: AgreementRegistry + Send + Sync,
    T: ChainClient + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        chain_client,
        config,
        collector,
    } = ctx;

    let service = async move {
        tracing::info!(
            interval_secs = config.interval.as_secs(),
            batch_size = config.batch_size,
            collector = %collector,
            "escrow reconciler service started"
        );

        let mut timer = tokio::time::interval(config.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        const DB_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            let providers = match tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.get_providers_for_escrow_reconciliation(config.batch_size),
            )
            .await
            {
                Ok(Ok(providers)) => providers,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to query providers for escrow reconciliation");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout querying providers for escrow reconciliation");
                    continue;
                }
            };

            if providers.is_empty() {
                tracing::debug!("escrow reconciliation: no providers to reconcile");
                continue;
            }

            let reconciled =
                reconcile_providers(&chain_client, &mut rx_stop, collector, providers).await;

            match reconciled {
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

/// Result of one sweep over the provider set.
enum Outcome {
    /// `rx_stop` fired mid-sweep; the caller should return.
    Stopped,
    /// The sweep finished; `ok`/`failed` count per-provider tx outcomes.
    Done { ok: u64, failed: u64 },
}

/// Call `reconcileProvider` once per distinct provider. One failed tx never
/// aborts the sweep — the next provider runs and the failed one retries next
/// tick (reconcile is idempotent). One tx per provider; `batch_size` bounds it.
async fn reconcile_providers<T>(
    chain_client: &T,
    rx_stop: &mut mpsc::Receiver<()>,
    collector: Address,
    providers: Vec<Address>,
) -> Outcome
where
    T: ChainClient,
{
    let mut seen = std::collections::HashSet::new();
    let mut ok: u64 = 0;
    let mut failed: u64 = 0;

    for provider in providers {
        if rx_stop.try_recv().is_ok() {
            tracing::debug!("escrow reconciler stopping mid-sweep");
            return Outcome::Stopped;
        }

        // The query already returns distinct rows; this guards against a
        // future change widening it without de-duplicating.
        if !seen.insert(provider) {
            continue;
        }

        match chain_client.reconcile_provider(collector, provider).await {
            Ok(Some(tx_hash)) => {
                ok += 1;
                tracing::info!(
                    %provider,
                    %tx_hash,
                    "submitted escrow reconciliation for provider"
                );
            }
            Ok(None) => {
                ok += 1;
                tracing::debug!(%provider, "escrow reconciliation was a no-op for provider");
            }
            Err(err) => {
                failed += 1;
                tracing::warn!(
                    %provider,
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
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use thegraph_core::alloy::primitives::B256;

    use super::*;
    use crate::{chain_client::ChainClientError, registry::StubAgreementRegistry};

    /// Records every `reconcile_provider` call so tests can assert the driver
    /// fired once per distinct provider.
    #[derive(Clone, Default)]
    struct RecordingChainClient {
        calls: Arc<Mutex<Vec<Address>>>,
    }

    #[async_trait]
    impl ChainClient for RecordingChainClient {
        async fn latest_block_timestamp(&self) -> Result<u64, ChainClientError> {
            // Err by default so a test must mock this explicitly to take the
            // live-chain-head path instead of silently reading timestamp 0.
            Err(ChainClientError::RpcError(anyhow::anyhow!(
                "latest_block_timestamp not mocked"
            )))
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
            Ok(Some(B256::ZERO))
        }
        async fn agreement_still_active(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<bool, ChainClientError> {
            unimplemented!()
        }

        async fn fetch_agreement_version_hash(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            unimplemented!()
        }
    }

    /// In-memory registry returning a fixed provider list.
    #[derive(Clone)]
    struct MockRegistry {
        providers: Vec<Address>,
        calls: Arc<Mutex<Vec<i64>>>,
    }

    impl MockRegistry {
        fn new(providers: Vec<Address>) -> Self {
            Self {
                providers,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl StubAgreementRegistry for MockRegistry {
        async fn get_unresponsive_indexers(
            &self,
            _lookback_days: i32,
            _chain_id: thegraph_core::alloy::primitives::ChainId,
        ) -> crate::registry::Result<Vec<thegraph_core::IndexerId>> {
            Ok(vec![])
        }
        async fn get_providers_for_escrow_reconciliation(
            &self,
            limit: i64,
        ) -> crate::registry::Result<Vec<Address>> {
            self.calls.lock().unwrap().push(limit);
            Ok(self.providers.clone())
        }
    }

    fn config(interval_ms: u64) -> EscrowReconcilerConfig {
        EscrowReconcilerConfig {
            enabled: true,
            interval: Duration::from_millis(interval_ms),
            batch_size: 500,
        }
    }

    fn agreement_conf(manager: Address) -> IndexingAgreementConfig {
        IndexingAgreementConfig {
            data_service: Address::ZERO,
            recurring_collector: Address::ZERO,
            recurring_agreement_manager: manager,
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
        let manager = Address::repeat_byte(0x77);

        // Enabled config runs; disabled or absent config never does.
        assert!(should_run(Some(&config(10)), &agreement_conf(manager)));

        let disabled = EscrowReconcilerConfig {
            enabled: false,
            ..config(10)
        };
        assert!(!should_run(Some(&disabled), &agreement_conf(manager)));
        assert!(!should_run(None, &agreement_conf(manager)));
    }

    #[tokio::test]
    async fn reconciles_once_per_distinct_provider() {
        // Arrange: three distinct providers in the registry.
        let providers = vec![
            Address::repeat_byte(0x11),
            Address::repeat_byte(0x22),
            Address::repeat_byte(0x33),
        ];
        let registry = MockRegistry::new(providers.clone());
        let chain = RecordingChainClient::default();
        let collector = Address::repeat_byte(0xaa);

        let ctx = Ctx {
            registry,
            chain_client: chain.clone(),
            config: config(10),
            collector,
        };

        // Act: run one sweep, then stop.
        let (handle, service) = new(ctx);
        let svc = tokio::spawn(service);
        tokio::time::sleep(Duration::from_millis(40)).await;
        handle.stop().await;
        svc.await.unwrap().unwrap();

        // Assert: each provider reconciled at least once, never an unknown one.
        let calls = chain.calls.lock().unwrap();
        for provider in &providers {
            assert!(
                calls.contains(provider),
                "expected reconcile_provider call for {provider}"
            );
        }
        assert!(
            calls.iter().all(|c| providers.contains(c)),
            "reconcile_provider called for an address not in the provider set"
        );
    }

    #[tokio::test]
    async fn de_duplicates_providers_within_a_sweep() {
        // Arrange: a duplicate provider in one batch. Drive the sweep helper
        // directly so the assertion is scoped to one sweep, not the timer.
        let dup = Address::repeat_byte(0x44);
        let chain = RecordingChainClient::default();
        let (_tx_stop, mut rx_stop) = mpsc::channel(1);

        // Act: one sweep over a batch that repeats `dup`.
        let outcome = reconcile_providers(
            &chain,
            &mut rx_stop,
            Address::ZERO,
            vec![dup, dup, Address::repeat_byte(0x55)],
        )
        .await;

        // Assert: the duplicate provider was reconciled exactly once.
        assert!(matches!(outcome, Outcome::Done { ok: 2, failed: 0 }));
        let calls = chain.calls.lock().unwrap();
        let dup_count = calls.iter().filter(|c| **c == dup).count();
        assert_eq!(
            dup_count, 1,
            "duplicate provider must be reconciled once per sweep"
        );
    }

    #[tokio::test]
    async fn no_op_when_no_providers() {
        // Arrange: empty provider set (the ExternalPayer steady state, and the
        // AgreementManager idle state, both look like this to the driver).
        let registry = MockRegistry::new(vec![]);
        let chain = RecordingChainClient::default();

        let ctx = Ctx {
            registry,
            chain_client: chain.clone(),
            config: config(10),
            collector: Address::ZERO,
        };

        // Act.
        let (handle, service) = new(ctx);
        let svc = tokio::spawn(service);
        tokio::time::sleep(Duration::from_millis(30)).await;
        handle.stop().await;
        svc.await.unwrap().unwrap();

        // Assert: no reconcile_provider tx was ever sent.
        assert!(
            chain.calls.lock().unwrap().is_empty(),
            "no providers means no reconcile_provider calls"
        );
    }
}
