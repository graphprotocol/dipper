//! Keeps the RecurringAgreementManager's escrow tidy. The manager isn't told when an
//! agreement ends without a collection, so this sweep drops ended agreements, withdraws
//! finished thaws and rebalances each provider periodically, and only where needed.

use std::{collections::HashSet, future::Future, time::Duration};

use thegraph_core::alloy::primitives::{Address, U256};
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    chain_client::{
        ChainClient, ChainClientError, EscrowAccount, ManagerEscrowReader, TrackedProviders,
    },
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
            agreements_per_sweep = config.agreements_per_sweep,
            collector = %collector,
            "escrow reconciler service started"
        );

        // The first sweep waits a full interval, so a restart doesn't send a burst.
        let mut timer = tokio::time::interval_at(
            tokio::time::Instant::now() + config.interval,
            config.interval,
        );
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut schedule = RebalanceSchedule::default();
        let mut agreement_cursor: u64 = 0;

        loop {
            tokio::select! {
                _ = rx_stop.recv() => break,
                _ = timer.tick() => {},
            }

            // Each read below takes a few RPC calls per provider or agreement, so let a
            // stop request cut them short rather than hold up shutdown.
            let tracked = tokio::select! {
                _ = rx_stop.recv() => break,
                tracked = chain_client.tracked_providers(collector) => tracked,
            };
            let tracked = match tracked {
                Ok(tracked) => tracked,
                Err(err) => {
                    tracing::error!(error = %err, "failed to list the providers the manager tracks");
                    continue;
                }
            };

            // Releases and provider reconciles share one `batch_size`, releases first.
            let budget = sweep_budget(config.batch_size);
            let ended = tokio::select! {
                _ = rx_stop.recv() => break,
                ended = ended_agreements(
                    &chain_client,
                    collector,
                    &tracked.providers,
                    &mut agreement_cursor,
                    config.agreements_per_sweep,
                    budget,
                ) => ended,
            };
            let left = budget.map(|max| max.saturating_sub(ended.len()));
            if !ended.is_empty() {
                match release_agreements(&chain_client, &mut rx_stop, collector, ended).await {
                    Outcome::Stopped => return Ok(()),
                    Outcome::Done { succeeded, failed } => {
                        tracing::info!(
                            released = succeeded.len(),
                            failed = failed.len(),
                            "released ended agreements"
                        );
                    }
                }
            }

            if left == Some(0) {
                tracing::info!(
                    "escrow reconciliation: releases used this sweep's batch_size; provider reconciles wait for a later sweep"
                );
                continue;
            }

            let read = tokio::select! {
                _ = rx_stop.recv() => break,
                read = providers_due(&chain_client, collector, &tracked, &config, left, &mut schedule) => read,
            };
            let due = match read {
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
                Outcome::Done { succeeded, failed } => {
                    tracing::info!(
                        reconciled = succeeded.len(),
                        failed = failed.len(),
                        "escrow reconciliation sweep completed"
                    );
                    schedule.settle(&succeeded, &failed);
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

/// Which providers are owed a rebalance. Each sweep hands out the slots in the chain
/// time since the previous one, so a late or failed sweep doesn't skip any, and a
/// rebalance that couldn't be sent stays owed until one goes through.
#[derive(Debug, Default)]
struct RebalanceSchedule {
    /// Chain time up to which rebalance slots have been handed out.
    covered_until: Option<u64>,
    /// Providers whose rebalance came due but hasn't gone through yet.
    owed: HashSet<Address>,
}

impl RebalanceSchedule {
    /// The chain-time window this sweep hands out slots for. The first sweep looks back
    /// one `interval`; a head older than the last one seen (a lagging RPC endpoint)
    /// covers nothing new.
    fn window(&self, now: u64, interval: Duration) -> (u64, u64) {
        let from = self
            .covered_until
            .unwrap_or_else(|| now.saturating_sub(interval.as_secs()));
        (from, now.max(from))
    }

    /// Record a sweep's reconciles: each one rebalances the provider in full, so a
    /// success settles what it owed, and a failure leaves it owed.
    fn settle(&mut self, succeeded: &[Address], failed: &[Address]) {
        for provider in succeeded {
            self.owed.remove(provider);
        }
        self.owed.extend(failed.iter().copied());
    }
}

/// Providers to reconcile this sweep: those whose thaw has finished first, then those
/// owed a rebalance, at most `limit` of them. A provider whose escrow can't be read is
/// skipped this sweep, and keeps any rebalance it is owed.
async fn providers_due<T>(
    chain_client: &T,
    collector: Address,
    tracked: &TrackedProviders,
    config: &EscrowReconcilerConfig,
    limit: Option<usize>,
    schedule: &mut RebalanceSchedule,
) -> Result<Vec<(Address, Reason)>, ChainClientError>
where
    T: ChainClient + ManagerEscrowReader,
{
    let TrackedProviders {
        providers,
        complete,
    } = tracked;
    let now = chain_client.latest_block_timestamp().await?;
    let (from, to) = schedule.window(now, config.interval);
    if *complete {
        let tracked: HashSet<Address> = providers.iter().copied().collect();
        schedule.owed.retain(|provider| tracked.contains(provider));
    }

    let mut due = Vec::new();
    let mut rebalances = Vec::new();
    for &provider in providers {
        if rebalance_due(provider, from, to, config.rebalance_interval) {
            schedule.owed.insert(provider);
        }
        match chain_client.escrow_account(collector, provider).await {
            Ok(account) if thaw_finished(&account, now) => {
                due.push((provider, Reason::ThawFinished));
            }
            Ok(_) if schedule.owed.contains(&provider) => {
                rebalances.push((provider, Reason::Rebalance));
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(%provider, error = %err, "failed to read provider escrow; skipping it this sweep");
            }
        }
    }
    // A short list may have missed a provider whose slot is in this window, so cover it
    // again next sweep; a provider listed now may then get 1 extra rebalance.
    if *complete {
        schedule.covered_until = Some(to);
    }

    due.extend(rebalances);
    let deferred = cap(&mut due, limit);
    if deferred > 0 {
        tracing::info!(
            deferred,
            "escrow reconciliation reached batch_size; the rest wait for a later sweep"
        );
    }
    Ok(due)
}

/// The most transactions a sweep may send: `batch_size` when it is positive, else no limit.
fn sweep_budget(batch_size: i64) -> Option<usize> {
    usize::try_from(batch_size).ok().filter(|&limit| limit > 0)
}

/// Keep at most `limit` items, returning how many were cut.
fn cap<T>(items: &mut Vec<T>, limit: Option<usize>) -> usize {
    let before = items.len();
    if let Some(limit) = limit {
        items.truncate(limit);
    }
    before - items.len()
}

/// Whether a thaw has finished, so `reconcileProvider` would withdraw it. Mirrors the
/// manager's own check (`thawEndTimestamp < block.timestamp`).
fn thaw_finished(account: &EscrowAccount, now: u64) -> bool {
    account.tokens_thawing > U256::ZERO && account.thaw_end_timestamp < U256::from(now)
}

/// Whether `provider`'s once-per-`rebalance` slot falls in the chain-time window
/// `(from, to]`. Each provider gets its own offset into the interval, so the
/// transactions spread out rather than landing in one sweep.
fn rebalance_due(provider: Address, from: u64, to: u64, rebalance: Duration) -> bool {
    let period = rebalance.as_secs();
    if period == 0 || to <= from {
        return false;
    }
    let mut first_bytes = [0u8; 8];
    first_bytes.copy_from_slice(&provider.as_slice()[..8]);
    let offset = u64::from_be_bytes(first_bytes) % period;

    to.saturating_add(offset) / period != from.saturating_add(offset) / period
}

/// Result of one round of reconciles, over providers or agreements.
enum Outcome<T> {
    /// `rx_stop` fired mid-round; the caller should return.
    Stopped,
    /// The round finished; the items whose reconcile went through or failed.
    Done { succeeded: Vec<T>, failed: Vec<T> },
}

/// Agreements the manager still counts against a provider although the collector says
/// nothing more can be claimed on them. Checks up to `limit` agreements from `cursor`,
/// stopping once `max_ended` are found; the rest, and any unreadable, wait for later sweeps.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn ended_agreements<T>(
    chain_client: &T,
    collector: Address,
    providers: &[Address],
    cursor: &mut u64,
    limit: u64,
    max_ended: Option<usize>,
) -> Vec<[u8; 16]>
where
    T: ManagerEscrowReader,
{
    let mut counts = Vec::with_capacity(providers.len());
    for &provider in providers {
        match chain_client
            .tracked_agreement_count(collector, provider)
            .await
        {
            Ok(count) => counts.push((provider, count)),
            Err(err) => {
                tracing::warn!(%provider, error = %err, "failed to count provider agreements; skipping it this sweep");
            }
        }
    }
    let total: u64 = counts.iter().map(|(_, count)| count).sum();
    if total == 0 || limit == 0 {
        return Vec::new();
    }

    let start = *cursor % total;
    let checks = limit.min(total);

    let mut ended = Vec::new();
    let mut seen = HashSet::new();
    let mut checked = 0;
    while checked < checks {
        if max_ended.is_some_and(|max| ended.len() >= max) {
            tracing::info!(
                checked,
                found = ended.len(),
                "found as many ended agreements as this sweep can release; the rest are checked later"
            );
            break;
        }
        let (provider, index) = locate(&counts, (start + checked) % total);
        checked += 1;
        let id = match chain_client
            .tracked_agreement_at(collector, provider, index)
            .await
        {
            Ok(id) => id,
            Err(err) => {
                tracing::warn!(%provider, index, error = %err, "failed to read a tracked agreement");
                continue;
            }
        };
        // The list can shift between reads, so the same agreement can turn up twice.
        if !seen.insert(id) {
            continue;
        }
        match chain_client.max_next_claim(collector, &id).await {
            Ok(claim) if claim.is_zero() => ended.push(id),
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(agreement_id = %hex_id(&id), error = %err, "failed to read an agreement's remaining claim");
            }
        }
    }
    *cursor = (start + checked) % total;
    ended
}

/// The provider and index for position `pos` when the providers' agreements are laid
/// end to end. `pos` must be below the sum of the counts.
fn locate(counts: &[(Address, u64)], mut pos: u64) -> (Address, u64) {
    for &(provider, count) in counts {
        if pos < count {
            return (provider, pos);
        }
        pos -= count;
    }
    unreachable!("position beyond the agreements counted")
}

fn hex_id(id: &[u8; 16]) -> String {
    format!(
        "0x{}",
        id.iter().map(|b| format!("{b:02x}")).collect::<String>()
    )
}

/// Call `reconcileAgreement` for each ended agreement, so the manager drops it and starts
/// releasing its escrow. One failure never aborts the rest; a later sweep finds it again.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn release_agreements<T>(
    chain_client: &T,
    rx_stop: &mut mpsc::Receiver<()>,
    collector: Address,
    agreements: Vec<[u8; 16]>,
) -> Outcome<[u8; 16]>
where
    T: ChainClient,
{
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

    for id in agreements {
        let result = tokio::select! {
            _ = rx_stop.recv() => {
                tracing::debug!("escrow reconciler stopping mid-release");
                return Outcome::Stopped;
            }
            result = chain_client.reconcile_agreement(collector, &id) => result,
        };

        match result {
            Ok(tx_hash) => {
                succeeded.push(id);
                tracing::info!(agreement_id = %hex_id(&id), ?tx_hash, "released ended agreement");
            }
            Err(err) => {
                failed.push(id);
                tracing::warn!(
                    agreement_id = %hex_id(&id),
                    error = %err,
                    "failed to release ended agreement; a later sweep will try again"
                );
            }
        }
    }

    Outcome::Done { succeeded, failed }
}

/// Call `reconcileProvider` once per distinct provider. One failed tx never aborts
/// the sweep: the next provider runs and the failed one is picked up by a later sweep.
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
async fn reconcile_providers<T>(
    chain_client: &T,
    rx_stop: &mut mpsc::Receiver<()>,
    collector: Address,
    providers: Vec<(Address, Reason)>,
) -> Outcome<Address>
where
    T: ChainClient,
{
    let mut seen = HashSet::new();
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();

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
                succeeded.push(provider);
                tracing::info!(
                    %provider,
                    ?reason,
                    %tx_hash,
                    "reconciled provider escrow"
                );
            }
            Ok(None) => {
                succeeded.push(provider);
                tracing::debug!(%provider, ?reason, "escrow reconciliation was a no-op for provider");
            }
            Err(err) => {
                failed.push(provider);
                tracing::warn!(
                    %provider,
                    ?reason,
                    error = %err,
                    "failed to reconcile provider escrow; a later sweep will try again"
                );
            }
        }
    }

    Outcome::Done { succeeded, failed }
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
    use crate::chain_client::AgreementOnChain;

    const NOW: u64 = 1_800_000_000;

    /// A manager with fixed provider escrow and agreements, recording every reconcile.
    #[derive(Clone, Default)]
    struct FakeManager {
        escrow: Arc<HashMap<Address, Result<EscrowAccount, String>>>,
        order: Vec<Address>,
        reads: Arc<Mutex<u32>>,
        calls: Arc<Mutex<Vec<Address>>>,
        /// Never finish a reconcile, like one stuck waiting for its receipt.
        stall: bool,
        /// Never answer the provider list, like a hung RPC endpoint.
        stall_reads: bool,
        /// Report the provider list as cut short.
        incomplete: bool,
        agreements: Arc<HashMap<Address, Vec<[u8; 16]>>>,
        /// What the collector says is left to claim; missing means the read fails.
        claims: Arc<HashMap<[u8; 16], U256>>,
        released: Arc<Mutex<Vec<[u8; 16]>>>,
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

        /// The same manager, also tracking `agreements` with these remaining claims.
        fn tracking(mut self, agreements: Vec<(Address, [u8; 16], u64)>) -> Self {
            let mut by_provider: HashMap<Address, Vec<[u8; 16]>> = HashMap::new();
            let mut claims = HashMap::new();
            for (provider, id, claim) in agreements {
                by_provider.entry(provider).or_default().push(id);
                claims.insert(id, U256::from(claim));
            }
            self.agreements = Arc::new(by_provider);
            self.claims = Arc::new(claims);
            self
        }

        fn released(&self) -> Vec<[u8; 16]> {
            self.released.lock().unwrap().clone()
        }

        /// What a read of the provider list returns.
        fn listed(&self) -> TrackedProviders {
            TrackedProviders {
                providers: self.order.clone(),
                complete: !self.incomplete,
            }
        }
    }

    #[async_trait]
    impl ManagerEscrowReader for FakeManager {
        async fn tracked_providers(
            &self,
            _collector: Address,
        ) -> Result<TrackedProviders, ChainClientError> {
            *self.reads.lock().unwrap() += 1;
            if self.stall_reads {
                std::future::pending::<()>().await;
            }
            Ok(self.listed())
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

        async fn tracked_agreement_count(
            &self,
            _collector: Address,
            provider: Address,
        ) -> Result<u64, ChainClientError> {
            Ok(self
                .agreements
                .get(&provider)
                .map_or(0, |ids| ids.len() as u64))
        }

        #[expect(
            clippy::cast_possible_truncation,
            reason = "predates this lint; fix when next touched"
        )]
        async fn tracked_agreement_at(
            &self,
            _collector: Address,
            provider: Address,
            index: u64,
        ) -> Result<[u8; 16], ChainClientError> {
            Ok(self.agreements[&provider][index as usize])
        }

        async fn max_next_claim(
            &self,
            _collector: Address,
            agreement_id: &[u8; 16],
        ) -> Result<U256, ChainClientError> {
            self.claims
                .get(agreement_id)
                .copied()
                .ok_or_else(|| ChainClientError::RpcError(anyhow::anyhow!("claim read failed")))
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
        async fn agreement_on_chain(
            &self,
            _agreement_id: &[u8; 16],
        ) -> Result<AgreementOnChain, ChainClientError> {
            unimplemented!()
        }
        async fn reconcile_agreement(
            &self,
            _collector: Address,
            agreement_id: &[u8; 16],
        ) -> Result<Option<B256>, ChainClientError> {
            self.released.lock().unwrap().push(*agreement_id);
            Ok(Some(B256::ZERO))
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
            agreements_per_sweep: 500,
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
        let due = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &no_rebalance(),
            None,
            &mut RebalanceSchedule::default(),
        )
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

        let due = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &no_rebalance(),
            None,
            &mut RebalanceSchedule::default(),
        )
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
        let due = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &every_sweep,
            Some(2),
            &mut RebalanceSchedule::default(),
        )
        .await
        .unwrap();

        //* Assert
        assert_eq!(due.len(), 2);
        assert_eq!(due[0], (finished, Reason::ThawFinished));
        assert_eq!(due[1].1, Reason::Rebalance);
    }

    #[test]
    fn each_provider_is_rebalanced_once_per_interval_however_sweeps_are_spaced() {
        let day = Duration::from_secs(86_400);
        // Sweeps late, early and skipped, as a slow RPC or a busy worker would leave them.
        let gaps = [600, 1_300, 45, 2_000, 600, 7_200, 600, 30];

        for provider in [Address::repeat_byte(0x01), Address::repeat_byte(0xfe)] {
            let (mut from, mut slots) = (NOW, 0);
            for gap in gaps.iter().cycle() {
                let to = from + gap;
                if to > NOW + 3 * 86_400 {
                    break;
                }
                slots += usize::from(rebalance_due(provider, from, to, day));
                from = to;
            }
            assert_eq!(slots, 3, "provider {provider}");
        }
        assert!(!rebalance_due(
            Address::repeat_byte(0x01),
            NOW,
            NOW + 86_400,
            Duration::ZERO
        ));
    }

    #[tokio::test]
    async fn a_rebalance_that_failed_is_sent_again_next_sweep() {
        //* Arrange - a rebalance falls due in the first sweep and its transaction fails
        let provider = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![(provider, settled())]);
        let config = config(Duration::from_secs(600), Duration::from_secs(600), 500);
        let mut schedule = RebalanceSchedule::default();
        let first = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &config,
            None,
            &mut schedule,
        )
        .await
        .unwrap();
        assert_eq!(first, vec![(provider, Reason::Rebalance)]);
        schedule.settle(&[], &[provider]);

        //* Act - the next sweep covers no new slot, the clock not having moved
        let second = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &config,
            None,
            &mut schedule,
        )
        .await
        .unwrap();

        //* Assert - still owed until a reconcile goes through
        assert_eq!(second, vec![(provider, Reason::Rebalance)]);
        schedule.settle(&[provider], &[]);
        let third = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &config,
            None,
            &mut schedule,
        )
        .await
        .unwrap();
        assert!(third.is_empty());
    }

    #[tokio::test]
    async fn a_short_provider_list_keeps_what_a_missing_provider_is_owed() {
        //* Arrange - `a` is owed a rebalance, then a read of the list stops before it
        let (a, b) = (Address::repeat_byte(0x11), Address::repeat_byte(0x22));
        let mut manager = FakeManager::with(vec![(b, settled()), (a, settled())]);
        let config = no_rebalance();
        let mut schedule = RebalanceSchedule {
            covered_until: Some(NOW),
            owed: HashSet::from([a]),
        };
        manager.order = vec![b];
        manager.incomplete = true;
        providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &config,
            None,
            &mut schedule,
        )
        .await
        .unwrap();

        //* Act - the next read gets the whole list
        manager.order = vec![b, a];
        manager.incomplete = false;
        let due = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &config,
            None,
            &mut schedule,
        )
        .await
        .unwrap();

        //* Assert
        assert_eq!(due, vec![(a, Reason::Rebalance)]);
    }

    #[tokio::test]
    async fn a_short_provider_list_leaves_its_window_for_the_next_sweep() {
        //* Arrange - every provider's slot falls in this window, but the read misses `a`
        let (a, b) = (Address::repeat_byte(0x11), Address::repeat_byte(0x22));
        let mut manager = FakeManager::with(vec![(b, settled()), (a, settled())]);
        let config = config(Duration::from_secs(600), Duration::from_secs(600), 500);
        let mut schedule = RebalanceSchedule {
            covered_until: Some(NOW - 600),
            owed: HashSet::new(),
        };
        manager.order = vec![b];
        manager.incomplete = true;
        let short = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &config,
            None,
            &mut schedule,
        )
        .await
        .unwrap();
        assert_eq!(short, vec![(b, Reason::Rebalance)]);
        schedule.settle(&[b], &[]);

        //* Act - the next read gets the whole list
        manager.order = vec![b, a];
        manager.incomplete = false;
        let due = providers_due(
            &manager,
            Address::ZERO,
            &manager.listed(),
            &config,
            None,
            &mut schedule,
        )
        .await
        .unwrap();

        //* Assert - `a` gets its slot, and `b` an extra rebalance
        assert_eq!(due, vec![(b, Reason::Rebalance), (a, Reason::Rebalance)]);
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

        assert!(
            matches!(&outcome, Outcome::Done { succeeded, failed } if succeeded.len() == 2 && failed.is_empty())
        );
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

    #[tokio::test(start_paused = true)]
    async fn a_stop_interrupts_a_sweep_still_reading_the_manager() {
        //* Arrange - the sweep starts, then hangs reading the provider list
        let manager = FakeManager {
            stall_reads: true,
            ..Default::default()
        };
        let (handle, service) = new(Ctx {
            chain_client: manager.clone(),
            config: no_rebalance(),
            collector: Address::ZERO,
        });
        let service = tokio::spawn(service);
        tokio::time::sleep(Duration::from_secs(601)).await;
        assert_eq!(*manager.reads.lock().unwrap(), 1, "the sweep is reading");

        //* Act
        let stopped = tokio::time::timeout(Duration::from_secs(5), handle.stop()).await;

        //* Assert - shutdown gives each service only a few seconds to stop
        assert!(stopped.is_ok(), "the reconciler ignored the stop");
        service.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn only_agreements_with_nothing_left_to_claim_are_released() {
        //* Arrange
        let provider = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![(provider, settled())]).tracking(vec![
            (provider, [0xe0; 16], 0),
            (provider, [0xa1; 16], 18_667),
        ]);
        let mut cursor = 0;

        //* Act
        let ended = ended_agreements(
            &manager,
            Address::ZERO,
            &manager.order,
            &mut cursor,
            500,
            None,
        )
        .await;

        //* Assert
        assert_eq!(ended, vec![[0xe0; 16]]);
    }

    #[tokio::test]
    async fn a_failed_claim_read_leaves_the_agreement_for_later() {
        let provider = Address::repeat_byte(0x11);
        let mut manager = FakeManager::with(vec![(provider, settled())])
            .tracking(vec![(provider, [0xe0; 16], 0), (provider, [0xe1; 16], 0)]);
        Arc::make_mut(&mut manager.claims).remove(&[0xe0; 16]);
        let mut cursor = 0;

        let ended = ended_agreements(
            &manager,
            Address::ZERO,
            &manager.order,
            &mut cursor,
            500,
            None,
        )
        .await;

        assert_eq!(ended, vec![[0xe1; 16]]);
    }

    /// The manager's list can shift between reads, so the same agreement can turn up twice.
    #[tokio::test]
    async fn an_agreement_read_twice_is_released_once() {
        let provider = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![(provider, settled())])
            .tracking(vec![(provider, [0xe0; 16], 0), (provider, [0xe0; 16], 0)]);
        let mut cursor = 0;

        let ended = ended_agreements(
            &manager,
            Address::ZERO,
            &manager.order,
            &mut cursor,
            500,
            None,
        )
        .await;

        assert_eq!(ended, vec![[0xe0; 16]]);
    }

    #[tokio::test]
    async fn checks_carry_on_where_the_last_sweep_stopped() {
        //* Arrange - 5 ended agreements across 2 providers, 2 checked per sweep
        let (a, b) = (Address::repeat_byte(0x11), Address::repeat_byte(0x22));
        let manager = FakeManager::with(vec![(a, settled()), (b, settled())]).tracking(vec![
            (a, [1; 16], 0),
            (a, [2; 16], 0),
            (a, [3; 16], 0),
            (b, [4; 16], 0),
            (b, [5; 16], 0),
        ]);
        let mut cursor = 0;

        //* Act
        let mut seen = Vec::new();
        for _ in 0..3 {
            seen.extend(
                ended_agreements(
                    &manager,
                    Address::ZERO,
                    &manager.order,
                    &mut cursor,
                    2,
                    None,
                )
                .await,
            );
        }

        //* Assert - every agreement checked once, then the cursor wraps round
        seen.sort();
        assert_eq!(
            seen,
            vec![[1; 16], [1; 16], [2; 16], [3; 16], [4; 16], [5; 16]]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_sweep_releases_ended_agreements() {
        //* Arrange
        let provider = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![(provider, settled())])
            .tracking(vec![(provider, [0xe0; 16], 0), (provider, [0xa1; 16], 5)]);
        let (handle, service) = new(Ctx {
            chain_client: manager.clone(),
            config: no_rebalance(),
            collector: Address::ZERO,
        });
        let service = tokio::spawn(service);

        //* Act
        tokio::time::sleep(Duration::from_secs(601)).await;
        handle.stop().await;
        service.await.unwrap().unwrap();

        //* Assert
        assert_eq!(manager.released(), vec![[0xe0; 16]]);
    }

    #[tokio::test(start_paused = true)]
    async fn ended_agreements_past_the_batch_are_checked_by_the_next_sweep() {
        //* Arrange - 5 ended agreements, and room to release 2 per sweep
        let provider = Address::repeat_byte(0x11);
        let manager = FakeManager::with(vec![(provider, settled())])
            .tracking((1..=5).map(|n| (provider, [n; 16], 0)).collect());
        let (handle, service) = new(Ctx {
            chain_client: manager.clone(),
            config: config(Duration::from_secs(600), Duration::ZERO, 2),
            collector: Address::ZERO,
        });
        let service = tokio::spawn(service);

        //* Act - 2 sweeps
        tokio::time::sleep(Duration::from_secs(1_201)).await;
        handle.stop().await;
        service.await.unwrap().unwrap();

        //* Assert - the second sweep starts at the first agreement the first one left
        assert_eq!(manager.released(), vec![[1; 16], [2; 16], [3; 16], [4; 16]]);
    }

    /// Runs 1 sweep over 2 providers whose thaws have finished, the first also tracking an
    /// ended agreement, and returns what was released and which providers were reconciled.
    async fn one_sweep_with_batch_size(batch_size: i64) -> (Vec<[u8; 16]>, Vec<Address>) {
        let (a, b) = (Address::repeat_byte(0x11), Address::repeat_byte(0x22));
        let manager = FakeManager::with(vec![
            (a, thawing_until(NOW - 1)),
            (b, thawing_until(NOW - 1)),
        ])
        .tracking(vec![(a, [0xe0; 16], 0)]);
        let (handle, service) = new(Ctx {
            chain_client: manager.clone(),
            config: config(Duration::from_secs(600), Duration::ZERO, batch_size),
            collector: Address::ZERO,
        });
        let service = tokio::spawn(service);
        tokio::time::sleep(Duration::from_secs(601)).await;
        handle.stop().await;
        service.await.unwrap().unwrap();
        (manager.released(), manager.calls())
    }

    #[tokio::test(start_paused = true)]
    async fn releases_and_provider_reconciles_share_one_batch() {
        let (released, reconciled) = one_sweep_with_batch_size(2).await;

        assert_eq!(released, vec![[0xe0; 16]]);
        assert_eq!(reconciled, vec![Address::repeat_byte(0x11)]);
    }

    #[tokio::test(start_paused = true)]
    async fn releases_that_fill_the_batch_leave_providers_for_a_later_sweep() {
        let (released, reconciled) = one_sweep_with_batch_size(1).await;

        assert_eq!(released, vec![[0xe0; 16]]);
        assert!(reconciled.is_empty(), "reconciled {reconciled:?}");
    }
}
