//! DIPs info watcher registry trait.

use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use thegraph_core::IndexerId;

use super::result::Result as RegistryResult;

/// What recording a cycle of DIPs info checks left: how long each indexer still missing its
/// DIPs info has gone without a readable answer, and the time each forgotten one had built up.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DipsInfoChecks {
    pub missing: HashMap<IndexerId, Duration>,
    pub cleared: HashMap<IndexerId, Duration>,
}

/// Trait for tracking how long each agreement holder's DIPs info has been missing.
#[async_trait]
pub trait DipsInfoRegistry {
    /// Get every indexer holding at least 1 agreement accepted on-chain, each once.
    async fn get_indexers_with_accepted_agreements(&self) -> RegistryResult<Vec<IndexerId>>;

    /// Record 1 cycle of checks: add `added` to the time each of `missing` has gone without a
    /// readable answer, and forget every other indexer, which starts its count again from 0.
    async fn record_dips_info_checks(
        &self,
        missing: &[IndexerId],
        added: Duration,
    ) -> RegistryResult<DipsInfoChecks>;

    /// Get the indexers blocked now for missing their DIPs info too long.
    async fn get_dips_info_blocks(&self) -> RegistryResult<Vec<IndexerId>>;

    /// Block each of `indexer_ids`, still missing its DIPs info, until `until`, which puts it on
    /// the denylist IISA gets. Returns those not blocked before this.
    async fn block_missing_dips_info(
        &self,
        indexer_ids: &[IndexerId],
        until: time::OffsetDateTime,
    ) -> RegistryResult<Vec<IndexerId>>;
}
