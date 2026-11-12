//! Set-aside change registry trait.
//!
//! The chain listener moves its cursor past a page whose changes keep failing to apply, so it
//! records those agreements here first and tries them again on every poll until they apply.

use async_trait::async_trait;
use dipper_core::ids::IndexingAgreementId;

use super::result::Result as RegistryResult;

/// An on-chain change to an agreement set aside after it kept failing to apply: the agreement,
/// and the block the change was seen at, as a state read from before it predates the change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SetAsideChange {
    pub agreement_id: IndexingAgreementId,
    pub block: u64,
}

#[async_trait]
pub trait SetAsideChangeRegistry {
    /// Record `changes` to be tried again. An agreement already recorded keeps the later block.
    async fn record_set_aside_changes(&self, changes: &[SetAsideChange]) -> RegistryResult<()>;

    /// Get every change set aside, the longest set aside first.
    async fn get_set_aside_changes(&self) -> RegistryResult<Vec<SetAsideChange>>;

    /// Remove the changes to `agreement_ids`, once they applied or had nothing to apply.
    async fn delete_set_aside_changes(
        &self,
        agreement_ids: &[IndexingAgreementId],
    ) -> RegistryResult<()>;
}
