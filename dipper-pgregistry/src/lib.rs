//! # A PostgreSQL-based registry for indexing operations.

use sqlx::{
    Acquire,
    migrate::{Migrate, MigrateError},
};

mod indexing_agreement;
mod indexing_request;
mod postgres;
mod result;

pub use indexing_agreement::{
    Indexer as IndexingAgreementIndexer, IndexingAgreement, Status as IndexingAgreementStatus,
    Terms as IndexingAgreementTerms, TermsMetadata as IndexingAgreementTermsMetadata,
    rejection_reason,
};
pub use indexing_request::{
    IndexingRequest, SetTargetOutcome as IndexingRequestSetTargetOutcome,
    Status as IndexingRequestStatus,
};
pub use postgres::{
    CancelKind, CancellingAgreement, ChainListenerStateRow, NewAgreementParams,
    PendingAcceptedEvent, PendingExpiredEvent, PendingTerminatedEvent, PgRegistry,
    ReconciliationAudit, ReconciliationItem, ReconciliationOutcome,
};
pub use result::{Error, Result};

/// Run the DB migrations.
///
/// It is used to ensure that the database is up to date with the latest migrations.
pub async fn run_db_migrations<'a, A>(conn: A) -> Result<(), MigrateError>
where
    A: Acquire<'a>,
    <A::Connection as std::ops::Deref>::Target: Migrate,
{
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator.run(conn).await?;
    Ok(())
}
