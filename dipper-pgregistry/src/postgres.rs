//! PostgreSQL implementation of the registry

use std::collections::HashMap;

use dipper_core::ids::{IndexingAgreementId, IndexingRequestId};
use sqlx::{Pool, Postgres, types::Json};
use thegraph_core::{
    DeploymentId, IndexerId,
    alloy::primitives::{Address, ChainId},
};
use url::Url;

/// Parameters for registering a new indexing agreement.
pub struct NewAgreementParams {
    pub agreement_id: IndexingAgreementId,
    pub nonce_uuid: uuid::Uuid,
    pub request_id: IndexingRequestId,
    pub deployment_id: DeploymentId,
    pub indexer_id: IndexerId,
    pub indexer_url: Url,
    pub terms: crate::IndexingAgreementTerms,
    pub terms_version_hash: Option<Vec<u8>>,
}

use self::common::{PgAddress, PgDeploymentId, PgIndexerId, PgU64, PgUrl};
use super::{
    indexing_agreement::{IndexingAgreement, Status as IndexingAgreementStatus},
    indexing_request::{
        IndexingRequest, SetTargetOutcome as IndexingRequestSetTargetOutcome,
        Status as IndexingRequestStatus,
    },
    result::Error,
};

pub(crate) mod common;
mod indexing_agreement;
mod indexing_request;

/// Chain listener state row from the database.
#[derive(Debug, Clone)]
pub struct ChainListenerStateRow {
    pub chain_id: u64,
    pub last_processed_block: u64,
    /// `id` of the last consumed entity at `last_processed_block`. `None`
    /// means the cursor sits at a block boundary (genesis or after a
    /// strict block advance). Stored as `BYTEA` and surfaced here as the
    /// strongly-typed `IndexingAgreementId`.
    pub last_processed_id: Option<dipper_core::ids::IndexingAgreementId>,
    pub last_processed_block_timestamp: Option<u64>,
}

/// Which party cancelled an agreement, for the atomic reconciliation
/// transition written by `apply_reconciliation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelKind {
    /// The reconciliation initiator (dipper's configured signer) cancelled.
    ByRequester,
    /// The indexer cancelled.
    ByIndexer,
}

/// Outcome of an atomic `apply_reconciliation` call. The two booleans
/// report which transitions actually affected a row, so the caller can
/// gate post-commit side effects (e.g. running pending-cancellation
/// bookkeeping only when a fresh `AcceptedOnChain` write happened).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReconciliationOutcome {
    pub did_accept: bool,
    pub did_cancel: bool,
}

/// One row's reconciliation request inside a batched apply.
#[derive(Debug, Clone)]
pub struct ReconciliationItem {
    pub agreement_id: dipper_core::ids::IndexingAgreementId,
    pub apply_accept: bool,
    pub cancel: Option<CancelKind>,
    /// On-chain payload observed for this transition, persisted alongside the
    /// status change so a later emission sweep can rebuild the lifecycle event
    /// from the row alone. Only the fields relevant to the applied transition
    /// are populated.
    pub audit: ReconciliationAudit,
}

/// Audit payload captured at reconcile time for later event emission.
///
/// Timestamps are chain seconds; tx hashes and `canceled_by` are the 0x hex
/// string form matching the event wire representation.
#[derive(Debug, Clone, Default)]
pub struct ReconciliationAudit {
    pub accepted_at: Option<u64>,
    pub accepted_tx: Option<String>,
    pub canceled_at: Option<u64>,
    pub canceled_by: Option<String>,
    pub canceled_tx: Option<String>,
}

/// A row that needs a `terminated` lifecycle event emitted. Sourced entirely
/// from the agreement row so the emission sweep is a pure DB -> broker drain.
#[derive(Debug, Clone)]
pub struct PendingTerminatedEvent {
    pub agreement_id: IndexingAgreementId,
    pub indexer_id: IndexerId,
    pub deployment_id: DeploymentId,
    pub protocol_network: ChainId,
    /// Chain seconds of the on-chain cancel; `None` if the cancel audit was
    /// never recorded (the sweep falls back to `updated_at`).
    pub canceled_at: Option<u64>,
    /// 0x address that cancelled; `None` -> the sweep falls back to the manager.
    pub canceled_by: Option<String>,
    /// 0x hex of the cancel tx; `None` -> emitted empty.
    pub canceled_tx: Option<String>,
    /// When the row was marked terminal; the `terminated_at` fallback.
    pub updated_at: time::OffsetDateTime,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for PendingTerminatedEvent {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row as _;
        let agreement_id = row.try_get("id")?;
        let PgIndexerId(indexer_id) = row.try_get("indexer_id")?;
        let sqlx::types::Json(terms): sqlx::types::Json<crate::indexing_agreement::Terms> =
            row.try_get("terms")?;
        let canceled_at: Option<i64> = row.try_get("canceled_at")?;
        let canceled_by: Option<String> = row.try_get("canceled_by")?;
        let canceled_tx: Option<String> = row.try_get("canceled_tx")?;
        let updated_at = row.try_get("updated_at")?;
        Ok(Self {
            agreement_id,
            indexer_id,
            deployment_id: terms.metadata.subgraph_deployment_id,
            protocol_network: terms.metadata.protocol_network,
            canceled_at: canceled_at.map(|v| v as u64),
            canceled_by,
            canceled_tx,
            updated_at,
        })
    }
}

/// A row that needs an `accepted` lifecycle event emitted. Sourced entirely from
/// the agreement row so the emission sweep is a pure DB -> broker drain.
#[derive(Debug, Clone)]
pub struct PendingAcceptedEvent {
    pub agreement_id: IndexingAgreementId,
    pub indexer_id: IndexerId,
    pub deployment_id: DeploymentId,
    pub protocol_network: ChainId,
    /// Chain seconds of the on-chain accept (always present -- the query gates on
    /// `accepted_at IS NOT NULL`).
    pub accepted_at: u64,
    /// 0x hex of the accept tx; empty if unset.
    pub accepted_tx: String,
    /// Agreement end timestamp, from the row's terms.
    pub ends_at: u64,
    /// Payer address, from the row's terms.
    pub payer: Address,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for PendingAcceptedEvent {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row as _;
        let agreement_id = row.try_get("id")?;
        let PgIndexerId(indexer_id) = row.try_get("indexer_id")?;
        let sqlx::types::Json(terms): sqlx::types::Json<crate::indexing_agreement::Terms> =
            row.try_get("terms")?;
        let accepted_at: i64 = row.try_get("accepted_at")?;
        let accepted_tx: Option<String> = row.try_get("accepted_tx")?;
        Ok(Self {
            agreement_id,
            indexer_id,
            deployment_id: terms.metadata.subgraph_deployment_id,
            protocol_network: terms.metadata.protocol_network,
            accepted_at: accepted_at as u64,
            accepted_tx: accepted_tx.unwrap_or_default(),
            ends_at: terms.ends_at,
            payer: terms.payer,
        })
    }
}

/// A row that needs a `request.expired` lifecycle event emitted. Sourced from
/// the agreement row alone; `request_expired_at` is the terms deadline (the true
/// expiry instant), so the sweep needs no chain-time snapshot.
#[derive(Debug, Clone)]
pub struct PendingExpiredEvent {
    pub agreement_id: IndexingAgreementId,
    pub indexer_id: IndexerId,
    pub deployment_id: DeploymentId,
    pub protocol_network: ChainId,
    /// Proposal time (terms `proposed_at`, or the row `created_at` for
    /// pre-`proposed_at` rows).
    pub request_proposed_at: u64,
    /// The deadline that passed -- the true expiry instant.
    pub request_expired_at: u64,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for PendingExpiredEvent {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        use sqlx::Row as _;
        let agreement_id = row.try_get("id")?;
        let PgIndexerId(indexer_id) = row.try_get("indexer_id")?;
        let created_at: time::OffsetDateTime = row.try_get("created_at")?;
        let sqlx::types::Json(terms): sqlx::types::Json<crate::indexing_agreement::Terms> =
            row.try_get("terms")?;
        let request_proposed_at = match terms.metadata.proposed_at {
            0 => created_at.unix_timestamp().max(0) as u64,
            ts => ts,
        };
        Ok(Self {
            agreement_id,
            indexer_id,
            deployment_id: terms.metadata.subgraph_deployment_id,
            protocol_network: terms.metadata.protocol_network,
            request_proposed_at,
            request_expired_at: terms.deadline,
        })
    }
}

/// A registry that stores indexing requests and agreements in a PostgreSQL database.
#[derive(Clone)]
pub struct PgRegistry {
    pool: Pool<Postgres>,
}

impl PgRegistry {
    /// Create a new instance of the registry with the given PostgreSQL connection pool.
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }
}

impl PgRegistry {
    /// Set the target number of indexer candidates for the key
    /// `(requested_by, deployment_id, deployment_chain_id)`.
    ///
    /// Atomic upsert: a fresh key inserts a new Open row; an existing Open row
    /// has its `num_candidates` adjusted (or transitions to Canceled when the
    /// new target is zero). See [`IndexingRequestSetTargetOutcome`] for the
    /// returned discriminator.
    pub async fn set_indexing_target_candidates(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
        num_candidates: i32,
    ) -> Result<IndexingRequestSetTargetOutcome, Error> {
        let mut tx = self.pool.begin().await?;

        let existing: Option<(IndexingRequestId, i32)> = sqlx::query_as(
            r#"
            SELECT id, num_candidates
            FROM dipper_reg_indexing_requests
            WHERE requested_by = $1
              AND deployment_id = $2
              AND deployment_chain_id = $3
              AND status = $4
            FOR UPDATE
            "#,
        )
        .bind(PgAddress(requested_by))
        .bind(PgDeploymentId(deployment_id))
        .bind(PgU64(deployment_chain_id))
        .bind(IndexingRequestStatus::Open)
        .fetch_optional(&mut *tx)
        .await?;

        let outcome = match existing {
            None if num_candidates == 0 => IndexingRequestSetTargetOutcome::NoOpAlreadyEmpty,
            None => {
                let new_id = IndexingRequestId::new();
                sqlx::query(
                    r#"
                    INSERT INTO dipper_reg_indexing_requests (
                        id, created_at, updated_at, status,
                        requested_by, deployment_id, deployment_chain_id, num_candidates
                    )
                    VALUES (
                        $1, timezone('UTC', now()), timezone('UTC', now()), $2,
                        $3, $4, $5, $6
                    )
                    "#,
                )
                .bind(new_id)
                .bind(IndexingRequestStatus::Open)
                .bind(PgAddress(requested_by))
                .bind(PgDeploymentId(deployment_id))
                .bind(PgU64(deployment_chain_id))
                .bind(num_candidates)
                .execute(&mut *tx)
                .await?;
                IndexingRequestSetTargetOutcome::Inserted { id: new_id }
            }
            Some((id, existing_count)) if existing_count == num_candidates => {
                IndexingRequestSetTargetOutcome::NoOp { id }
            }
            Some((id, _)) if num_candidates == 0 => {
                sqlx::query(
                    r#"
                    UPDATE dipper_reg_indexing_requests
                    SET status = $1, updated_at = timezone('UTC', now())
                    WHERE id = $2
                    "#,
                )
                .bind(IndexingRequestStatus::Canceled)
                .bind(id)
                .execute(&mut *tx)
                .await?;
                IndexingRequestSetTargetOutcome::Canceled { id }
            }
            Some((id, _)) => {
                sqlx::query(
                    r#"
                    UPDATE dipper_reg_indexing_requests
                    SET num_candidates = $1, updated_at = timezone('UTC', now())
                    WHERE id = $2
                    "#,
                )
                .bind(num_candidates)
                .bind(id)
                .execute(&mut *tx)
                .await?;
                IndexingRequestSetTargetOutcome::Updated {
                    id,
                    new_num_candidates: num_candidates,
                }
            }
        };

        tx.commit().await?;
        Ok(outcome)
    }

    pub async fn get_all_indexing_requests(&self) -> Result<Vec<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id,
                num_candidates
            FROM dipper_reg_indexing_requests
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_request_by_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Option<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id,
                num_candidates
            FROM dipper_reg_indexing_requests
            WHERE id = $1
            "#,
        )
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_requests_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id,
                num_candidates
            FROM dipper_reg_indexing_requests
            WHERE deployment_id = $1
            "#,
        )
        .bind(PgDeploymentId(*deployment_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Returns all indexing agreements associated with an indexing request that are in an active
    /// state: `CREATED` or `ACCEPTED_ON_CHAIN`.
    pub async fn get_active_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE indexing_request_id = $1 AND status IN ($2, $3)
            "#,
        )
        .bind(request_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Counts the indexing agreements for a deployment that are accepted on-chain.
    ///
    /// Used to populate `remaining_accepted_indexing_agreements` on the
    /// `terminated` lifecycle event: the number of still-active accepted
    /// agreements left on the Subgraph (deployment) after a cancellation.
    pub async fn count_accepted_agreements_by_deployment(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<i64, Error> {
        let (count,): (i64,) = sqlx::query_as(
            r#"
            SELECT COUNT(*)
            FROM dipper_reg_indexing_agreements
            WHERE deployment_id = $1 AND status = $2
            "#,
        )
        .bind(PgDeploymentId(*deployment_id))
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn register_new_indexing_agreement(
        &self,
        params: NewAgreementParams,
    ) -> Result<IndexingAgreementId, Error> {
        let NewAgreementParams {
            agreement_id,
            nonce_uuid,
            request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            terms,
            terms_version_hash,
        } = params;
        sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_agreements (
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                terms_version_hash
            )
            VALUES (
                $1, $2, timezone('UTC', now()), timezone('UTC', now()), $3, $4, $5, $6,
                $7, $8, $9
            )
            RETURNING id
            "#,
        )
        .bind(agreement_id)
        .bind(nonce_uuid)
        .bind(IndexingAgreementStatus::default())
        .bind(request_id)
        .bind(PgDeploymentId(deployment_id))
        .bind(PgIndexerId(indexer_id))
        .bind(PgUrl(indexer_url))
        .bind(Json(terms))
        .bind(terms_version_hash)
        .fetch_one(&self.pool)
        .await
        .map(|(id,)| id)
        .map_err(Into::into)
    }

    pub async fn get_indexing_agreement_by_id(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<Option<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Batch lookup of agreements by id. Missing ids are absent from the
    /// returned map. Single round-trip (`WHERE id = ANY($1)`) so the
    /// chain listener's per-page reconcile prep doesn't issue one SELECT
    /// per snapshot.
    pub async fn get_indexing_agreements_by_ids(
        &self,
        agreement_ids: &[IndexingAgreementId],
    ) -> Result<HashMap<IndexingAgreementId, IndexingAgreement>, Error> {
        if agreement_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows: Vec<IndexingAgreement> = sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE id = ANY($1)
            "#,
        )
        .bind(agreement_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|a| (a.id, a)).collect())
    }

    pub async fn get_indexing_agreements_by_deployment_id(
        &self,
        deployment_id: &DeploymentId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE deployment_id = $1
            "#,
        )
        .bind(PgDeploymentId(*deployment_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn get_indexing_agreements_by_indexer_id(
        &self,
        indexer_id: &IndexerId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE indexer_id = $1
            "#,
        )
        .bind(PgIndexerId(*indexer_id))
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Get aggregated deployment-to-indexers mapping for active agreements.
    ///
    /// Returns agreements that are in `CREATED` or `ACCEPTED_ON_CHAIN` status
    /// for any of the provided indexer IDs, grouped by deployment. This performs database-side
    /// aggregation, returning only the deployment IDs and their associated indexer IDs rather
    /// than full agreement objects.
    ///
    /// Returns a map where keys are deployment IDs and values are lists of indexer IDs
    /// that have active agreements for that deployment.
    pub async fn get_pending_agreement_indexers_by_deployment(
        &self,
        indexer_ids: &[IndexerId],
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>, Error> {
        if indexer_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let pg_indexer_ids: Vec<PgIndexerId> =
            indexer_ids.iter().map(|id| PgIndexerId(*id)).collect();

        let rows: Vec<(PgDeploymentId, Vec<PgIndexerId>)> = sqlx::query_as(
            r#"
            SELECT
                deployment_id,
                array_agg(indexer_id) as indexer_ids
            FROM dipper_reg_indexing_agreements
            WHERE indexer_id = ANY($1) AND status IN ($2, $3)
            GROUP BY deployment_id
            "#,
        )
        .bind(&pg_indexer_ids[..])
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(deployment, indexers)| {
                (deployment.0, indexers.into_iter().map(|i| i.0).collect())
            })
            .collect())
    }

    /// Get declined `CanceledByIndexer`/`Expired`/`Rejected` indexers grouped by
    /// deployment (deployment id -> indexer ids). Each rejection reason gets its own
    /// exclusion window, as does an expiry that never had an offer transaction.
    pub async fn get_declined_indexers_by_deployment(
        &self,
        default_lookback_days: i32,
        price_lookback_days: i32,
        transient_lookback_minutes: i32,
        uncertain_lookback_days: i32,
    ) -> Result<HashMap<DeploymentId, Vec<IndexerId>>, Error> {
        use crate::rejection_reason::{
            AGREEMENT_EXPIRED, CAPACITY_EXCEEDED, DEADLINE_EXPIRED, INDEXER_UNAVAILABLE,
            INVALID_SIGNATURE, PRICE_TOO_LOW, REPLAY_DETECTED, SENDER_NOT_TRUSTED,
            SUBGRAPH_MANIFEST_UNAVAILABLE, UNEXPECTED_SERVICE_PROVIDER, UNSPECIFIED,
            UNSUPPORTED_METADATA_VERSION,
        };

        let rows: Vec<(PgDeploymentId, Vec<PgIndexerId>)> = sqlx::query_as(
            r#"
            SELECT
                deployment_id,
                array_agg(DISTINCT indexer_id) as indexer_ids
            FROM dipper_reg_indexing_agreements
            WHERE status IN ($1, $2, $3)
              AND (
                -- PRICE_TOO_LOW: shorter lookback (until next IISA refresh)
                (rejection_reason = $6
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $4))
                OR
                -- Transient, not-indexer's-fault, or dipper-side faults that
                -- clear once dipper is fixed: very short lookback
                (rejection_reason IN ($8, $9, $10, $11, $12, $13, $14, $15, $16)
                 AND updated_at >= timezone('UTC', now()) - make_interval(mins => $7))
                OR
                -- Uncertain reasons (sender not trusted, unspecified/unknown):
                -- may clear within about a day, so a 1-day lookback
                (rejection_reason IN ($18, $19)
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $17))
                OR
                -- An expiry with no offer transaction and no rejection reason means we never
                -- landed the offer, so the indexer never had one to accept: our fault, so it
                -- gets the short dipper-side lookback. An expiry that does carry a reason keeps
                -- the window that reason earns. The catch-all below excludes exactly this set.
                -- Read the empty hash as strong evidence, not proof. It is also empty when the
                -- write recording it failed, and when the transaction confirmed after this row
                -- had already expired, which both let off an indexer that could have accepted:
                -- the harmless direction. The column's own migration calls it observability
                -- only, so note that this query is what makes it load-bearing.
                (status = $2 AND offer_tx_hash IS NULL AND rejection_reason IS NULL
                 AND updated_at >= timezone('UTC', now()) - make_interval(mins => $7))
                OR
                -- All other rejections/expirations/cancellations: standard lookback
                (COALESCE(rejection_reason, '') NOT IN ($6, $8, $9, $10, $11, $12, $13, $14, $15, $16, $18, $19)
                 AND NOT (status = $2 AND offer_tx_hash IS NULL AND rejection_reason IS NULL)
                 AND updated_at >= timezone('UTC', now()) - make_interval(days => $5))
              )
            GROUP BY deployment_id
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByIndexer) // $1
        .bind(IndexingAgreementStatus::Expired) // $2
        .bind(IndexingAgreementStatus::Rejected) // $3
        .bind(price_lookback_days) // $4
        .bind(default_lookback_days) // $5
        .bind(PRICE_TOO_LOW) // $6
        .bind(transient_lookback_minutes) // $7
        .bind(DEADLINE_EXPIRED) // $8
        .bind(SUBGRAPH_MANIFEST_UNAVAILABLE) // $9
        .bind(UNEXPECTED_SERVICE_PROVIDER) // $10
        .bind(AGREEMENT_EXPIRED) // $11
        .bind(UNSUPPORTED_METADATA_VERSION) // $12
        .bind(CAPACITY_EXCEEDED) // $13
        .bind(INDEXER_UNAVAILABLE) // $14
        .bind(INVALID_SIGNATURE) // $15
        .bind(REPLAY_DETECTED) // $16
        .bind(uncertain_lookback_days) // $17
        .bind(SENDER_NOT_TRUSTED) // $18
        .bind(UNSPECIFIED) // $19
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(deployment, indexers)| {
                (deployment.0, indexers.into_iter().map(|i| i.0).collect())
            })
            .collect())
    }

    /// Get indexers with a recent `Unresponsive` agreement on a given chain. Used
    /// to skip an unresponsive indexer for that chain's deployments for
    /// `lookback_days` after it last failed to respond there.
    pub async fn get_unresponsive_indexers(
        &self,
        lookback_days: i32,
        chain_id: ChainId,
    ) -> Result<Vec<IndexerId>, Error> {
        let rows: Vec<(PgIndexerId,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT a.indexer_id
            FROM dipper_reg_indexing_agreements a
            JOIN dipper_reg_indexing_requests r ON a.indexing_request_id = r.id
            WHERE a.status = $1
              AND a.updated_at >= timezone('UTC', now()) - make_interval(days => $2)
              AND r.deployment_chain_id = $3
            "#,
        )
        .bind(IndexingAgreementStatus::Unresponsive)
        .bind(lookback_days)
        .bind(PgU64(chain_id))
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id.0).collect())
    }

    pub async fn get_indexing_agreements_by_indexing_request_id(
        &self,
        request_id: &IndexingRequestId,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE indexing_request_id = $1
            "#,
        )
        .bind(request_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn mark_indexing_agreement_as_unresponsive(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::Unresponsive)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    /// Persist the on-chain tx hash of the most recent `offer()` submission
    /// for this agreement. Overwrites any prior value, so a resubmit after
    /// mempool eviction records the live hash rather than the dropped one.
    /// Observability-only: no status transition is performed here.
    ///
    /// Guarded on `status IN (Created, AcceptedOnChain)` so a delayed
    /// receipt-confirmation cannot stamp `offer_tx_hash` onto a row that
    /// has since transitioned to `Expired`, `Unresponsive`, `Rejected`,
    /// or one of the cancel states. The caller treats any failure here
    /// as non-fatal and just logs; a no-match result is also non-fatal
    /// and silently skipped.
    pub async fn update_offer_tx_hash(
        &self,
        agreement_id: &IndexingAgreementId,
        tx_hash: &[u8; 32],
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                offer_tx_hash = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status IN ($3, $4)
            "#,
        )
        .bind(&tx_hash[..])
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Backfill a `terms_version_hash` recovered from on-chain state for an
    /// agreement whose local copy is missing — e.g. a row from before the
    /// `terms_version_hash` column existed. No status guard: unlike
    /// `update_offer_tx_hash` (which risks clobbering a live tx hash for an
    /// agreement that has since moved on), a missing hash never gets a
    /// legitimate DB-side update to race with, and cancellation from any
    /// non-terminal status still needs the recovered value.
    pub async fn update_terms_version_hash(
        &self,
        agreement_id: &IndexingAgreementId,
        hash: &[u8; 32],
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                terms_version_hash = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND terms_version_hash IS NULL
            "#,
        )
        .bind(&hash[..])
        .bind(agreement_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_indexing_agreement_as_canceled_by_requester(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status IN ($3, $4, $5)
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByRequester)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(IndexingAgreementStatus::Rejected)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    /// Atomically apply a reconciliation-driven state transition (accept
    /// and/or cancel) in a single database transaction so the
    /// chain_listener's Accept-then-Cancel-in-one-snapshot path does not
    /// leave an intermediate `AcceptedOnChain` row visible to concurrent
    /// readers.
    ///
    /// `did_accept` is false when the agreement was not in `Created` or
    /// `Expired` at UPDATE time; callers gate side effects like
    /// `execute_pending_cancellations` on this so it only fires on a fresh
    /// accept write.
    ///
    /// Roll back with `NoRecordsUpdated` only when an accept landed in
    /// this tx but the paired cancel matched no row — committing then
    /// would leave the AcceptedOnChain write visible without its
    /// follow-up cancel, which the Accept-then-Cancel-in-one-snapshot
    /// invariant forbids. When both writes find no matching row (caller
    /// passed `apply_accept = false` and the cancel filter rejected, e.g.
    /// the row is in a terminal-but-not-cancel state like `Unresponsive`
    /// that the chain_listener's Rust-side guard does not catch), commit
    /// the empty tx and return `Ok` with both flags false. The
    /// chain_listener treats that as a successful no-op rather than a
    /// hard error.
    pub async fn apply_reconciliation(
        &self,
        agreement_id: &IndexingAgreementId,
        apply_accept: bool,
        cancel: Option<CancelKind>,
    ) -> Result<ReconciliationOutcome, Error> {
        let mut tx = self.pool.begin().await?;

        let did_accept = if apply_accept {
            update_status_from(
                &mut tx,
                agreement_id,
                IndexingAgreementStatus::AcceptedOnChain,
                &[
                    IndexingAgreementStatus::Created,
                    IndexingAgreementStatus::Expired,
                ],
            )
            .await?
        } else {
            false
        };

        let mut did_cancel = false;
        if let Some(kind) = cancel {
            let (new_status, allowed_from): (_, &[IndexingAgreementStatus]) = match kind {
                CancelKind::ByRequester => (
                    IndexingAgreementStatus::CanceledByRequester,
                    &[
                        IndexingAgreementStatus::Created,
                        IndexingAgreementStatus::AcceptedOnChain,
                        IndexingAgreementStatus::Rejected,
                    ],
                ),
                CancelKind::ByIndexer => (
                    IndexingAgreementStatus::CanceledByIndexer,
                    &[IndexingAgreementStatus::AcceptedOnChain],
                ),
            };
            did_cancel =
                update_status_from(&mut tx, agreement_id, new_status, allowed_from).await?;
            if did_accept && !did_cancel {
                return Err(Error::NoRecordsUpdated);
            }
        }

        tx.commit().await?;

        Ok(ReconciliationOutcome {
            did_accept,
            did_cancel,
        })
    }

    /// Batched `apply_reconciliation`. Collapses single-transition items
    /// into one transaction with at most three batched `UPDATE`s
    /// (accept, cancel-by-requester, cancel-by-indexer); items combining
    /// accept+cancel still run per-row in the same tx so the rollback
    /// rule on a paired-cancel miss is preserved. Every input id appears
    /// in the returned map, with both flags `false` when no row flipped.
    ///
    /// Caller contract: input ids must be unique. Duplicates would leave
    /// stale outcome flags from the first iteration; debug_asserted.
    pub async fn apply_reconciliation_batch(
        &self,
        items: &[ReconciliationItem],
    ) -> Result<HashMap<IndexingAgreementId, ReconciliationOutcome>, Error> {
        debug_assert!(
            {
                let mut seen: std::collections::HashSet<IndexingAgreementId> =
                    std::collections::HashSet::with_capacity(items.len());
                items.iter().all(|i| seen.insert(i.agreement_id))
            },
            "apply_reconciliation_batch requires unique agreement_ids; duplicates would leave \
             stale outcome flags from the first iteration",
        );

        let mut outcomes: HashMap<IndexingAgreementId, ReconciliationOutcome> =
            HashMap::with_capacity(items.len());
        for item in items {
            outcomes.insert(item.agreement_id, ReconciliationOutcome::default());
        }

        if items.is_empty() {
            return Ok(outcomes);
        }

        // Single-pass partition into the four item shapes.
        let mut paired: Vec<&ReconciliationItem> = Vec::new();
        let mut accept_only: Vec<IndexingAgreementId> = Vec::new();
        let mut cancel_by_requester: Vec<IndexingAgreementId> = Vec::new();
        let mut cancel_by_indexer: Vec<IndexingAgreementId> = Vec::new();
        for item in items {
            match (item.apply_accept, item.cancel) {
                (true, Some(_)) => paired.push(item),
                (true, None) => accept_only.push(item.agreement_id),
                (false, Some(CancelKind::ByRequester)) => {
                    cancel_by_requester.push(item.agreement_id)
                }
                (false, Some(CancelKind::ByIndexer)) => cancel_by_indexer.push(item.agreement_id),
                (false, None) => {}
            }
        }

        let mut tx = self.pool.begin().await?;

        for item in paired {
            let cancel_kind = item.cancel.expect("paired implies Some");
            let did_accept = update_status_from(
                &mut tx,
                &item.agreement_id,
                IndexingAgreementStatus::AcceptedOnChain,
                &[
                    IndexingAgreementStatus::Created,
                    IndexingAgreementStatus::Expired,
                ],
            )
            .await?;
            let (new_status, allowed_from): (_, &[IndexingAgreementStatus]) = match cancel_kind {
                CancelKind::ByRequester => (
                    IndexingAgreementStatus::CanceledByRequester,
                    &[
                        IndexingAgreementStatus::Created,
                        IndexingAgreementStatus::AcceptedOnChain,
                        IndexingAgreementStatus::Rejected,
                    ],
                ),
                CancelKind::ByIndexer => (
                    IndexingAgreementStatus::CanceledByIndexer,
                    &[IndexingAgreementStatus::AcceptedOnChain],
                ),
            };
            let did_cancel =
                update_status_from(&mut tx, &item.agreement_id, new_status, allowed_from).await?;
            if did_accept && !did_cancel {
                return Err(Error::NoRecordsUpdated);
            }
            outcomes.insert(
                item.agreement_id,
                ReconciliationOutcome {
                    did_accept,
                    did_cancel,
                },
            );
        }

        for id in batch_update_status_from(
            &mut tx,
            &accept_only,
            IndexingAgreementStatus::AcceptedOnChain,
            &[
                IndexingAgreementStatus::Created,
                IndexingAgreementStatus::Expired,
            ],
        )
        .await?
        {
            outcomes.entry(id).or_default().did_accept = true;
        }

        for id in batch_update_status_from(
            &mut tx,
            &cancel_by_requester,
            IndexingAgreementStatus::CanceledByRequester,
            &[
                IndexingAgreementStatus::Created,
                IndexingAgreementStatus::AcceptedOnChain,
                IndexingAgreementStatus::Rejected,
            ],
        )
        .await?
        {
            outcomes.entry(id).or_default().did_cancel = true;
        }

        for id in batch_update_status_from(
            &mut tx,
            &cancel_by_indexer,
            IndexingAgreementStatus::CanceledByIndexer,
            &[IndexingAgreementStatus::AcceptedOnChain],
        )
        .await?
        {
            outcomes.entry(id).or_default().did_cancel = true;
        }

        // Persist the observed on-chain payload for every row that actually
        // transitioned, in the same transaction as the status change, so a later
        // emission sweep can rebuild the lifecycle event from the row alone.
        for item in items {
            let outcome = outcomes
                .get(&item.agreement_id)
                .copied()
                .unwrap_or_default();
            if outcome.did_accept {
                persist_accept_audit(&mut tx, &item.agreement_id, &item.audit).await?;
            }
            if outcome.did_cancel {
                persist_cancel_audit(&mut tx, &item.agreement_id, &item.audit).await?;
            }
        }

        tx.commit().await?;

        Ok(outcomes)
    }

    /// Fetch a batch of agreements awaiting a `terminated` event: in a
    /// terminal-cancel state, genuinely accepted on-chain (`accepted_at IS NOT
    /// NULL`, so a never-accepted local cancel is excluded), and not yet
    /// emitted. Oldest-marked first so the backlog drains in order.
    pub async fn get_agreements_pending_terminated_emission(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingTerminatedEvent>, Error> {
        let rows = sqlx::query_as::<_, PendingTerminatedEvent>(
            r#"
            SELECT id, indexer_id, terms, canceled_at, canceled_by, canceled_tx, updated_at
            FROM dipper_reg_indexing_agreements
            WHERE status IN ($1, $2, $3)
              AND accepted_at IS NOT NULL
              AND terminated_event_emitted_at IS NULL
            ORDER BY updated_at ASC
            LIMIT $4
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByRequester)
        .bind(IndexingAgreementStatus::CanceledByIndexer)
        .bind(IndexingAgreementStatus::AbandonedByIndexer)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Stamp the `terminated` emission marker after a confirmed broker send.
    pub async fn mark_terminated_event_emitted(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET terminated_event_emitted_at = timezone('UTC', now())
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fetch a batch of agreements awaiting an `accepted` event: the accept was
    /// observed by the reconcile path (`accepted_at IS NOT NULL`, so pre-feature
    /// rows are never backfilled) and not yet emitted. Note this is NOT gated on
    /// current status: an agreement accepted and then cancelled in a single
    /// snapshot (dipper was behind) ends up `CanceledBy*` with `accepted_at` set,
    /// and must still emit its `accepted` before the `terminated`. Oldest-accepted
    /// first.
    pub async fn get_agreements_pending_accepted_emission(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingAcceptedEvent>, Error> {
        let rows = sqlx::query_as::<_, PendingAcceptedEvent>(
            r#"
            SELECT id, indexer_id, terms, accepted_at, accepted_tx
            FROM dipper_reg_indexing_agreements
            WHERE accepted_at IS NOT NULL
              AND accepted_event_emitted_at IS NULL
            ORDER BY accepted_at ASC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Stamp the `accepted` emission marker after a confirmed broker send.
    pub async fn mark_accepted_event_emitted(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET accepted_event_emitted_at = timezone('UTC', now())
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fetch a batch of agreements awaiting a `request.expired` event: currently
    /// `Expired` and not yet emitted. A row recovered `Expired -> AcceptedOnChain`
    /// before the sweep runs is no longer selected, so no premature `expired`
    /// contradicts a later `accepted`. Oldest-updated first.
    pub async fn get_agreements_pending_expired_emission(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingExpiredEvent>, Error> {
        let rows = sqlx::query_as::<_, PendingExpiredEvent>(
            r#"
            SELECT id, indexer_id, created_at, terms
            FROM dipper_reg_indexing_agreements
            WHERE status = $1
              AND expired_event_emitted_at IS NULL
            ORDER BY updated_at ASC
            LIMIT $2
            "#,
        )
        .bind(IndexingAgreementStatus::Expired)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Stamp the `request.expired` emission marker after a confirmed broker send.
    pub async fn mark_expired_event_emitted(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET expired_event_emitted_at = timezone('UTC', now())
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record the accepted audit payload out-of-band from `apply_reconciliation`.
    ///
    /// Used for the rejected-then-accepted anomaly: such a row goes
    /// `Rejected -> Canceled` without ever transiting `AcceptedOnChain`, so the
    /// normal accept transition never records it. Persisting it here (from the
    /// observed snapshot) marks the row as genuinely accepted so its eventual
    /// `terminated` is sweep-eligible. `COALESCE` keeps any existing value.
    pub async fn record_accepted_audit(
        &self,
        agreement_id: &IndexingAgreementId,
        accepted_at: u64,
        accepted_tx: &str,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET accepted_at = COALESCE(accepted_at, $2),
                accepted_tx = COALESCE(accepted_tx, $3)
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .bind(accepted_at as i64)
        .bind(accepted_tx)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record the cancel audit payload for a dipper-initiated cancel so the
    /// emission sweep can populate the `terminated` event's tx/by/at fields.
    /// `COALESCE` keeps any value already observed on-chain. Best-effort
    /// enrichment: the event still emits (with fallbacks) if never recorded.
    pub async fn record_cancel_audit(
        &self,
        agreement_id: &IndexingAgreementId,
        canceled_at: u64,
        canceled_by: &str,
        canceled_tx: Option<&str>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET canceled_at = COALESCE(canceled_at, $2),
                canceled_by = COALESCE(canceled_by, $3),
                canceled_tx = COALESCE(canceled_tx, $4)
            WHERE id = $1
            "#,
        )
        .bind(agreement_id)
        .bind(canceled_at as i64)
        .bind(canceled_by)
        .bind(canceled_tx)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // =========================================================================
    // Reassignment operations
    // =========================================================================

    /// Get open indexing requests eligible for reassessment.
    ///
    /// Returns requests that are in the `OPEN` status and were created at least
    /// `min_age_seconds` ago. Results are ordered by `updated_at` ascending to
    /// prioritize requests that haven't been reassessed recently.
    ///
    /// If `batch_size` is greater than 0, limits the number of results.
    /// If `batch_size` is 0 or negative, returns all matching requests.
    pub async fn get_open_indexing_requests_for_reassessment(
        &self,
        min_age_seconds: i64,
        batch_size: i64,
    ) -> Result<Vec<IndexingRequest>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                created_at,
                updated_at,
                status,
                requested_by,
                deployment_id,
                deployment_chain_id,
                num_candidates
            FROM dipper_reg_indexing_requests
            WHERE status = $1
              AND created_at < timezone('UTC', now()) - ($2 * interval '1 second')
            ORDER BY updated_at ASC
            LIMIT CASE WHEN $3 > 0 THEN $3 ELSE NULL END
            "#,
        )
        .bind(IndexingRequestStatus::Open)
        .bind(min_age_seconds)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Atomically set the coverage-shortfall latch on a request, returning whether
    /// the value changed. The `shortfall_active <> $2` guard makes the UPDATE a
    /// no-op (returning no row) when the value is unchanged, so the caller learns
    /// whether this was a transition.
    pub async fn set_indexing_request_shortfall_active(
        &self,
        id: &IndexingRequestId,
        active: bool,
    ) -> Result<bool, Error> {
        let record: Option<(IndexingRequestId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_requests
            SET shortfall_active = $2
            WHERE id = $1 AND shortfall_active <> $2
            RETURNING id
            "#,
        )
        .bind(id)
        .bind(active)
        .fetch_optional(&self.pool)
        .await?;
        Ok(record.is_some())
    }

    // =========================================================================
    // Deadline expiration operations
    // =========================================================================

    /// Get `Created` agreements whose RCA deadline has passed.
    ///
    /// Compares `terms.deadline` against `chain_timestamp` (block time).
    pub async fn get_expired_created_agreements(
        &self,
        batch_size: i64,
        chain_timestamp: u64,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE status = $1
              AND CAST(terms->>'deadline' AS bigint) < $3
            ORDER BY CAST(terms->>'deadline' AS bigint) ASC
            LIMIT CASE WHEN $2 > 0 THEN $2 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(batch_size)
        .bind(chain_timestamp as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Mark an agreement as `Expired` (deadline passed, never accepted on-chain).
    ///
    /// Only transitions from `Created` status. Returns [`NoRecordsUpdated`](Error::NoRecordsUpdated)
    /// if the agreement doesn't exist or isn't in `Created` status.
    pub async fn mark_indexing_agreement_as_expired(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::Expired)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    /// Mark an agreement as `Rejected` (indexer rejected the proposal off-chain).
    ///
    /// Only transitions from `Created` status. The indexer may still accept on-chain
    /// before the deadline, in which case Dipper will cancel via `cancelIndexingAgreementByPayer`.
    ///
    /// Returns [`NoRecordsUpdated`](Error::NoRecordsUpdated) if the agreement doesn't exist
    /// or isn't in `Created` status.
    pub async fn mark_indexing_agreement_as_rejected(
        &self,
        agreement_id: &IndexingAgreementId,
        rejection_reason: Option<&str>,
    ) -> Result<(), Error> {
        let record: Option<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now()),
                rejection_reason = $4
            WHERE id = $2 AND status = $3
            RETURNING id
            "#,
        )
        .bind(IndexingAgreementStatus::Rejected)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::Created)
        .bind(rejection_reason)
        .fetch_optional(&self.pool)
        .await?;

        if record.is_none() {
            return Err(Error::NoRecordsUpdated);
        }

        Ok(())
    }

    // =========================================================================
    // Liveness tracking operations
    // =========================================================================

    /// Get all `AcceptedOnChain` agreements for liveness checking.
    ///
    /// Returns agreements ordered by `last_progress_at` ascending (NULLs first),
    /// so agreements that have never been checked are processed first.
    pub async fn get_accepted_on_chain_agreements(
        &self,
        batch_size: i64,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            FROM dipper_reg_indexing_agreements
            WHERE status = $1
            ORDER BY last_progress_at ASC NULLS FIRST
            LIMIT CASE WHEN $2 > 0 THEN $2 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Get agreements still `AcceptedOnChain` whose parent request is `Canceled`.
    ///
    /// Used by the chain listener's periodic orphan-cancel sweep to retry
    /// on-chain `cancelIndexingAgreementByPayer` calls that failed during the
    /// reassessment that flipped the request row.
    pub async fn get_agreements_pending_chain_cancel(
        &self,
        batch_size: i64,
    ) -> Result<Vec<IndexingAgreement>, Error> {
        sqlx::query_as(
            r#"
            SELECT
                a.id,
                a.nonce_uuid,
                a.created_at,
                a.updated_at,
                a.status,
                a.indexing_request_id,
                a.deployment_id,
                a.indexer_id,
                a.indexer_url,
                a.terms,
                a.last_block_height,
                a.last_progress_at,
                a.rejection_reason,
                a.terms_version_hash
            FROM dipper_reg_indexing_agreements a
            JOIN dipper_reg_indexing_requests r
              ON a.indexing_request_id = r.id
            WHERE a.status = $1
              AND r.status = $2
            ORDER BY a.updated_at ASC
            LIMIT CASE WHEN $3 > 0 THEN $3 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(IndexingRequestStatus::Canceled)
        .bind(batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Distinct `indexer_id` (on-chain `service_provider`) of agreements whose
    /// protocol-manager escrow may be orphaned or mid-thaw: ended, canceled, or
    /// still-accepted. Bounded by `limit`.
    pub async fn get_providers_for_escrow_reconciliation(
        &self,
        limit: i64,
    ) -> Result<Vec<Address>, Error> {
        let rows: Vec<(PgIndexerId,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT indexer_id
            FROM dipper_reg_indexing_agreements
            WHERE status IN ($1, $2, $3, $4)
            ORDER BY indexer_id
            LIMIT CASE WHEN $5 > 0 THEN $5 ELSE NULL END
            "#,
        )
        .bind(IndexingAgreementStatus::CanceledByRequester)
        .bind(IndexingAgreementStatus::CanceledByIndexer)
        .bind(IndexingAgreementStatus::Expired)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id.0.into_inner()).collect())
    }

    /// Update the sync progress for an agreement.
    ///
    /// Called when the liveness checker observes the block height has changed
    /// (either advancing or resetting due to a resync).
    pub async fn update_agreement_sync_progress(
        &self,
        agreement_id: &IndexingAgreementId,
        block_height: u64,
        progress_at: time::OffsetDateTime,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                last_block_height = $1,
                last_progress_at = $2
            WHERE id = $3
            "#,
        )
        .bind(block_height as i64)
        .bind(progress_at)
        .bind(agreement_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Count active agreements per deployment.
    ///
    /// Returns a map of deployment ID to count of `Created` or `AcceptedOnChain`
    /// agreements. Used by the liveness checker to determine the tolerance threshold
    /// for each deployment.
    pub async fn count_active_agreements_by_deployment(
        &self,
    ) -> Result<HashMap<DeploymentId, usize>, Error> {
        let rows: Vec<(PgDeploymentId, i64)> = sqlx::query_as(
            r#"
            SELECT deployment_id, COUNT(*) as count
            FROM dipper_reg_indexing_agreements
            WHERE status IN ($1, $2)
            GROUP BY deployment_id
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(deployment, count)| (deployment.0, count as usize))
            .collect())
    }

    /// Count `Created` (in-flight, not yet accepted) agreements per indexer,
    /// returning the per-indexer map and global total in one round-trip. Offer
    /// pacing reads both to gauge spare acceptance capacity before creating more.
    pub async fn count_created_agreements_by_indexer(
        &self,
    ) -> Result<(HashMap<IndexerId, u64>, u64), Error> {
        // GROUPING SETS emits one row per indexer plus a single ()-group row
        // with a NULL indexer_id carrying the global total.
        let rows: Vec<(Option<PgIndexerId>, i64)> = sqlx::query_as(
            r#"
            SELECT indexer_id, COUNT(*) as count
            FROM dipper_reg_indexing_agreements
            WHERE status = $1
            GROUP BY GROUPING SETS ((indexer_id), ())
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .fetch_all(&self.pool)
        .await?;

        let mut per_indexer = HashMap::new();
        let mut global = 0u64;
        for (indexer, count) in rows {
            match indexer {
                Some(id) => {
                    per_indexer.insert(id.0, count as u64);
                }
                None => global = count as u64,
            }
        }
        Ok((per_indexer, global))
    }

    /// Whether any agreement is in `Created` or `AcceptedOnChain` status.
    ///
    /// Cheap `EXISTS` probe used by the chain listener's adaptive-interval
    /// gate every poll; the per-deployment `count_active_agreements_by_deployment`
    /// would scan the full active set just to discard the counts.
    pub async fn exists_active_agreements(&self) -> Result<bool, Error> {
        let (exists,): (bool,) = sqlx::query_as(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM dipper_reg_indexing_agreements
                WHERE status IN ($1, $2)
                LIMIT 1
            )
            "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    /// Mark an agreement as `AbandonedByIndexer`.
    ///
    /// Transitions `AcceptedOnChain → AbandonedByIndexer`. Returns the full
    /// agreement for use in the subsequent reassessment call.
    ///
    /// Returns [`NoRecordsUpdated`](Error::NoRecordsUpdated) if the agreement
    /// doesn't exist or isn't in `AcceptedOnChain` status.
    pub async fn mark_indexing_agreement_as_abandoned(
        &self,
        agreement_id: &IndexingAgreementId,
    ) -> Result<IndexingAgreement, Error> {
        let record: Option<IndexingAgreement> = sqlx::query_as(
            r#"
            UPDATE dipper_reg_indexing_agreements
            SET
                status = $1,
                updated_at = timezone('UTC', now())
            WHERE id = $2 AND status = $3
            RETURNING
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                last_block_height,
                last_progress_at,
                rejection_reason,
                terms_version_hash
            "#,
        )
        .bind(IndexingAgreementStatus::AbandonedByIndexer)
        .bind(agreement_id)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_optional(&self.pool)
        .await?;

        record.ok_or(Error::NoRecordsUpdated)
    }

    // =========================================================================
    // Indexer denylist operations
    // =========================================================================

    /// Get all active (non-expired) denied indexer IDs.
    ///
    /// Entries with an expiration date in the past are excluded.
    pub async fn get_indexer_denylist(&self) -> Result<Vec<IndexerId>, Error> {
        let rows: Vec<(PgIndexerId,)> = sqlx::query_as(
            r#"
            SELECT indexer_id
            FROM dipper_indexer_denylist
            WHERE expires_at IS NULL OR expires_at > timezone('UTC', now())
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id.0).collect())
    }

    // =========================================================================
    // Optimistic DIPs fees
    // =========================================================================

    /// Returns (agreement_id, indexer_id, deployment_id, base_rate_wei,
    /// entity_rate_wei) per active agreement for optimistic fee estimation.
    ///
    /// Queries all `Created` or `AcceptedOnChain` agreements and extracts
    /// both rate fields from the terms metadata.
    pub async fn get_agreement_fee_rates(
        &self,
    ) -> Result<Vec<(IndexingAgreementId, IndexerId, DeploymentId, f64, f64)>, Error> {
        let rows: Vec<(
            IndexingAgreementId,
            PgIndexerId,
            sqlx::types::Json<super::indexing_agreement::Terms>,
        )> = sqlx::query_as(
            r#"
                SELECT id, indexer_id, terms
                FROM dipper_reg_indexing_agreements
                WHERE status IN ($1, $2)
                "#,
        )
        .bind(IndexingAgreementStatus::Created)
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .fetch_all(&self.pool)
        .await?;

        let rates = rows
            .into_iter()
            .map(|(agreement_id, pg_indexer_id, terms_json)| {
                let meta = &terms_json.0.metadata;
                (
                    agreement_id,
                    pg_indexer_id.0,
                    meta.subgraph_deployment_id,
                    meta.tokens_per_second.to::<u128>() as f64,
                    meta.tokens_per_entity_per_second.to::<u128>() as f64,
                )
            })
            .collect();

        Ok(rates)
    }

    // =========================================================================
    // Chain listener state operations
    // =========================================================================

    /// Get the chain listener state for a given chain ID.
    /// Returns `None` if no state exists for the chain (first run).
    pub async fn get_chain_listener_state(
        &self,
        chain_id: u64,
    ) -> Result<Option<ChainListenerStateRow>, Error> {
        let row: Option<(
            i64,
            i64,
            Option<dipper_core::ids::IndexingAgreementId>,
            Option<i64>,
        )> = sqlx::query_as(
            r#"
            SELECT chain_id, last_processed_block, last_processed_id, last_processed_block_timestamp
            FROM dipper_chain_listener_state
            WHERE chain_id = $1
            "#,
        )
        .bind(chain_id as i64)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|(chain_id, block, id, ts)| ChainListenerStateRow {
            chain_id: chain_id as u64,
            last_processed_block: block as u64,
            last_processed_id: id,
            last_processed_block_timestamp: ts.map(|t| t as u64),
        }))
    }

    /// Update the chain listener state for a given chain ID.
    ///
    /// Creates the record if it doesn't exist (upsert). `last_processed_id`
    /// is the keyset's `id` discriminator at `last_processed_block`; `None`
    /// means the cursor sits at a block boundary.
    pub async fn update_chain_listener_state(
        &self,
        chain_id: u64,
        last_processed_block: u64,
        last_processed_id: Option<dipper_core::ids::IndexingAgreementId>,
        last_processed_block_timestamp: Option<u64>,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO dipper_chain_listener_state
                (chain_id, last_processed_block, last_processed_id, last_processed_block_timestamp, updated_at)
            VALUES ($1, $2, $3, $4, timezone('UTC', now()))
            ON CONFLICT (chain_id)
            DO UPDATE SET
                last_processed_block = EXCLUDED.last_processed_block,
                last_processed_id = EXCLUDED.last_processed_id,
                last_processed_block_timestamp = EXCLUDED.last_processed_block_timestamp,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(chain_id as i64)
        .bind(last_processed_block as i64)
        .bind(last_processed_id)
        .bind(last_processed_block_timestamp.map(|t| t as i64))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // -- Pending cancellations --

    /// Register a new agreement and record a pending cancellation in a single
    /// transaction. Guarantees that if the agreement row exists, the pending
    /// cancellation linking it to the old agreement also exists.
    pub async fn register_agreement_with_pending_cancellation(
        &self,
        params: NewAgreementParams,
        old_agreement_id: IndexingAgreementId,
    ) -> Result<IndexingAgreementId, Error> {
        let NewAgreementParams {
            agreement_id,
            nonce_uuid,
            request_id,
            deployment_id,
            indexer_id,
            indexer_url,
            terms,
            terms_version_hash,
        } = params;
        let mut tx = self.pool.begin().await?;

        let (new_id,): (IndexingAgreementId,) = sqlx::query_as(
            r#"
            INSERT INTO dipper_reg_indexing_agreements (
                id,
                nonce_uuid,
                created_at,
                updated_at,
                status,
                indexing_request_id,
                deployment_id,
                indexer_id,
                indexer_url,
                terms,
                terms_version_hash
            )
            VALUES (
                $1, $2, timezone('UTC', now()), timezone('UTC', now()), $3, $4, $5, $6,
                $7, $8, $9
            )
            RETURNING id
            "#,
        )
        .bind(agreement_id)
        .bind(nonce_uuid)
        .bind(IndexingAgreementStatus::default())
        .bind(request_id)
        .bind(PgDeploymentId(deployment_id))
        .bind(PgIndexerId(indexer_id))
        .bind(PgUrl(indexer_url))
        .bind(Json(terms))
        .bind(terms_version_hash)
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO dipper_pending_cancellations
                (new_agreement_id, old_agreement_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(new_id)
        .bind(old_agreement_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(new_id)
    }

    /// Get all pending cancellations linked to a new agreement.
    pub async fn get_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> Result<Vec<IndexingAgreementId>, Error> {
        let rows: Vec<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            SELECT old_agreement_id
            FROM dipper_pending_cancellations
            WHERE new_agreement_id = $1
            "#,
        )
        .bind(new_agreement_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Delete all pending cancellation records for a new agreement.
    /// Called when the new agreement fails (old agreements stay active).
    pub async fn delete_pending_cancellations_by_new_agreement(
        &self,
        new_agreement_id: IndexingAgreementId,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM dipper_pending_cancellations
            WHERE new_agreement_id = $1
            "#,
        )
        .bind(new_agreement_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Delete a single pending cancellation record after it has been processed.
    pub async fn delete_pending_cancellation(
        &self,
        new_agreement_id: IndexingAgreementId,
        old_agreement_id: IndexingAgreementId,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            DELETE FROM dipper_pending_cancellations
            WHERE new_agreement_id = $1 AND old_agreement_id = $2
            "#,
        )
        .bind(new_agreement_id)
        .bind(old_agreement_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// List the distinct `new_agreement_id`s of pending cancellation rows
    /// whose linked agreement has reached `AcceptedOnChain` locally.
    ///
    /// Each ID returned is a candidate for `execute_pending_cancellations`
    /// re-run. The chain_listener uses this as a periodic sweep to recover
    /// from a partial-progress crash inside that function: the local row
    /// was transitioned to `AcceptedOnChain` but the cancellation fan-out
    /// did not complete, so the rows linger here. Without the sweep the
    /// next reconcile pass for the same agreement would not re-enter the
    /// fan-out path (the gate at `chain_listener.rs:494` only fires on a
    /// fresh transition, not on a no-op `AcceptedOnChain` row).
    ///
    /// `execute_pending_cancellations` is idempotent against
    /// already-canceled old agreements and against deleted pending rows,
    /// so feeding the same ID through it twice is safe.
    pub async fn list_executable_pending_cancellations(
        &self,
        limit: i64,
    ) -> Result<Vec<IndexingAgreementId>, Error> {
        let rows: Vec<(IndexingAgreementId,)> = sqlx::query_as(
            r#"
            SELECT DISTINCT pc.new_agreement_id
            FROM dipper_pending_cancellations pc
            INNER JOIN dipper_reg_indexing_agreements a
                ON a.id = pc.new_agreement_id
            WHERE a.status = $1
            ORDER BY pc.new_agreement_id
            LIMIT $2
            "#,
        )
        .bind(IndexingAgreementStatus::AcceptedOnChain)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}

/// Batched form of `update_status_from`: transitions all rows whose `id`
/// is in `agreement_ids` and whose current status is in `allowed_from` to
/// `new_status`, in one statement. Returns the ids of the rows that
/// actually flipped (matched the CAS guard) so callers can build per-id
/// outcome maps. Empty input is a fast-path no-op.
async fn batch_update_status_from(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    agreement_ids: &[IndexingAgreementId],
    new_status: IndexingAgreementStatus,
    allowed_from: &[IndexingAgreementStatus],
) -> Result<Vec<IndexingAgreementId>, Error> {
    if agreement_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = (0..allowed_from.len())
        .map(|i| format!("${}", i + 3))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET status = $1, updated_at = timezone('UTC', now())
        WHERE id = ANY($2) AND status IN ({placeholders})
        RETURNING id
        "#
    );
    let mut query = sqlx::query_as::<_, (IndexingAgreementId,)>(&sql)
        .bind(new_status)
        .bind(agreement_ids);
    for status in allowed_from {
        query = query.bind(*status);
    }
    let rows: Vec<(IndexingAgreementId,)> = query.fetch_all(&mut **tx).await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Transition an agreement's status inside a transaction. Thin wrapper
/// over `batch_update_status_from` for the single-row case.
/// Persist the accepted-transition audit payload for a single row. `COALESCE`
/// keeps any already-stored value rather than clobbering it with `NULL`.
async fn persist_accept_audit(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    id: &IndexingAgreementId,
    audit: &ReconciliationAudit,
) -> Result<(), Error> {
    if audit.accepted_at.is_none() && audit.accepted_tx.is_none() {
        return Ok(());
    }
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET accepted_at = COALESCE($2, accepted_at),
            accepted_tx = COALESCE($3, accepted_tx)
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(audit.accepted_at.map(|v| v as i64))
    .bind(audit.accepted_tx.as_deref())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Persist the cancel-transition audit payload for a single row. `COALESCE`
/// keeps any already-stored value rather than clobbering it with `NULL`.
async fn persist_cancel_audit(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    id: &IndexingAgreementId,
    audit: &ReconciliationAudit,
) -> Result<(), Error> {
    if audit.canceled_at.is_none() && audit.canceled_by.is_none() && audit.canceled_tx.is_none() {
        return Ok(());
    }
    sqlx::query(
        r#"
        UPDATE dipper_reg_indexing_agreements
        SET canceled_at = COALESCE($2, canceled_at),
            canceled_by = COALESCE($3, canceled_by),
            canceled_tx = COALESCE($4, canceled_tx)
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(audit.canceled_at.map(|v| v as i64))
    .bind(audit.canceled_by.as_deref())
    .bind(audit.canceled_tx.as_deref())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn update_status_from(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    agreement_id: &IndexingAgreementId,
    new_status: IndexingAgreementStatus,
    allowed_from: &[IndexingAgreementStatus],
) -> Result<bool, Error> {
    Ok(!batch_update_status_from(
        tx,
        std::slice::from_ref(agreement_id),
        new_status,
        allowed_from,
    )
    .await?
    .is_empty())
}
