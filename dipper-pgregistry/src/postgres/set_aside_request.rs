//! Studio indexing requests set aside because their subgraph manifest could not be read from
//! IPFS when their Kafka record was consumed, kept to be applied once it can.

use sqlx::Row as _;
use thegraph_core::{
    DeploymentId,
    alloy::primitives::{Address, ChainId},
};

use super::{
    PgRegistry,
    common::{PgAddress, PgDeploymentId, PgU64},
};
use crate::result::Error;

/// A Studio indexing request set aside until its subgraph manifest can be read. There is at
/// most 1 for each `requested_by`, `deployment_id` and `deployment_chain_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAsideRequest {
    pub requested_by: Address,
    pub deployment_id: DeploymentId,
    pub deployment_chain_id: ChainId,
    /// The number of indexers asked for, never 0: a cancel is not set aside.
    pub num_candidates: i32,
    /// Where the request's record sits in Kafka.
    pub topic: String,
    pub partition_id: i32,
    pub record_offset: i64,
    /// The id Studio gave the request's event.
    pub event_id: String,
    /// Why the manifest could not be read, as of the last attempt.
    pub reason: String,
    /// How many times the manifest was read again since the request was set aside.
    pub attempts: i32,
    /// Seconds since the request was set aside.
    pub waited_secs: i64,
}

/// The columns every query here returns a [`SetAsideRequest`] from.
macro_rules! columns {
    () => {
        "requested_by, deployment_id, deployment_chain_id, num_candidates, topic, partition_id, \
         record_offset, event_id, reason, attempts, \
         FLOOR(EXTRACT(EPOCH FROM (now() - set_aside_at)))::BIGINT AS waited_secs"
    };
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for SetAsideRequest {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self, sqlx::Error> {
        let PgAddress(requested_by) = row.try_get("requested_by")?;
        let PgDeploymentId(deployment_id) = row.try_get("deployment_id")?;
        let PgU64(deployment_chain_id) = row.try_get("deployment_chain_id")?;
        Ok(Self {
            requested_by,
            deployment_id,
            deployment_chain_id,
            num_candidates: row.try_get("num_candidates")?,
            topic: row.try_get("topic")?,
            partition_id: row.try_get("partition_id")?,
            record_offset: row.try_get("record_offset")?,
            event_id: row.try_get("event_id")?,
            reason: row.try_get("reason")?,
            attempts: row.try_get("attempts")?,
            waited_secs: row.try_get("waited_secs")?,
        })
    }
}

impl PgRegistry {
    /// Set `request` aside, its manifest to be read again in `retry_in_secs`, in place of any
    /// request set aside for the same requester, deployment and chain. Returns `false`, changing
    /// nothing, when that same event's record is already set aside, as when it is delivered twice.
    pub async fn set_aside_studio_request(
        &self,
        request: &SetAsideRequest,
        retry_in_secs: i64,
    ) -> Result<bool, Error> {
        let result = sqlx::query(
            r#"
            INSERT INTO dipper_studio_set_aside_requests AS stored (
                requested_by, deployment_id, deployment_chain_id, num_candidates,
                topic, partition_id, record_offset, event_id, reason, next_attempt_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now() + make_interval(secs => $10::DOUBLE PRECISION))
            ON CONFLICT (requested_by, deployment_id, deployment_chain_id)
            DO UPDATE SET
                num_candidates = EXCLUDED.num_candidates,
                topic = EXCLUDED.topic,
                partition_id = EXCLUDED.partition_id,
                record_offset = EXCLUDED.record_offset,
                event_id = EXCLUDED.event_id,
                reason = EXCLUDED.reason,
                set_aside_at = now(),
                attempts = 0,
                next_attempt_at = EXCLUDED.next_attempt_at,
                ready = FALSE,
                still_waiting_alerted_at = NULL
            WHERE (stored.topic, stored.partition_id, stored.record_offset, stored.event_id)
                IS DISTINCT FROM
                (EXCLUDED.topic, EXCLUDED.partition_id, EXCLUDED.record_offset, EXCLUDED.event_id)
            "#,
        )
        .bind(PgAddress(request.requested_by))
        .bind(PgDeploymentId(request.deployment_id))
        .bind(PgU64(request.deployment_chain_id))
        .bind(request.num_candidates)
        .bind(&request.topic)
        .bind(request.partition_id)
        .bind(request.record_offset)
        .bind(&request.event_id)
        .bind(&request.reason)
        .bind(seconds(retry_in_secs))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Remove the request set aside for a requester, deployment and chain, whichever record it
    /// came from. Returns whether there was one.
    pub async fn forget_set_aside_studio_request(
        &self,
        requested_by: Address,
        deployment_id: DeploymentId,
        deployment_chain_id: ChainId,
    ) -> Result<bool, Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM dipper_studio_set_aside_requests
            WHERE requested_by = $1 AND deployment_id = $2 AND deployment_chain_id = $3
            "#,
        )
        .bind(PgAddress(requested_by))
        .bind(PgDeploymentId(deployment_id))
        .bind(PgU64(deployment_chain_id))
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Remove `request` now that it is dealt with. Returns `false`, changing nothing, if it was
    /// removed or replaced by a later record since it was listed.
    pub async fn forget_set_aside_studio_record(
        &self,
        request: &SetAsideRequest,
    ) -> Result<bool, Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM dipper_studio_set_aside_requests
            WHERE requested_by = $1 AND deployment_id = $2 AND deployment_chain_id = $3
              AND topic = $4 AND partition_id = $5 AND record_offset = $6 AND event_id = $7
            "#,
        )
        .bind(PgAddress(request.requested_by))
        .bind(PgDeploymentId(request.deployment_id))
        .bind(PgU64(request.deployment_chain_id))
        .bind(&request.topic)
        .bind(request.partition_id)
        .bind(request.record_offset)
        .bind(&request.event_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// The requests set aside from `topic` whose manifest is due another read, the longest
    /// overdue first.
    pub async fn get_due_set_aside_studio_requests(
        &self,
        topic: &str,
    ) -> Result<Vec<SetAsideRequest>, Error> {
        let due = sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM dipper_studio_set_aside_requests \
             WHERE topic = $1 AND NOT ready AND next_attempt_at <= now() \
             ORDER BY next_attempt_at, record_offset"
        ))
        .bind(topic)
        .fetch_all(&self.pool)
        .await?;
        Ok(due)
    }

    /// The requests set aside from a partition of `topic` whose manifest could be read, in the
    /// order their records were consumed.
    pub async fn get_ready_set_aside_studio_requests(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Result<Vec<SetAsideRequest>, Error> {
        let ready = sqlx::query_as(concat!(
            "SELECT ",
            columns!(),
            " FROM dipper_studio_set_aside_requests \
             WHERE topic = $1 AND partition_id = $2 AND ready \
             ORDER BY record_offset"
        ))
        .bind(topic)
        .bind(partition_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(ready)
    }

    /// Mark `request` ready to apply, now that its manifest could be read. Changes nothing if
    /// the request was removed or replaced by a later record since it was listed.
    pub async fn mark_set_aside_studio_request_ready(
        &self,
        request: &SetAsideRequest,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_studio_set_aside_requests
            SET ready = TRUE
            WHERE requested_by = $1 AND deployment_id = $2 AND deployment_chain_id = $3
              AND topic = $4 AND partition_id = $5 AND record_offset = $6 AND event_id = $7
            "#,
        )
        .bind(PgAddress(request.requested_by))
        .bind(PgDeploymentId(request.deployment_id))
        .bind(PgU64(request.deployment_chain_id))
        .bind(&request.topic)
        .bind(request.partition_id)
        .bind(request.record_offset)
        .bind(&request.event_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Count 1 more failed read of `request`'s manifest, for `reason`, and leave the request
    /// waiting for the next read in `retry_in_secs`. Changes nothing if the request was removed
    /// or replaced by a later record since it was listed.
    pub async fn retry_set_aside_studio_request_later(
        &self,
        request: &SetAsideRequest,
        reason: &str,
        retry_in_secs: i64,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE dipper_studio_set_aside_requests
            SET attempts = attempts + 1,
                reason = $7,
                next_attempt_at = now() + make_interval(secs => $8::DOUBLE PRECISION),
                ready = FALSE
            WHERE requested_by = $1 AND deployment_id = $2 AND deployment_chain_id = $3
              AND topic = $4 AND partition_id = $5 AND record_offset = $6 AND event_id = $9
            "#,
        )
        .bind(PgAddress(request.requested_by))
        .bind(PgDeploymentId(request.deployment_id))
        .bind(PgU64(request.deployment_chain_id))
        .bind(&request.topic)
        .bind(request.partition_id)
        .bind(request.record_offset)
        .bind(reason)
        .bind(seconds(retry_in_secs))
        .bind(&request.event_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The requests set aside from `topic` at least `waiting_secs` ago that have not been
    /// alerted on as still waiting. Each is recorded as alerted on in the same statement, so
    /// it is returned once, whatever restarts in between.
    pub async fn claim_still_waiting_studio_requests(
        &self,
        topic: &str,
        waiting_secs: i64,
    ) -> Result<Vec<SetAsideRequest>, Error> {
        let claimed = sqlx::query_as(concat!(
            "UPDATE dipper_studio_set_aside_requests \
             SET still_waiting_alerted_at = now() \
             WHERE topic = $1 AND still_waiting_alerted_at IS NULL \
               AND set_aside_at <= now() - make_interval(secs => $2::DOUBLE PRECISION) \
             RETURNING ",
            columns!()
        ))
        .bind(topic)
        .bind(seconds(waiting_secs))
        .fetch_all(&self.pool)
        .await?;
        Ok(claimed)
    }
}

/// A count of seconds as `make_interval` takes it. Every count passed here is far below 2^53,
/// up to which a float holds each whole number exactly.
fn seconds(secs: i64) -> f64 {
    secs as f64
}
