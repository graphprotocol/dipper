//! Periodic reassignment service for re-evaluating indexing requests
//!
//! This module provides a background service that periodically reassesses open indexing
//! requests against current network conditions. It queues `ReassessIndexingRequest`
//! messages through the worker queue, which diff the IISA target state against current
//! active agreements to determine necessary adds and cancellations.

use std::{future::Future, time::Duration};

use time::OffsetDateTime;
use tokio::{sync::mpsc, time::MissedTickBehavior};

use crate::{
    config::ReassignmentConfig,
    registry::IndexingRequestRegistry,
    worker::service::{JobPriority, WorkerQueue},
};

/// Calculate the duration until the next occurrence of the target UTC hour.
fn duration_until_utc_hour(target_hour: u8) -> Duration {
    let now = OffsetDateTime::now_utc();
    let current_hour = now.hour();
    let current_minute = now.minute();
    let current_second = now.second();

    let hours_until = if current_hour < target_hour {
        target_hour - current_hour
    } else if current_hour == target_hour && current_minute == 0 && current_second == 0 {
        0 // Exactly at target hour
    } else {
        // Past target hour today, wait until tomorrow
        24 - current_hour + target_hour
    };

    let seconds_into_current_hour = (current_minute as u64) * 60 + (current_second as u64);
    let total_seconds = (hours_until as u64) * 3600 - seconds_into_current_hour;

    Duration::from_secs(total_seconds)
}

/// Handle for controlling the reassignment service lifecycle
#[derive(Clone)]
pub struct Handle {
    tx_stop: mpsc::Sender<()>,
}

impl Handle {
    /// Stop the reassignment service gracefully
    pub async fn stop(&self) {
        if self.tx_stop.is_closed() {
            return;
        }

        let _ = self.tx_stop.send(()).await;
        self.tx_stop.closed().await;
    }
}

/// Context required by the reassignment service
pub struct Ctx<R, W> {
    /// Registry for querying indexing requests
    pub registry: R,
    /// Worker queue for submitting reassessment jobs
    pub worker_queue: W,
    /// Service configuration
    pub config: ReassignmentConfig,
}

/// Create a new reassignment service
///
/// Returns a handle for controlling the service and a future that must be spawned
/// on a runtime. The service periodically queries for open indexing requests older
/// than the configured minimum age and queues them for reassessment.
#[expect(
    clippy::cast_possible_wrap,
    clippy::too_many_lines,
    reason = "predates this lint; fix when next touched"
)]
pub fn new<R, W>(ctx: Ctx<R, W>) -> (Handle, impl Future<Output = anyhow::Result<()>>)
where
    R: IndexingRequestRegistry + Send + Sync,
    W: WorkerQueue + Send + Sync,
{
    let (tx_stop, mut rx_stop) = mpsc::channel(1);

    let Ctx {
        registry,
        worker_queue,
        config,
    } = ctx;

    let service = async move {
        // Calculate initial delay until target UTC hour
        let initial_delay = duration_until_utc_hour(config.run_at_utc_hour);

        tracing::info!(
            run_at_utc_hour = config.run_at_utc_hour,
            initial_delay_secs = initial_delay.as_secs(),
            interval_secs = config.interval.as_secs(),
            batch_size = config.batch_size,
            min_age_secs = config.min_request_age.as_secs(),
            "reassignment service started, waiting for first cycle"
        );

        // Wait until the target hour (or handle early shutdown)
        tokio::select! {
            _ = rx_stop.recv() => {
                tracing::debug!("reassignment service stopped before first cycle");
                return Ok(());
            }
            _ = tokio::time::sleep(initial_delay) => {}
        }

        // Set up interval timer for subsequent cycles
        let mut timer = tokio::time::interval(config.interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Skip the first immediate tick (we just ran the initial delay)
        timer.tick().await;

        // Timeouts for individual operations to prevent hangs
        const DB_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
        const QUEUE_PUSH_TIMEOUT: Duration = Duration::from_secs(10);

        let mut first_cycle = true;

        loop {
            // Wait for timer on subsequent cycles (first cycle runs immediately after initial delay)
            if !first_cycle {
                tokio::select! {
                    _ = rx_stop.recv() => break,
                    _ = timer.tick() => {},
                }
            }
            first_cycle = false;

            tracing::debug!("starting reassessment cycle");

            let min_age_seconds = config.min_request_age.as_secs() as i64;

            // Query open requests eligible for reassessment (with timeout)
            let query_result = tokio::time::timeout(
                DB_QUERY_TIMEOUT,
                registry.get_open_indexing_requests_for_reassessment(
                    min_age_seconds,
                    config.batch_size,
                ),
            )
            .await;

            let requests = match query_result {
                Ok(Ok(requests)) => requests,
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to query indexing requests for reassessment");
                    continue;
                }
                Err(_) => {
                    tracing::error!("timeout querying indexing requests for reassessment");
                    continue;
                }
            };

            if requests.is_empty() {
                tracing::debug!("reassessment cycle: no eligible requests");
                continue;
            }

            tracing::info!(
                count = requests.len(),
                "reassessment cycle: processing requests"
            );

            let mut queued = 0;
            let mut failed = 0;
            let mut timed_out = 0;

            for request in requests {
                // Check for shutdown between queue pushes to stay responsive
                if rx_stop.try_recv().is_ok() {
                    tracing::debug!("reassignment service stopping mid-cycle");
                    return Ok(());
                }

                let push_result = tokio::time::timeout(
                    QUEUE_PUSH_TIMEOUT,
                    worker_queue.reassess_indexing_request(
                        request.id,
                        request.deployment_id,
                        request.deployment_chain_id,
                        request.num_candidates,
                        // Background: the periodic sweep yields to interactive work.
                        JobPriority::Background,
                    ),
                )
                .await;

                match push_result {
                    Ok(Ok(_job_id)) => {
                        queued += 1;
                        tracing::debug!(
                            request_id = %request.id,
                            deployment_id = %request.deployment_id,
                            "queued request for reassessment"
                        );
                    }
                    Ok(Err(err)) => {
                        failed += 1;
                        tracing::warn!(
                            request_id = %request.id,
                            error = %err,
                            "failed to queue request for reassessment"
                        );
                    }
                    Err(_) => {
                        timed_out += 1;
                        tracing::warn!(
                            request_id = %request.id,
                            "timeout queuing request for reassessment"
                        );
                    }
                }
            }

            tracing::info!(
                queued = queued,
                failed = failed,
                timed_out = timed_out,
                "reassessment cycle completed"
            );
        }

        tracing::debug!("reassignment service stopped");
        Ok(())
    };

    (Handle { tx_stop }, service)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReassignmentConfig::default();
        assert!(config.enabled);
        assert_eq!(config.interval, Duration::from_secs(86400)); // 24 hours
        assert_eq!(config.run_at_utc_hour, 10); // 10:00 UTC
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.min_request_age, Duration::from_secs(86400));
    }

    #[test]
    fn test_duration_until_utc_hour() {
        // This is a basic sanity check - the actual delay depends on current time
        let delay = duration_until_utc_hour(2);
        // Should always be less than 24 hours
        assert!(delay.as_secs() < 86400);
    }
}
