//! Shared test utilities for the dipper-service crate.
//!
//! Currently provides [`CapturingEventsProducer`], a test double for
//! [`SubgraphIndexingAgreementEventsProducer`] that records every produced event
//! so handler/service tests can assert exactly which lifecycle events were emitted
//! (and with which payloads) without a Kafka backend, and [`logged`], which captures what
//! dipper logs.

use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use dipper_producer::{
    events::{EmitError, SubgraphIndexingAgreementEventsProducer},
    proto,
};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use tracing::subscriber::Interest;

/// One captured lifecycle event: the routing metadata plus the typed payload.
#[derive(Clone, Debug, PartialEq)]
pub enum CapturedEvent {
    RequestReceived {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    },
    Proposed {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    },
    Accepted {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    },
    RequestExpired {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    },
    NIndexersUnavailable {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    },
    Terminated {
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    },
}

/// A [`SubgraphIndexingAgreementEventsProducer`] that records produced events into
/// a shared buffer for assertions. Clone shares the same buffer.
#[derive(Clone, Default)]
pub struct CapturingEventsProducer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl CapturingEventsProducer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of all events captured so far, in production order.
    pub fn events(&self) -> Vec<CapturedEvent> {
        self.events.lock().expect("events lock poisoned").clone()
    }

    fn push(&self, event: CapturedEvent) {
        self.events
            .lock()
            .expect("events lock poisoned")
            .push(event);
    }
}

#[async_trait]
impl SubgraphIndexingAgreementEventsProducer for CapturingEventsProducer {
    fn produce_subgraph_indexing_agreement_request_received(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestReceived,
    ) {
        self.push(CapturedEvent::RequestReceived {
            deployment,
            chain_id,
            event,
        });
    }

    fn produce_subgraph_indexing_agreement_proposed(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementProposed,
    ) {
        self.push(CapturedEvent::Proposed {
            deployment,
            chain_id,
            event,
        });
    }

    async fn emit_subgraph_indexing_agreement_accepted(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementAccepted,
    ) -> Result<(), EmitError> {
        self.push(CapturedEvent::Accepted {
            deployment,
            chain_id,
            event,
        });
        Ok(())
    }

    async fn emit_subgraph_indexing_agreement_request_expired(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementRequestExpired,
    ) -> Result<(), EmitError> {
        self.push(CapturedEvent::RequestExpired {
            deployment,
            chain_id,
            event,
        });
        Ok(())
    }

    fn produce_subgraph_indexing_agreement_n_indexers_unavailable(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementNIndexersUnavailable,
    ) {
        self.push(CapturedEvent::NIndexersUnavailable {
            deployment,
            chain_id,
            event,
        });
    }

    async fn emit_subgraph_indexing_agreement_terminated(
        &self,
        deployment: DeploymentId,
        chain_id: ChainId,
        event: proto::SubgraphIndexingAgreementTerminated,
    ) -> Result<(), EmitError> {
        self.push(CapturedEvent::Terminated {
            deployment,
            chain_id,
            event,
        });
        Ok(())
    }
}

/// What dipper logs, at DEBUG and above, while `run` runs.
pub async fn logged(run: impl std::future::Future<Output = ()>) -> String {
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let lines = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&lines);
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || Buffer(Arc::clone(&writer)))
        .finish();
    keep_every_log_line_askable();
    let _logging = tracing::subscriber::set_default(subscriber);
    run.await;
    String::from_utf8(lines.lock().unwrap().clone()).unwrap()
}

/// Installs, once per test binary, a global subscriber that answers "ask each
/// time" for every log line and then declines them all. `tracing` caches that
/// answer per log line for the whole process the first time any thread reaches
/// the line, and with no global subscriber the answer is "never": a test that
/// captures nothing can reach a line first and cache it as off, so a test
/// capturing logs on another thread never sees it.
fn keep_every_log_line_askable() {
    static INSTALLED: OnceLock<bool> = OnceLock::new();
    let installed =
        *INSTALLED.get_or_init(|| tracing::subscriber::set_global_default(Askable).is_ok());
    assert!(
        installed,
        "the tests install no other global tracing subscriber; capture logs through `logged`"
    );
}

/// Answers "ask each time" for every log line and then declines them all, so it
/// never sees a span or event; `new_span` is unreachable because `enabled` is
/// always false.
struct Askable;

impl tracing::Subscriber for Askable {
    fn register_callsite(&self, _: &'static tracing::Metadata<'static>) -> Interest {
        Interest::sometimes()
    }
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[cfg(test)]
mod tests {
    use super::logged;

    fn reach_a_shared_line() {
        tracing::info!("A line another thread reached first");
    }

    #[tokio::test]
    async fn captures_a_line_that_a_thread_capturing_nothing_reached_first() {
        let logs = logged(async {
            std::thread::spawn(reach_a_shared_line).join().unwrap();
            reach_a_shared_line();
        })
        .await;

        assert!(
            logs.contains("A line another thread reached first"),
            "{logs}"
        );
    }
}
