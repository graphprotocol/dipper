//! Shared test utilities for the dipper-service crate.
//!
//! Currently provides [`CapturingEventsProducer`], a test double for
//! [`SubgraphIndexingAgreementEventsProducer`] that records every produced event
//! so handler/service tests can assert exactly which lifecycle events were emitted
//! (and with which payloads) without a Kafka backend, and [`logged`], which captures what
//! dipper logs.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use dipper_producer::{
    events::{EmitError, SubgraphIndexingAgreementEventsProducer},
    proto,
};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};

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

/// What dipper logs, at every level, while `run` runs.
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
    let _logging = tracing::subscriber::set_default(subscriber);
    run.await;
    String::from_utf8(lines.lock().unwrap().clone()).unwrap()
}
