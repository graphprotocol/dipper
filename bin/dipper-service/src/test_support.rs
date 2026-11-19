//! Shared test utilities for the dipper-service crate.
//!
//! Currently provides [`CapturingEventsProducer`], a test double for
//! [`SubgraphIndexingAgreementEventsProducer`] that records every produced event
//! so handler/service tests can assert exactly which lifecycle events were emitted
//! (and with which payloads) without a Kafka backend, and [`logged`], which captures what
//! dipper logs.

use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use dipper_producer::{
    events::{EmitError, SubgraphIndexingAgreementEventsProducer},
    proto,
};
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use tracing::subscriber::Interest;

use crate::request_chain_check::{ChainCheckError, RequestChainCheck};

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

/// A chain check that turns every request away for 1 reason, and counts how often it was asked.
#[derive(Default)]
pub struct RefusingChainCheck {
    why: Refusal,
    calls: AtomicUsize,
}

/// Why a [`RefusingChainCheck`] turns requests away.
#[derive(Default, Clone, Copy)]
enum Refusal {
    /// The manifest indexes another chain.
    #[default]
    WrongChain,
    /// The manifest names several networks.
    SeveralNetworks,
    /// IPFS answers that it can't find the manifest.
    NotFound,
    /// IPFS is down.
    Unavailable,
}

impl RefusingChainCheck {
    pub fn wrong_chain() -> Self {
        Self::default()
    }

    pub fn several_networks() -> Self {
        Self::refusing(Refusal::SeveralNetworks)
    }

    pub fn not_found() -> Self {
        Self::refusing(Refusal::NotFound)
    }

    pub fn unavailable() -> Self {
        Self::refusing(Refusal::Unavailable)
    }

    fn refusing(why: Refusal) -> Self {
        Self {
            why,
            ..Self::default()
        }
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RequestChainCheck for RefusingChainCheck {
    async fn check(
        &self,
        _deployment: &DeploymentId,
        requested_chain_id: ChainId,
    ) -> Result<(), ChainCheckError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(match self.why {
            Refusal::WrongChain => ChainCheckError::Mismatch {
                network: "arbitrum-sepolia".to_string(),
                manifest_chain_id: 421614,
                requested_chain_id,
            },
            Refusal::SeveralNetworks => ChainCheckError::Unresolvable(
                "the subgraph manifest's data sources name more than 1 network: gnosis, mainnet"
                    .to_string(),
            ),
            Refusal::NotFound => ChainCheckError::NotFound(
                "IPFS has no subgraph manifest for this deployment (HTTP 404)".to_string(),
            ),
            Refusal::Unavailable => ChainCheckError::Unavailable("IPFS is down".to_string()),
        })
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

/// A local URL that refuses connections, carrying `key` in its path the way a gateway URL can
/// carry its API key.
pub fn refused_url_with_key(key: &str) -> url::Url {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("http://127.0.0.1:{port}/api/{key}/subgraphs/id/test")
        .parse()
        .unwrap()
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

/// The value of the series `name` with all of `labels` in rendered Prometheus text, if present.
/// Matches labels in any order, so a test does not depend on how the exporter orders them.
pub fn metric_value(rendered: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    rendered.lines().find_map(|line| {
        let rest = line.strip_prefix(name)?;
        let (series_labels, value) = match rest.strip_prefix('{') {
            Some(rest) => rest.split_once("} ")?,
            None => ("", rest.strip_prefix(' ')?),
        };
        let has_all = labels.iter().all(|(key, val)| {
            series_labels
                .split(',')
                .any(|pair| pair == format!(r#"{key}="{val}""#))
        });
        let count = if series_labels.is_empty() {
            0
        } else {
            series_labels.split(',').count()
        };
        (has_all && count == labels.len()).then(|| value.trim().parse().ok())?
    })
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
