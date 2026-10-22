//! Check that an indexing request names the chain its subgraph indexes. The chain
//! decides prices and indexers and nothing downstream checks it, so the manifest's
//! network is looked up (registry, then `additional_networks`) and compared.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use dipper_core::manifest::{
    ManifestFetchError, chain_id_for_network, fetch_manifest, ipfs_cat_url, network_from_manifest,
};
use graph_networks_registry::NetworksRegistry;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::config::RequestChainCheckConfig;

/// Deployments whose manifest result is remembered. A manifest never changes, so a
/// hit needs no expiry; past this many the cache starts again from empty.
const MAX_CACHED_MANIFESTS: usize = 10_000;

#[derive(Debug, thiserror::Error)]
pub enum ChainCheckError {
    /// The manifest indexes another chain: the request is wrong and retrying won't help.
    #[error(
        "the subgraph manifest indexes {network} (chain ID {manifest_chain_id}), \
         not chain ID {requested_chain_id}"
    )]
    Mismatch {
        network: String,
        manifest_chain_id: ChainId,
        requested_chain_id: ChainId,
    },
    /// There is no manifest, or it can't be tied to a single chain ID; a retry won't change that.
    #[error("{0}")]
    Unresolvable(String),
    /// IPFS did not return the manifest; the same request may pass later.
    #[error("could not read the subgraph manifest from IPFS: {0}")]
    Unavailable(String),
}

impl ChainCheckError {
    /// Whether the same request could pass once IPFS answers.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

#[async_trait]
pub trait RequestChainCheck: Send + Sync {
    /// `Ok` when `deployment`'s manifest indexes `chain_id`.
    async fn check(
        &self,
        deployment: &DeploymentId,
        chain_id: ChainId,
    ) -> Result<(), ChainCheckError>;
}

/// Lets every request through, for setups whose manifests can't be read.
pub struct SkipChainCheck;

#[async_trait]
impl RequestChainCheck for SkipChainCheck {
    async fn check(&self, _: &DeploymentId, _: ChainId) -> Result<(), ChainCheckError> {
        Ok(())
    }
}

/// Reads each deployment's manifest from IPFS once and compares its chain.
pub struct ManifestChainCheck {
    http: reqwest::Client,
    ipfs_cat: Url,
    timeout: Duration,
    registry: Arc<NetworksRegistry>,
    additional_networks: Arc<BTreeMap<ChainId, String>>,
    /// Each deployment's network, or why its manifest names none.
    manifests: Mutex<HashMap<DeploymentId, Result<String, String>>>,
}

impl ManifestChainCheck {
    pub fn new(
        config: &RequestChainCheckConfig,
        registry: Arc<NetworksRegistry>,
        additional_networks: Arc<BTreeMap<ChainId, String>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http: reqwest::Client::new(),
            ipfs_cat: ipfs_cat_url(config.ipfs_url.clone())?,
            timeout: config.timeout,
            registry,
            additional_networks,
            manifests: Mutex::new(HashMap::new()),
        })
    }

    async fn network(&self, deployment: &DeploymentId) -> Result<String, ChainCheckError> {
        let cached = self
            .manifests
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(deployment)
            .cloned();
        if let Some(result) = cached {
            return result.map_err(ChainCheckError::Unresolvable);
        }
        let result =
            match fetch_manifest(&self.http, &self.ipfs_cat, deployment, self.timeout).await {
                Ok(manifest) => network_from_manifest(&manifest).map_err(|err| error_chain(&err)),
                // A manifest IPFS doesn't have may yet be added, so only its contents are remembered.
                Err(err @ ManifestFetchError::NotFound(_)) => {
                    return Err(ChainCheckError::Unresolvable(err.to_string()));
                }
                Err(err) if err.is_transient() => {
                    return Err(ChainCheckError::Unavailable(error_chain(&err)));
                }
                Err(err) => Err(err.to_string()),
            };
        let mut manifests = self.manifests.lock().unwrap_or_else(|p| p.into_inner());
        if manifests.len() >= MAX_CACHED_MANIFESTS {
            manifests.clear();
        }
        manifests.insert(*deployment, result.clone());
        result.map_err(ChainCheckError::Unresolvable)
    }

    /// The chain ID of a manifest network: the registry first, then the
    /// `additional_networks` names configured for local chains.
    fn chain_id(&self, network: &str) -> Result<ChainId, ChainCheckError> {
        match chain_id_for_network(&self.registry, network) {
            Ok(chain_id) => Ok(chain_id),
            Err(err) => self
                .additional_networks
                .iter()
                .find_map(|(chain_id, name)| (name == network).then_some(*chain_id))
                .ok_or_else(|| ChainCheckError::Unresolvable(err.to_string())),
        }
    }
}

#[async_trait]
impl RequestChainCheck for ManifestChainCheck {
    async fn check(
        &self,
        deployment: &DeploymentId,
        requested_chain_id: ChainId,
    ) -> Result<(), ChainCheckError> {
        let network = self.network(deployment).await?;
        let manifest_chain_id = self.chain_id(&network)?;
        if manifest_chain_id == requested_chain_id {
            Ok(())
        } else {
            Err(ChainCheckError::Mismatch {
                network,
                manifest_chain_id,
                requested_chain_id,
            })
        }
    }
}

/// An error and its causes on 1 line, since the RPC caller sees only the message.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    message
}

#[cfg(test)]
mod tests {
    use dipper_core::manifest::TEST_REGISTRY_JSON;
    use thegraph_core::deployment_id;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;

    const DEPLOYMENT: &str = "QmQ5w3LqJdBGZvHNYTWq7np2B1qbBQMpHP77ZKQPzGVTrg";

    fn manifest(network: &str) -> String {
        format!("specVersion: 1.0.0\ndataSources:\n  - kind: ethereum\n    network: {network}\n")
    }

    /// A check against `server`, which answers manifest reads for [`DEPLOYMENT`].
    fn check_against(server: &MockServer) -> ManifestChainCheck {
        let config = RequestChainCheckConfig {
            enabled: true,
            ipfs_url: Url::parse(&format!("{}/ipfs", server.uri())).unwrap(),
            timeout: Duration::from_secs(2),
        };
        ManifestChainCheck::new(
            &config,
            Arc::new(NetworksRegistry::from_json(TEST_REGISTRY_JSON).unwrap()),
            Arc::new(BTreeMap::from([(1337, "hardhat".to_string())])),
        )
        .unwrap()
    }

    /// Answer every manifest read for [`DEPLOYMENT`] with `response`, `times` times in all.
    async fn serve_manifest(server: &MockServer, response: ResponseTemplate, times: u64) {
        Mock::given(method("POST"))
            .and(path("/ipfs/api/v0/cat"))
            .and(query_param("arg", DEPLOYMENT))
            .respond_with(response)
            .expect(times)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn passes_a_request_on_the_manifest_chain() {
        let server = MockServer::start().await;
        let body = ResponseTemplate::new(200).set_body_string(manifest("arbitrum-sepolia"));
        serve_manifest(&server, body, 1).await;

        let result = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 421614)
            .await;

        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn refuses_a_request_on_another_chain() {
        let server = MockServer::start().await;
        let body = ResponseTemplate::new(200).set_body_string(manifest("arbitrum-sepolia"));
        serve_manifest(&server, body, 1).await;

        let err = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 1)
            .await
            .unwrap_err();

        assert!(
            matches!(
                &err,
                ChainCheckError::Mismatch {
                    network,
                    manifest_chain_id: 421614,
                    requested_chain_id: 1,
                } if network == "arbitrum-sepolia"
            ),
            "{err:?}"
        );
        assert!(!err.is_transient());
    }

    #[tokio::test]
    async fn resolves_registry_aliases_and_configured_local_chains() {
        let server = MockServer::start().await;
        let check = check_against(&server);

        assert_eq!(check.chain_id("ethereum").unwrap(), 1);
        assert_eq!(check.chain_id("hardhat").unwrap(), 1337);
        assert!(matches!(
            check.chain_id("not-a-network"),
            Err(ChainCheckError::Unresolvable(_))
        ));
    }

    #[tokio::test]
    async fn a_missing_manifest_is_final_but_read_again_next_time() {
        //* Arrange - IPFS answers 404 once it has searched for the manifest
        let server = MockServer::start().await;
        serve_manifest(&server, ResponseTemplate::new(404), 2).await;
        let check = check_against(&server);

        //* Act
        let first = check.check(&deployment_id!(DEPLOYMENT), 1).await;
        let second = check.check(&deployment_id!(DEPLOYMENT), 1).await;

        //* Assert - final, so the consumer moves on; not remembered, as it may yet be added
        assert!(
            matches!(first, Err(ChainCheckError::Unresolvable(_))),
            "{first:?}"
        );
        assert!(
            matches!(second, Err(ChainCheckError::Unresolvable(_))),
            "{second:?}"
        );
    }

    #[tokio::test]
    async fn a_failing_ipfs_is_worth_retrying() {
        let server = MockServer::start().await;
        serve_manifest(&server, ResponseTemplate::new(502), 1).await;

        let err = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 421614)
            .await
            .unwrap_err();

        assert!(err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    async fn a_slow_ipfs_is_worth_retrying() {
        let server = MockServer::start().await;
        let slow = ResponseTemplate::new(200)
            .set_body_string(manifest("mainnet"))
            .set_delay(Duration::from_secs(10));
        serve_manifest(&server, slow, 1).await;

        let err = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 1)
            .await
            .unwrap_err();

        assert!(err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    async fn reads_each_manifest_from_ipfs_once() {
        let server = MockServer::start().await;
        let body = ResponseTemplate::new(200).set_body_string(manifest("mainnet"));
        serve_manifest(&server, body, 1).await;
        let check = check_against(&server);

        for _ in 0..3 {
            check.check(&deployment_id!(DEPLOYMENT), 1).await.unwrap();
        }
        // The mock's `expect` is verified when the server drops.
    }

    #[tokio::test]
    async fn remembers_a_manifest_that_names_no_single_network() {
        let server = MockServer::start().await;
        let body = ResponseTemplate::new(200).set_body_string("dataSources:\n  - kind: ethereum\n");
        serve_manifest(&server, body, 1).await;
        let check = check_against(&server);

        for _ in 0..2 {
            let err = check
                .check(&deployment_id!(DEPLOYMENT), 1)
                .await
                .unwrap_err();
            assert!(matches!(err, ChainCheckError::Unresolvable(_)), "{err:?}");
        }
    }
}
