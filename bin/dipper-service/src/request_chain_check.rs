//! Check that an indexing request names the chain its subgraph indexes. The chain
//! decides prices and indexers and nothing downstream checks it, so the manifest's
//! network is looked up (registry, then `additional_networks`) and compared.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use dipper_core::manifest::{chain_id_for_network, network_from_manifest};
use graph_networks_registry::NetworksRegistry;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

use crate::config::RequestChainCheckConfig;

/// Larger manifests are refused rather than read into memory: real ones are a few KiB.
const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;

/// Deployments whose network is remembered. A manifest never changes, so a hit
/// needs no expiry; past this many the cache starts again from empty.
const MAX_CACHED_NETWORKS: usize = 10_000;

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
    /// The manifest can't be tied to a single chain ID, which won't change on a retry.
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
    registry: Arc<NetworksRegistry>,
    additional_networks: Arc<BTreeMap<ChainId, String>>,
    networks: Mutex<HashMap<DeploymentId, String>>,
}

impl ManifestChainCheck {
    pub fn new(
        config: &RequestChainCheckConfig,
        registry: Arc<NetworksRegistry>,
        additional_networks: Arc<BTreeMap<ChainId, String>>,
    ) -> anyhow::Result<Self> {
        let mut ipfs_url = config.ipfs_url.clone();
        // Without the trailing slash, `join` would replace the last path segment.
        if !ipfs_url.path().ends_with('/') {
            ipfs_url.set_path(&format!("{}/", ipfs_url.path()));
        }
        let http = reqwest::Client::builder().timeout(config.timeout).build()?;
        Ok(Self {
            http,
            ipfs_cat: ipfs_url.join("api/v0/cat")?,
            registry,
            additional_networks,
            networks: Mutex::new(HashMap::new()),
        })
    }

    async fn network(&self, deployment: &DeploymentId) -> Result<String, ChainCheckError> {
        if let Some(network) = self.cached(deployment) {
            return Ok(network);
        }
        let manifest = self.fetch_manifest(deployment).await?;
        let network = network_from_manifest(&manifest)
            .map_err(|err| ChainCheckError::Unresolvable(error_chain(&err)))?;
        let mut networks = self.networks.lock().unwrap_or_else(|p| p.into_inner());
        if networks.len() >= MAX_CACHED_NETWORKS {
            networks.clear();
        }
        networks.insert(*deployment, network.clone());
        Ok(network)
    }

    fn cached(&self, deployment: &DeploymentId) -> Option<String> {
        let networks = self.networks.lock().unwrap_or_else(|p| p.into_inner());
        networks.get(deployment).cloned()
    }

    async fn fetch_manifest(&self, deployment: &DeploymentId) -> Result<String, ChainCheckError> {
        let unavailable = |err: reqwest::Error| ChainCheckError::Unavailable(error_chain(&err));
        let mut url = self.ipfs_cat.clone();
        url.query_pairs_mut()
            .append_pair("arg", &deployment.to_string());
        // The IPFS RPC API only answers POST.
        let mut response = self
            .http
            .post(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(unavailable)?;
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(unavailable)? {
            if body.len() + chunk.len() > MAX_MANIFEST_BYTES {
                return Err(ChainCheckError::Unresolvable(format!(
                    "the subgraph manifest is larger than {MAX_MANIFEST_BYTES} bytes"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body).map_err(|_| {
            ChainCheckError::Unresolvable("the subgraph manifest is not UTF-8 text".to_string())
        })
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
    use std::time::Duration;

    use thegraph_core::deployment_id;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;

    const DEPLOYMENT: &str = "QmQ5w3LqJdBGZvHNYTWq7np2B1qbBQMpHP77ZKQPzGVTrg";

    const REGISTRY_JSON: &str = r#"{
        "$schema": "https://networks-registry.thegraph.com/TheGraphNetworksRegistrySchema_v0_7.json",
        "version": "0.7.0",
        "title": "Test Registry",
        "description": "Test Registry",
        "updatedAt": "2026-01-01T00:00:00Z",
        "networks": [
            {
                "id": "mainnet",
                "fullName": "Ethereum Mainnet",
                "shortName": "Ethereum",
                "caip2Id": "eip155:1",
                "networkType": "mainnet",
                "aliases": ["ethereum"],
                "issuanceRewards": true,
                "services": {}
            },
            {
                "id": "arbitrum-sepolia",
                "fullName": "Arbitrum Sepolia",
                "shortName": "Arbitrum Sepolia",
                "caip2Id": "eip155:421614",
                "networkType": "testnet",
                "issuanceRewards": false,
                "services": {}
            }
        ]
    }"#;

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
            Arc::new(NetworksRegistry::from_json(REGISTRY_JSON).unwrap()),
            Arc::new(BTreeMap::from([(1337, "hardhat".to_string())])),
        )
        .unwrap()
    }

    async fn serve_manifest(server: &MockServer, response: ResponseTemplate) {
        Mock::given(method("POST"))
            .and(path("/ipfs/api/v0/cat"))
            .and(query_param("arg", DEPLOYMENT))
            .respond_with(response)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn passes_a_request_on_the_manifest_chain() {
        let server = MockServer::start().await;
        serve_manifest(
            &server,
            ResponseTemplate::new(200).set_body_string(manifest("arbitrum-sepolia")),
        )
        .await;

        let result = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 421614)
            .await;

        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn refuses_a_request_on_another_chain() {
        let server = MockServer::start().await;
        serve_manifest(
            &server,
            ResponseTemplate::new(200).set_body_string(manifest("arbitrum-sepolia")),
        )
        .await;

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
    async fn refuses_a_manifest_without_a_single_network_for_good() {
        let server = MockServer::start().await;
        serve_manifest(
            &server,
            ResponseTemplate::new(200).set_body_string("dataSources:\n  - kind: ethereum\n"),
        )
        .await;

        let err = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 1)
            .await
            .unwrap_err();

        assert!(matches!(err, ChainCheckError::Unresolvable(_)), "{err:?}");
        assert!(!err.is_transient());
    }

    #[tokio::test]
    async fn an_ipfs_failure_is_worth_retrying() {
        let server = MockServer::start().await;
        serve_manifest(&server, ResponseTemplate::new(500)).await;

        let err = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 421614)
            .await
            .unwrap_err();

        assert!(err.is_transient(), "{err:?}");
    }

    #[tokio::test]
    async fn refuses_a_manifest_too_large_to_read() {
        let server = MockServer::start().await;
        serve_manifest(
            &server,
            ResponseTemplate::new(200).set_body_bytes(vec![b'a'; MAX_MANIFEST_BYTES + 1]),
        )
        .await;

        let err = check_against(&server)
            .check(&deployment_id!(DEPLOYMENT), 421614)
            .await
            .unwrap_err();

        assert!(matches!(err, ChainCheckError::Unresolvable(_)), "{err:?}");
    }

    #[tokio::test]
    async fn reads_each_manifest_from_ipfs_once() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/ipfs/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string(manifest("mainnet")))
            .expect(1)
            .mount(&server)
            .await;
        let check = check_against(&server);

        for _ in 0..3 {
            check.check(&deployment_id!(DEPLOYMENT), 1).await.unwrap();
        }
        // The mock's `expect(1)` is verified when the server drops.
    }
}
