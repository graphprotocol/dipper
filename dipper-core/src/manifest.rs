//! Work out which chain a subgraph deployment indexes from its manifest. The CLI uses
//! this to pick the chain for a request, and the service to check the chain a request
//! arrives with, so both read a manifest the same way.

use std::{collections::BTreeSet, time::Duration};

use graph_networks_registry::NetworksRegistry;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

/// Larger manifests are refused rather than read into memory: real ones are a few KiB.
pub const MAX_MANIFEST_BYTES: usize = 4 * 1024 * 1024;

/// Why a manifest could not be read from IPFS. Reqwest errors are kept without their URL,
/// since an IPFS endpoint can carry credentials in it.
#[derive(Debug, thiserror::Error)]
pub enum ManifestFetchError {
    /// IPFS answered, but with no manifest: unknown or malformed CIDs get a 4xx.
    #[error("IPFS has no subgraph manifest for this deployment (HTTP {0})")]
    NotFound(u16),
    /// IPFS could not be reached, failed, or did not answer in time.
    #[error("IPFS did not return the subgraph manifest")]
    Unreachable(#[source] reqwest::Error),
    #[error("the subgraph manifest is larger than {MAX_MANIFEST_BYTES} bytes")]
    TooLarge,
    #[error("the subgraph manifest is not UTF-8 text")]
    NotUtf8,
}

impl ManifestFetchError {
    /// Whether the same read could succeed later. A missing manifest counts as final:
    /// IPFS only says so after searching for it.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unreachable(_))
    }
}

/// The `cat` endpoint of the IPFS API at `ipfs_url`, which manifests are read from.
pub fn ipfs_cat_url(mut ipfs_url: Url) -> Result<Url, url::ParseError> {
    // Without the trailing slash, `join` would replace the last path segment.
    if !ipfs_url.path().ends_with('/') {
        ipfs_url.set_path(&format!("{}/", ipfs_url.path()));
    }
    ipfs_url.join("api/v0/cat")
}

/// Read `deployment`'s manifest through the IPFS `cat` endpoint `cat_url`.
pub async fn fetch_manifest(
    http: &reqwest::Client,
    cat_url: &Url,
    deployment: &DeploymentId,
    timeout: Duration,
) -> Result<String, ManifestFetchError> {
    let unreachable = |err: reqwest::Error| ManifestFetchError::Unreachable(err.without_url());
    let mut url = cat_url.clone();
    url.query_pairs_mut()
        .append_pair("arg", &deployment.to_string());
    // The IPFS RPC API only answers POST.
    let mut response = http
        .post(url)
        .timeout(timeout)
        .send()
        .await
        .map_err(unreachable)?;
    let status = response.status();
    if status.is_client_error() {
        return Err(ManifestFetchError::NotFound(status.as_u16()));
    }
    response = response.error_for_status().map_err(unreachable)?;
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(unreachable)? {
        if body.len() + chunk.len() > MAX_MANIFEST_BYTES {
            return Err(ManifestFetchError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|_| ManifestFetchError::NotUtf8)
}

/// Why a manifest does not name exactly 1 network.
#[derive(Debug, thiserror::Error)]
pub enum ManifestNetworkError {
    #[error("the subgraph manifest is not valid YAML")]
    InvalidYaml(#[source] serde_yaml::Error),
    #[error("the subgraph manifest names no network")]
    NoNetwork,
    #[error("the subgraph manifest names more than 1 network: {}", .0.join(", "))]
    SeveralNetworks(Vec<String>),
}

/// Why a network name has no EVM chain ID in the networks registry.
#[derive(Debug, thiserror::Error)]
pub enum NetworkLookupError {
    #[error("network '{0}' is not in the networks registry")]
    Unknown(String),
    #[error("network '{network}' has no EVM chain ID (its CAIP-2 ID is '{caip2_id}')")]
    NotEvm { network: String, caip2_id: String },
}

/// The 1 network a subgraph manifest indexes, across its data sources and templates.
pub fn network_from_manifest(manifest: &str) -> Result<String, ManifestNetworkError> {
    #[derive(serde::Deserialize)]
    struct Manifest {
        #[serde(default, rename = "dataSources")]
        data_sources: Vec<Source>,
        #[serde(default)]
        templates: Vec<Source>,
    }

    #[derive(serde::Deserialize)]
    struct Source {
        network: Option<String>,
    }

    let manifest: Manifest =
        serde_yaml::from_str(manifest).map_err(ManifestNetworkError::InvalidYaml)?;
    let networks: BTreeSet<String> = manifest
        .data_sources
        .into_iter()
        .chain(manifest.templates)
        .filter_map(|source| source.network)
        .collect();
    let mut iter = networks.iter();
    match (iter.next(), iter.next()) {
        (Some(network), None) => Ok(network.clone()),
        (None, _) => Err(ManifestNetworkError::NoNetwork),
        (Some(_), Some(_)) => Err(ManifestNetworkError::SeveralNetworks(
            networks.into_iter().collect(),
        )),
    }
}

/// The EVM chain ID for a network name or alias in the networks registry.
pub fn chain_id_for_network(
    registry: &NetworksRegistry,
    network: &str,
) -> Result<ChainId, NetworkLookupError> {
    let entry = registry
        .get_network_by_graph_id(network)
        .ok_or_else(|| NetworkLookupError::Unknown(network.to_string()))?;
    entry
        .caip2_id
        .strip_prefix("eip155:")
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| NetworkLookupError::NotEvm {
            network: network.to_string(),
            caip2_id: entry.caip2_id.clone(),
        })
}

/// A networks registry with an EVM mainnet (alias `ethereum`), an EVM testnet and a
/// non-EVM network, for tests that look chains up.
#[cfg(any(test, feature = "test-util"))]
pub const TEST_REGISTRY_JSON: &str = r#"{
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
            },
            {
                "id": "solana-mainnet-beta",
                "fullName": "Solana Mainnet Beta",
                "shortName": "Solana",
                "caip2Id": "solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp",
                "networkType": "mainnet",
                "issuanceRewards": false,
                "services": {}
            }
        ]
    }"#;

#[cfg(test)]
mod tests {
    use thegraph_core::deployment_id;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;

    const DEPLOYMENT: &str = "QmQ5w3LqJdBGZvHNYTWq7np2B1qbBQMpHP77ZKQPzGVTrg";

    fn registry() -> NetworksRegistry {
        NetworksRegistry::from_json(TEST_REGISTRY_JSON).unwrap()
    }

    async fn fetch_from(response: ResponseTemplate) -> Result<String, ManifestFetchError> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/ipfs/api/v0/cat"))
            .and(query_param("arg", DEPLOYMENT))
            .respond_with(response)
            .mount(&server)
            .await;
        let cat_url = ipfs_cat_url(Url::parse(&format!("{}/ipfs", server.uri())).unwrap()).unwrap();
        fetch_manifest(
            &reqwest::Client::new(),
            &cat_url,
            &deployment_id!(DEPLOYMENT),
            Duration::from_secs(5),
        )
        .await
    }

    #[test]
    fn test_ipfs_cat_url_keeps_a_path_prefix() {
        let with_prefix = ipfs_cat_url(Url::parse("https://example.com/ipfs").unwrap()).unwrap();
        let bare = ipfs_cat_url(Url::parse("https://example.com").unwrap()).unwrap();

        assert_eq!(with_prefix.as_str(), "https://example.com/ipfs/api/v0/cat");
        assert_eq!(bare.as_str(), "https://example.com/api/v0/cat");
    }

    #[tokio::test]
    async fn test_fetch_manifest_reads_the_manifest() {
        let manifest = fetch_from(ResponseTemplate::new(200).set_body_string("specVersion: 1.0.0"))
            .await
            .unwrap();

        assert_eq!(manifest, "specVersion: 1.0.0");
    }

    #[tokio::test]
    async fn test_fetch_manifest_tells_a_missing_manifest_from_a_failing_ipfs() {
        let missing = fetch_from(ResponseTemplate::new(404)).await.unwrap_err();
        let failing = fetch_from(ResponseTemplate::new(502)).await.unwrap_err();

        assert!(
            matches!(missing, ManifestFetchError::NotFound(404)),
            "{missing:?}"
        );
        assert!(!missing.is_transient());
        assert!(failing.is_transient(), "{failing:?}");
    }

    #[tokio::test]
    async fn test_fetch_manifest_refuses_a_manifest_too_large_to_read() {
        let err = fetch_from(
            ResponseTemplate::new(200).set_body_bytes(vec![b'a'; MAX_MANIFEST_BYTES + 1]),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, ManifestFetchError::TooLarge), "{err:?}");
    }

    #[tokio::test]
    async fn test_fetch_manifest_errors_leave_the_url_out() {
        //* Arrange - nothing listens on port 9, so the connection fails
        let cat_url = ipfs_cat_url(Url::parse("http://user:secret@127.0.0.1:9").unwrap()).unwrap();

        //* Act
        let err = fetch_manifest(
            &reqwest::Client::new(),
            &cat_url,
            &deployment_id!(DEPLOYMENT),
            Duration::from_secs(5),
        )
        .await
        .unwrap_err();

        //* Assert
        let mut text = err.to_string();
        let mut source = std::error::Error::source(&err);
        while let Some(cause) = source {
            text.push_str(&cause.to_string());
            source = cause.source();
        }
        assert!(!text.contains("secret"), "{text}");
    }

    #[test]
    fn test_network_from_manifest_reads_data_sources_and_templates() {
        let manifest = indoc::indoc! {"
            dataSources:
              - kind: ethereum
                network: arbitrum-sepolia
            templates:
              - kind: ethereum
                network: arbitrum-sepolia
        "};

        assert_eq!(network_from_manifest(manifest).unwrap(), "arbitrum-sepolia");
    }

    #[test]
    fn test_network_from_manifest_rejects_mixed_or_missing_networks() {
        let mixed = indoc::indoc! {"
            dataSources:
              - network: mainnet
            templates:
              - network: arbitrum-sepolia
        "};
        let err = network_from_manifest(mixed).unwrap_err().to_string();
        assert!(err.contains("arbitrum-sepolia, mainnet"), "{err}");

        let none = "dataSources:\n  - kind: ethereum\n";
        assert!(matches!(
            network_from_manifest(none),
            Err(ManifestNetworkError::NoNetwork)
        ));
        assert!(matches!(
            network_from_manifest("not: [valid"),
            Err(ManifestNetworkError::InvalidYaml(_))
        ));
    }

    #[test]
    fn test_chain_id_for_network_accepts_ids_and_aliases() {
        let registry = registry();

        assert_eq!(chain_id_for_network(&registry, "mainnet").unwrap(), 1);
        assert_eq!(chain_id_for_network(&registry, "ethereum").unwrap(), 1);
        assert_eq!(
            chain_id_for_network(&registry, "arbitrum-sepolia").unwrap(),
            421614
        );
    }

    #[test]
    fn test_chain_id_for_network_rejects_unknown_and_non_evm_networks() {
        let registry = registry();

        assert!(matches!(
            chain_id_for_network(&registry, "hardhat"),
            Err(NetworkLookupError::Unknown(_))
        ));
        assert!(matches!(
            chain_id_for_network(&registry, "solana-mainnet-beta"),
            Err(NetworkLookupError::NotEvm { .. })
        ));
    }
}
