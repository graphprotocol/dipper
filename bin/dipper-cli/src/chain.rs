//! Work out which chain a subgraph deployment indexes. The chain ID is signed into
//! the indexing request and decides which prices apply, so it is read from the
//! deployment's manifest rather than typed by hand.

use std::{collections::BTreeSet, time::Duration};

use anyhow::{Context, anyhow, bail};
use graph_networks_registry::NetworksRegistry;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

/// The IPFS API subgraph manifests are read from unless `--ipfs-url` says otherwise.
pub const DEFAULT_IPFS_URL: &str = "https://ipfs.thegraph.com";

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// A chain chosen on the command line instead of the one in the manifest.
#[derive(Debug, Clone)]
pub enum ChainOverride {
    /// A network name from the networks registry, e.g. `arbitrum-sepolia`.
    Name(String),
    /// A numeric chain ID, for chains the registry doesn't list (e.g. a local hardhat chain).
    Id(ChainId),
}

/// Reads subgraph manifests from IPFS and network names from the networks registry.
pub struct ChainResolver {
    http: reqwest::Client,
    ipfs_api: Url,
    registry_url: String,
}

impl ChainResolver {
    pub fn new(ipfs_url: Url) -> anyhow::Result<Self> {
        Self::with_registry_url(ipfs_url, NetworksRegistry::get_latest_version_url())
    }

    fn with_registry_url(mut ipfs_url: Url, registry_url: String) -> anyhow::Result<Self> {
        // Without the trailing slash, `join` would replace the last path segment.
        if !ipfs_url.path().ends_with('/') {
            ipfs_url.set_path(&format!("{}/", ipfs_url.path()));
        }
        let ipfs_api = ipfs_url.join("api/v0/cat")?;
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("failed to build the HTTP client")?;
        Ok(Self {
            http,
            ipfs_api,
            registry_url,
        })
    }

    /// The chain ID to sign into the indexing request for `deployment`, printing
    /// to stderr which chain was picked or why an override wasn't checked.
    pub async fn resolve(
        &self,
        deployment: &DeploymentId,
        chain: Option<ChainOverride>,
    ) -> anyhow::Result<ChainId> {
        let (manifest, registry) =
            tokio::join!(self.fetch_manifest(deployment), self.fetch_registry());
        let manifest_network = manifest.and_then(|manifest| network_from_manifest(&manifest));
        let (chain_id, notice) = choose_chain_id(chain, manifest_network, registry)?;
        if let Some(notice) = notice {
            eprintln!("{notice}");
        }
        Ok(chain_id)
    }

    async fn fetch_manifest(&self, deployment: &DeploymentId) -> anyhow::Result<String> {
        let mut url = self.ipfs_api.clone();
        url.query_pairs_mut()
            .append_pair("arg", &deployment.to_string());
        // The IPFS RPC API only answers POST.
        let response = self
            .http
            .post(url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| {
                format!(
                    "failed to read the subgraph manifest from {}",
                    self.ipfs_api
                )
            })?;
        response
            .text()
            .await
            .context("failed to read the subgraph manifest body")
    }

    async fn fetch_registry(&self) -> anyhow::Result<NetworksRegistry> {
        let response = self
            .http
            .get(&self.registry_url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| {
                format!(
                    "failed to fetch the networks registry from {}",
                    self.registry_url
                )
            })?;
        let json = response
            .text()
            .await
            .context("failed to read the networks registry body")?;
        NetworksRegistry::from_json(&json).context("failed to parse the networks registry")
    }
}

/// Pick the chain ID from an override or the manifest, with an optional message for
/// the user. An override always wins, so a request made for the wrong chain can still
/// be changed or cancelled, but a disagreement with the manifest is reported.
fn choose_chain_id(
    chain: Option<ChainOverride>,
    manifest_network: anyhow::Result<String>,
    registry: anyhow::Result<NetworksRegistry>,
) -> anyhow::Result<(ChainId, Option<String>)> {
    let Some(chain) = chain else {
        let network = manifest_network.map_err(|err| {
            anyhow!(
                "could not work out the chain from the subgraph manifest ({err:#}); \
                 pass --chain-name or --chain-id"
            )
        })?;
        let chain_id = chain_id_for_network(&registry?, &network)?;
        let notice =
            format!("Using chain {network} (chain ID {chain_id}) from the subgraph manifest");
        return Ok((chain_id, Some(notice)));
    };

    let chain_id = match chain {
        ChainOverride::Name(name) => {
            let registry = registry.as_ref().map_err(|err| anyhow!("{err:#}"))?;
            chain_id_for_network(registry, &name)?
        }
        ChainOverride::Id(id) => id,
    };
    let manifest_chain = manifest_network.and_then(|network| {
        let registry = registry.as_ref().map_err(|err| anyhow!("{err:#}"))?;
        Ok((network.clone(), chain_id_for_network(registry, &network)?))
    });
    let notice = match manifest_chain {
        Ok((_, manifest_chain_id)) if manifest_chain_id == chain_id => None,
        Ok((network, manifest_chain_id)) => Some(format!(
            "warning: the subgraph manifest indexes {network} (chain ID {manifest_chain_id}), \
             but chain ID {chain_id} was given; using {chain_id}"
        )),
        Err(err) => Some(format!(
            "warning: could not check chain ID {chain_id} against the subgraph manifest: {err:#}"
        )),
    };
    Ok((chain_id, notice))
}

/// The one network a subgraph manifest indexes, across its data sources and templates.
fn network_from_manifest(manifest: &str) -> anyhow::Result<String> {
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
        serde_yaml::from_str(manifest).context("the subgraph manifest is not valid YAML")?;
    let networks: BTreeSet<String> = manifest
        .data_sources
        .into_iter()
        .chain(manifest.templates)
        .filter_map(|source| source.network)
        .collect();
    let mut iter = networks.iter();
    match (iter.next(), iter.next()) {
        (Some(network), None) => Ok(network.clone()),
        (None, _) => bail!("the subgraph manifest names no network"),
        (Some(_), Some(_)) => bail!(
            "the subgraph manifest names more than 1 network: {}",
            networks.into_iter().collect::<Vec<_>>().join(", ")
        ),
    }
}

/// The EVM chain ID for a network name or alias in the networks registry.
fn chain_id_for_network(registry: &NetworksRegistry, network: &str) -> anyhow::Result<ChainId> {
    let entry = registry.get_network_by_graph_id(network).ok_or_else(|| {
        anyhow!("network '{network}' is not in the networks registry; pass --chain-id")
    })?;
    entry
        .caip2_id
        .strip_prefix("eip155:")
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| {
            anyhow!(
                "network '{network}' has no EVM chain ID (its CAIP-2 ID is '{}')",
                entry.caip2_id
            )
        })
}

#[cfg(test)]
mod tests {
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

    fn registry() -> anyhow::Result<NetworksRegistry> {
        Ok(NetworksRegistry::from_json(REGISTRY_JSON)?)
    }

    fn manifest(network: &str) -> String {
        format!("specVersion: 1.0.0\ndataSources:\n  - kind: ethereum\n    network: {network}\n")
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
        assert!(network_from_manifest(none).is_err());
        assert!(network_from_manifest("not: [valid").is_err());
    }

    #[test]
    fn test_chain_id_for_network_accepts_ids_and_aliases() {
        let registry = registry().unwrap();

        assert_eq!(chain_id_for_network(&registry, "mainnet").unwrap(), 1);
        assert_eq!(chain_id_for_network(&registry, "ethereum").unwrap(), 1);
        assert_eq!(
            chain_id_for_network(&registry, "arbitrum-sepolia").unwrap(),
            421614
        );
    }

    #[test]
    fn test_chain_id_for_network_rejects_unknown_and_non_evm_networks() {
        let registry = registry().unwrap();

        assert!(chain_id_for_network(&registry, "hardhat").is_err());
        assert!(chain_id_for_network(&registry, "solana-mainnet-beta").is_err());
    }

    #[test]
    fn test_choose_chain_id_uses_the_manifest_without_an_override() {
        let (chain_id, notice) =
            choose_chain_id(None, Ok("arbitrum-sepolia".to_string()), registry()).unwrap();

        assert_eq!(chain_id, 421614);
        assert!(notice.unwrap().contains("arbitrum-sepolia"));
    }

    #[test]
    fn test_choose_chain_id_fails_without_an_override_or_a_manifest() {
        let err = choose_chain_id(None, Err(anyhow!("IPFS is down")), registry())
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("IPFS is down") && err.contains("--chain-id"),
            "{err}"
        );
    }

    #[test]
    fn test_choose_chain_id_keeps_an_override_that_disagrees_with_the_manifest() {
        //* Act
        let (chain_id, notice) = choose_chain_id(
            Some(ChainOverride::Id(1)),
            Ok("arbitrum-sepolia".to_string()),
            registry(),
        )
        .unwrap();

        //* Assert - so a request made for the wrong chain can still be cancelled
        assert_eq!(chain_id, 1);
        assert!(notice.unwrap().contains("indexes arbitrum-sepolia"));
    }

    #[test]
    fn test_choose_chain_id_is_quiet_when_the_override_matches_the_manifest() {
        let (chain_id, notice) = choose_chain_id(
            Some(ChainOverride::Name("arbitrum-sepolia".to_string())),
            Ok("arbitrum-sepolia".to_string()),
            registry(),
        )
        .unwrap();

        assert_eq!(chain_id, 421614);
        assert_eq!(notice, None);
    }

    #[test]
    fn test_choose_chain_id_accepts_a_local_chain_id_the_registry_lacks() {
        let (chain_id, notice) = choose_chain_id(
            Some(ChainOverride::Id(1337)),
            Ok("hardhat".to_string()),
            Err(anyhow!("offline")),
        )
        .unwrap();

        assert_eq!(chain_id, 1337);
        assert!(notice.unwrap().contains("could not check"));
    }

    #[test]
    fn test_choose_chain_id_rejects_a_chain_name_it_cannot_look_up() {
        assert!(
            choose_chain_id(
                Some(ChainOverride::Name("arbitrum-sepolia".to_string())),
                Ok("arbitrum-sepolia".to_string()),
                Err(anyhow!("offline")),
            )
            .is_err()
        );
        assert!(
            choose_chain_id(
                Some(ChainOverride::Name("not-a-network".to_string())),
                Ok("mainnet".to_string()),
                registry(),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_resolve_reads_the_manifest_and_registry_over_http() {
        //* Arrange
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/ipfs/api/v0/cat"))
            .and(query_param("arg", DEPLOYMENT))
            .respond_with(ResponseTemplate::new(200).set_body_string(manifest("arbitrum-sepolia")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/registry.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(REGISTRY_JSON))
            .mount(&server)
            .await;
        let resolver = ChainResolver::with_registry_url(
            Url::parse(&format!("{}/ipfs", server.uri())).unwrap(),
            format!("{}/registry.json", server.uri()),
        )
        .unwrap();

        //* Act
        let chain_id = resolver
            .resolve(&DEPLOYMENT.parse().unwrap(), None)
            .await
            .unwrap();

        //* Assert
        assert_eq!(chain_id, 421614);
    }
}
