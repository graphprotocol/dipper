//! Work out which chain a subgraph deployment indexes. The chain ID is signed into
//! the indexing request and decides which prices apply, so it is read from the
//! deployment's manifest rather than typed by hand.

use std::time::Duration;

use anyhow::{Context, anyhow};
use dipper_core::manifest::{
    chain_id_for_network, fetch_manifest, ipfs_cat_url, network_from_manifest,
};
use graph_networks_registry::NetworksRegistry;
use thegraph_core::{DeploymentId, alloy::primitives::ChainId};
use url::Url;

/// The IPFS API subgraph manifests are read from unless `--ipfs-url` says otherwise.
pub const DEFAULT_IPFS_URL: &str = "https://ipfs.thegraph.com";

const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long to wait on a lookup that only serves to check an override.
const CHECK_TIMEOUT: Duration = Duration::from_secs(5);

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
    check_timeout: Duration,
}

impl ChainResolver {
    pub fn new(ipfs_url: Url) -> anyhow::Result<Self> {
        Self::with_registry_url(ipfs_url, NetworksRegistry::get_latest_version_url())
    }

    fn with_registry_url(ipfs_url: Url, registry_url: String) -> anyhow::Result<Self> {
        let ipfs_api = ipfs_cat_url(ipfs_url)?;
        let http = reqwest::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("failed to build the HTTP client")?;
        Ok(Self {
            http,
            ipfs_api,
            registry_url,
            check_timeout: CHECK_TIMEOUT,
        })
    }

    /// The chain ID to sign into the indexing request for `deployment`, printing
    /// to stderr which chain was picked or why an override wasn't checked.
    pub async fn resolve(
        &self,
        deployment: &DeploymentId,
        chain: Option<ChainOverride>,
    ) -> anyhow::Result<ChainId> {
        // With an override, the manifest only feeds a warning, and so does the registry
        // for a numeric ID, so an IPFS or registry outage shouldn't hold up the request.
        let manifest_timeout = if chain.is_some() {
            self.check_timeout
        } else {
            HTTP_TIMEOUT
        };
        let registry_timeout = match chain {
            Some(ChainOverride::Id(_)) => self.check_timeout,
            _ => HTTP_TIMEOUT,
        };
        let (manifest, registry) = tokio::join!(
            self.fetch_manifest(deployment, manifest_timeout),
            self.fetch_registry(registry_timeout)
        );
        let manifest_network = manifest
            .and_then(|manifest| network_from_manifest(&manifest).map_err(anyhow::Error::from));
        let (chain_id, notice) = choose_chain_id(chain, manifest_network, registry)?;
        if let Some(notice) = notice {
            eprintln!("{notice}");
        }
        Ok(chain_id)
    }

    async fn fetch_manifest(
        &self,
        deployment: &DeploymentId,
        timeout: Duration,
    ) -> anyhow::Result<String> {
        fetch_manifest(&self.http, &self.ipfs_api, deployment, timeout)
            .await
            .with_context(|| {
                format!(
                    "failed to read the subgraph manifest from {}",
                    self.ipfs_api
                )
            })
    }

    async fn fetch_registry(&self, timeout: Duration) -> anyhow::Result<NetworksRegistry> {
        let response = self
            .http
            .get(&self.registry_url)
            .timeout(timeout)
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
/// be cancelled; the Dipper refuses it for anything else, so a disagreement is reported.
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
        // The CLI prints only the outermost message, so the cause goes into it.
        let chain_id = registry
            .and_then(|registry| {
                chain_id_for_network(&registry, &network).map_err(anyhow::Error::from)
            })
            .map_err(|err| {
                anyhow!("could not look up the chain ID of {network} ({err:#}); pass --chain-id")
            })?;
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
             but chain ID {chain_id} was given; the Dipper accepts that only to cancel a request"
        )),
        Err(err) => Some(format!(
            "warning: could not check chain ID {chain_id} against the subgraph manifest: {err:#}"
        )),
    };
    Ok((chain_id, notice))
}

#[cfg(test)]
mod tests {
    use dipper_core::manifest::TEST_REGISTRY_JSON;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;

    const DEPLOYMENT: &str = "QmQ5w3LqJdBGZvHNYTWq7np2B1qbBQMpHP77ZKQPzGVTrg";

    fn registry() -> anyhow::Result<NetworksRegistry> {
        Ok(NetworksRegistry::from_json(TEST_REGISTRY_JSON)?)
    }

    fn manifest(network: &str) -> String {
        format!("specVersion: 1.0.0\ndataSources:\n  - kind: ethereum\n    network: {network}\n")
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
    fn test_choose_chain_id_explains_a_registry_failure_without_an_override() {
        let err = choose_chain_id(
            None,
            Ok("arbitrum-sepolia".to_string()),
            Err(anyhow!("registry is down")),
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("registry is down") && err.contains("--chain-id"),
            "{err}"
        );
    }

    #[test]
    fn test_choose_chain_id_does_not_suggest_a_chain_id_that_was_given() {
        let (_, notice) = choose_chain_id(
            Some(ChainOverride::Id(1337)),
            Ok("hardhat".to_string()),
            registry(),
        )
        .unwrap();

        let notice = notice.unwrap();
        assert!(!notice.contains("pass --chain-id"), "{notice}");
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
    async fn test_resolve_with_a_chain_id_does_not_wait_long_on_ipfs() {
        //* Arrange - IPFS and the registry both take 30s to answer
        let server = MockServer::start().await;
        let slow = |body: String| {
            ResponseTemplate::new(200)
                .set_body_string(body)
                .set_delay(Duration::from_secs(30))
        };
        Mock::given(method("POST"))
            .respond_with(slow(manifest("mainnet")))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .respond_with(slow(TEST_REGISTRY_JSON.to_string()))
            .mount(&server)
            .await;
        let mut resolver = ChainResolver::with_registry_url(
            Url::parse(&server.uri()).unwrap(),
            format!("{}/registry.json", server.uri()),
        )
        .unwrap();
        resolver.check_timeout = Duration::from_millis(200);

        //* Act
        let resolved = tokio::time::timeout(
            Duration::from_secs(5),
            resolver.resolve(&DEPLOYMENT.parse().unwrap(), Some(ChainOverride::Id(1337))),
        )
        .await;

        //* Assert
        let chain_id = resolved.expect("the check should give up long before 5s");
        assert_eq!(chain_id.unwrap(), 1337);
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
            .respond_with(ResponseTemplate::new(200).set_body_string(TEST_REGISTRY_JSON))
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
