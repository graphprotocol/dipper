//! Work out which chain a subgraph deployment indexes from its manifest. The CLI uses
//! this to pick the chain for a request, and the service to check the chain a request
//! arrives with, so both read a manifest the same way.

use std::collections::BTreeSet;

use graph_networks_registry::NetworksRegistry;
use thegraph_core::alloy::primitives::ChainId;

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

#[cfg(test)]
mod tests {
    use super::*;

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

    fn registry() -> NetworksRegistry {
        NetworksRegistry::from_json(REGISTRY_JSON).unwrap()
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
