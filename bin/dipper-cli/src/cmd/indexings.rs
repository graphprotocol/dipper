use std::str::FromStr;

use clap::{Command, arg, command, value_parser};
use dipper_core::ids::IndexingRequestId;
use dipper_rpc::admin::indexing_requests::SetIndexingTargetCandidates;
use thegraph_core::{DeploymentId, SubgraphId, alloy::primitives::ChainId, signed_message};
use url::Url;
use uuid::Uuid;

use super::{common, result::Result};
use crate::{
    chain::{ChainOverride, ChainResolver},
    client,
    client::IndexingRequestsRpcClient,
    config::Config,
    signer,
};

/// The `indexings` command implementation
pub(super) async fn run(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("list", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            list(conf).await
        }
        Some(("status", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            status(conf, matches).await
        }
        Some(("set-target-candidates", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            set_target(conf, matches).await
        }
        _ => Err(anyhow::anyhow!("No indexings command specified").into()),
    }
}

/// The `indexings list` command
///
/// This function lists all registered indexing requests.
///
/// This function calls the `get_all_indexing_requests` RPC method on the DIPs gateway server.
// TODO(post-mvp): Add support for pagination
pub async fn list(conf: Config) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let res = rpc_client
        .get_all_indexing_requests()
        .await
        .map_err(|err| anyhow::anyhow!("Failed to list indexing requests: {err}"))?;

    // Print the result as pretty JSON so one can use `jq` to explore the output
    println!(
        "{}",
        serde_json::to_string_pretty(&res)
            .map_err(|err| anyhow::anyhow!("Failed to serialize indexing requests: {err}"))?
    );

    Ok(())
}

/// The `indexings status` command
pub async fn status(conf: Config, matches: &clap::ArgMatches) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);

    match matches.get_one::<IndexingRequestSelector>("INDEXING_ID") {
        // ID is an UUIDv7
        Some(IndexingRequestSelector::IndexingRequestId(id)) => {
            let res = rpc_client
                .get_indexing_request_by_id(*id)
                .await
                .map_err(|err| anyhow::anyhow!("Failed to get indexing request by ID: {err}"))?;

            // Print the result as pretty JSON so one can use `jq` to explore the output
            println!(
                "{}",
                serde_json::to_string_pretty(&res).map_err(|err| anyhow::anyhow!(
                    "Failed to serialize indexing request: {err}"
                ))?
            );

            Ok(())
        }
        // ID is a Deployment ID
        Some(IndexingRequestSelector::DeploymentId(id)) => {
            let res = rpc_client
                .get_indexing_requests_by_deployment_id(*id)
                .await
                .map_err(|err| {
                    anyhow::anyhow!("Failed to get indexing requests by deployment ID: {err}")
                })?;

            // Print the result as pretty JSON so one can use `jq` to explore the output
            println!(
                "{}",
                serde_json::to_string_pretty(&res).map_err(|err| anyhow::anyhow!(
                    "Failed to serialize indexing requests: {err}"
                ))?
            );

            Ok(())
        }
        // ID is a Subgraph ID
        Some(IndexingRequestSelector::SubgraphId(id)) => {
            // TODO(post-mvp): Add support for querying by Subgraph ID
            Err(anyhow::anyhow!("Invalid indexing request ID: `{id}`").into())
        }
        None => unreachable!("No ID provided"),
    }
}

/// The `indexings set-target-candidates` command: an idempotent upsert keyed on
/// `(requester, deployment, chain)`, where `--num-candidates 0` cancels. The chain
/// comes from the deployment's manifest unless `--chain-name` or `--chain-id` is given.
pub async fn set_target(conf: Config, matches: &clap::ArgMatches) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let signer = signer::new_private_key_eip712_signer(&conf.signing_key);
    let signer_eip712_domain = signer::eip712_domain();

    let request_deployment_id = match matches.get_one::<SubgraphIdOrDeploymentId>("SUBGRAPH") {
        // ID is a Deployment ID
        Some(SubgraphIdOrDeploymentId::DeploymentId(id)) => id,

        // ID is a Subgraph ID
        // TODO(post-mvp): Add support for querying by Subgraph ID
        Some(SubgraphIdOrDeploymentId::SubgraphId(id)) => {
            return Err(anyhow::anyhow!("Invalid subgraph ID: `{id}`").into());
        }
        None => unreachable!("No ID provided"),
    };

    let chain_override = match (
        matches.get_one::<String>("chain-name"),
        matches.get_one::<ChainId>("chain-id"),
    ) {
        (Some(name), _) => Some(ChainOverride::Name(name.clone())),
        (None, Some(id)) => Some(ChainOverride::Id(*id)),
        (None, None) => None,
    };
    let request_chain_id = ChainResolver::new(conf.ipfs_url.clone())?
        .resolve(request_deployment_id, chain_override)
        .await?;

    let num_candidates = matches.get_one::<usize>("num-candidates").copied();

    let req = signed_message::sign(
        &signer,
        &signer_eip712_domain,
        SetIndexingTargetCandidates {
            deployment_id: *request_deployment_id,
            chain_id: request_chain_id,
            num_candidates,
        },
    )
    .map_err(|err| anyhow::anyhow!("Failed to sign RPC request: {err}"))?;

    let res = rpc_client
        .set_indexing_target_candidates(req.into())
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "Failed to set indexing target for deployment '{request_deployment_id}': {err}"
            )
        })?;

    match res {
        Some(id) => println!("{}", id),
        None => println!(
            "no-op: no open indexing request exists for deployment '{request_deployment_id}' on chain {request_chain_id}"
        ),
    }

    Ok(())
}

/// Create the `indexings` DIPs indexing requests admin command
pub(super) fn cmd() -> Command {
    command!("indexings")
        .about("Manage indexings")
        .args(
            // Common arg options to be used by all subcommands
            &[
                common::env_file_arg().global(true),
                common::server_url_arg().global(true),
                common::signing_key_arg().global(true),
            ],
        )
        .subcommands(&[
            command!("list")
                .alias("ls")
                .about("List all indexing requests"),
            command!("status")
                .about("Get an indexing request status")
                .arg(
                    arg!(<INDEXING_ID> "The indexing request's ID (UUID, Subgraph ID or Deployment ID)")
                        .value_parser(value_parser!(IndexingRequestSelector)),
                ),
            command!("set-target-candidates")
                .about("Set the target number of indexer candidates for a deployment; use --num-candidates 0 to cancel")
                .args([
                    arg!(<SUBGRAPH> "The indexing request's Subgraph (or Deployment) ID")
                        .value_parser(value_parser!(SubgraphIdOrDeploymentId)),
                    arg!(--"chain-name" <NETWORK> "Use this network instead of the one in the subgraph manifest (e.g. arbitrum-sepolia)")
                        .required(false)
                        .conflicts_with("chain-id"),
                    arg!(--"chain-id" <ID> "Use this numeric chain ID instead of the manifest's network (e.g. 1337 for a local chain)")
                        .value_parser(value_parser!(ChainId))
                        .required(false),
                    arg!(--"ipfs-url" <URL> "The IPFS API to read the subgraph manifest from (env DIPS_IPFS_URL, default https://ipfs.thegraph.com)")
                        .value_parser(value_parser!(Url))
                        .required(false),
                    arg!(--"num-candidates" <N> "Target number of indexers to assign (0 cancels). Defaults to server maximum.")
                        .value_parser(value_parser!(usize))
                        .required(false),
                ]),
        ])
}

/// A subgraph ID or deployment ID.
///
/// This type is used to parse a subgraph ID or deployment ID from a string.
#[derive(Debug, Clone)]
enum SubgraphIdOrDeploymentId {
    /// A subgraph ID
    SubgraphId(SubgraphId),
    /// A deployment ID
    DeploymentId(DeploymentId),
}

impl FromStr for SubgraphIdOrDeploymentId {
    type Err = anyhow::Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        // First, try to parse the value as a Deployment ID
        if let Ok(id) = val.parse() {
            return Ok(SubgraphIdOrDeploymentId::DeploymentId(id));
        }

        // Otherwise, try to parse the value as a Subgraph ID
        if let Ok(id) = val.parse() {
            return Ok(SubgraphIdOrDeploymentId::SubgraphId(id));
        }

        Err(anyhow::anyhow!("Invalid subgraph ID: {val}"))
    }
}

/// An _indexing request_ selector.
///
/// This type is used to parse an indexing request ID (UUID), subgraph ID or deployment ID from a
/// string.
#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
enum IndexingRequestSelector {
    /// An indexing request ID (UUIDv7)
    IndexingRequestId(IndexingRequestId),
    /// A subgraph ID
    SubgraphId(SubgraphId),
    /// A deployment ID
    DeploymentId(DeploymentId),
}

impl FromStr for IndexingRequestSelector {
    type Err = anyhow::Error;

    fn from_str(val: &str) -> Result<Self, Self::Err> {
        // First, try to parse the value as an Indexing Request ID (UUIDv7)
        if let Ok(id) = val.parse::<Uuid>().map(Into::into) {
            return Ok(IndexingRequestSelector::IndexingRequestId(id));
        }

        // Next, try to parse the value as a Deployment ID
        if let Ok(id) = val.parse() {
            return Ok(IndexingRequestSelector::DeploymentId(id));
        }

        // Finally, try to parse the value as a Subgraph ID
        if let Ok(id) = val.parse() {
            return Ok(IndexingRequestSelector::SubgraphId(id));
        }

        Err(anyhow::anyhow!("Invalid indexing request selector: {val}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse `indexings set-target-candidates <deployment>` followed by `args`.
    fn parse_set_target(args: &[&str]) -> std::result::Result<clap::ArgMatches, clap::Error> {
        let base = [
            "indexings",
            "--server-url",
            "http://localhost:9000",
            "--signing-key",
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            "set-target-candidates",
            "QmQ5w3LqJdBGZvHNYTWq7np2B1qbBQMpHP77ZKQPzGVTrg",
        ];
        cmd().try_get_matches_from(base.into_iter().chain(args.iter().copied()))
    }

    #[test]
    fn test_set_target_candidates_needs_no_chain() {
        let matches = parse_set_target(&[]).unwrap();
        let (_, matches) = matches.subcommand().unwrap();

        assert_eq!(matches.get_one::<String>("chain-name"), None);
        assert_eq!(matches.get_one::<ChainId>("chain-id"), None);
    }

    #[test]
    fn test_set_target_candidates_reads_the_ipfs_url_from_the_env_file() {
        //* Arrange
        let env_file = std::env::temp_dir().join(format!("dipper-cli-{}.env", std::process::id()));
        std::fs::write(&env_file, "DIPS_IPFS_URL=http://ipfs.env-file.test:5001\n").unwrap();
        let matches = parse_set_target(&["--env-file", env_file.to_str().unwrap()]).unwrap();
        let (_, matches) = matches.subcommand().unwrap();

        //* Act
        let conf = common::load_conf(matches);
        std::fs::remove_file(&env_file).unwrap();

        //* Assert
        assert_eq!(
            conf.unwrap().ipfs_url.as_str(),
            "http://ipfs.env-file.test:5001/"
        );
    }

    #[test]
    fn test_set_target_candidates_rejects_both_chain_flags() {
        let err = parse_set_target(&["--chain-name", "mainnet", "--chain-id", "1"]).unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn test_set_target_candidates_rejects_the_old_positional_chain_id() {
        assert!(parse_set_target(&["1"]).is_err());
    }
}
