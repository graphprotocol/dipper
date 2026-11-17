use std::str::FromStr;

use clap::{Command, arg, command, value_parser};
use dipper_core::ids::IndexingRequestId;
use dipper_rpc::admin::indexing_requests::{
    DEFAULT_PAGE_SIZE, IndexingRequestStatus, SetIndexingTargetCandidates,
};
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
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
pub(super) async fn run(matches: &clap::ArgMatches) -> Result<()> {
    match matches.subcommand() {
        Some(("list", matches)) => {
            let conf = common::load_conf(matches)?;
            tracing::debug!("Configuration loaded: {:?}", conf);

            list(conf, matches).await
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
/// Prints the registered indexing requests as 1 JSON array, in the order they were created, so
/// one can use `jq` to explore the output. The server hands them over a page at a time through
/// its `get_all_indexing_requests` RPC method, and each page is printed as it arrives, so a long
/// list never has to fit in 1 reply or in memory.
pub async fn list(conf: Config, matches: &clap::ArgMatches) -> Result<()> {
    let rpc_client = client::new(&conf.server_url);
    let status = matches.get_one::<IndexingRequestStatus>("status").copied();
    let mut paging = Paging::new(matches.get_one::<u64>("limit").copied());
    let mut array = JsonArray::default();

    while let Some(after) = paging.start() {
        let page = rpc_client
            .get_all_indexing_requests(after, paging.ask(), status)
            .await
            .map_err(|err| anyhow::anyhow!("Failed to list indexing requests: {err}"))?;
        let requests = paging.take(page.requests, page.next)?;
        print!("{}", array.items(&requests)?);
    }
    println!("{}", array.end());

    Ok(())
}

/// Walks a listing that the server hands over a page at a time: where each page starts, how
/// many requests to ask for, and when to stop.
struct Paging {
    /// The id the next page starts after. `Some(None)` is the first page, `None` no more pages.
    start: Option<Option<IndexingRequestId>>,
    /// How many more requests the caller wants, when they set a limit.
    left: Option<u64>,
}

impl Paging {
    /// A listing of every request, or of the first `most` of them.
    fn new(most: Option<u64>) -> Self {
        Self {
            start: Some(None),
            left: most,
        }
    }

    /// The id the next page starts after, or `None` once the listing is complete.
    fn start(&self) -> Option<Option<IndexingRequestId>> {
        self.start
    }

    /// How many requests to ask the server for. With no limit the server's own page size
    /// applies, and with one no page asks for more than is still wanted.
    fn ask(&self) -> Option<u32> {
        let left = self.left?.min(u64::from(DEFAULT_PAGE_SIZE));
        Some(u32::try_from(left).unwrap_or(DEFAULT_PAGE_SIZE))
    }

    /// Take a page and where the server says the next one starts, and return the requests to
    /// print. A page can be empty and still have a next one, as the server leaves out any
    /// request it can't read.
    fn take<T>(&mut self, mut items: Vec<T>, next: Option<IndexingRequestId>) -> Result<Vec<T>> {
        if let Some(left) = self.left {
            items.truncate(usize::try_from(left).unwrap_or(usize::MAX));
            self.left = Some(left.saturating_sub(items.len() as u64));
        }
        // Without this a server that kept answering with the same page would never let go.
        if next.is_some() && Some(next) == self.start {
            return Err(anyhow::anyhow!(
                "Failed to list indexing requests: the server returned a page that does not \
                 move on from the one before"
            )
            .into());
        }
        self.start = match next {
            Some(next) if self.left != Some(0) => Some(Some(next)),
            _ => None,
        };
        Ok(items)
    }
}

/// Writes values as 1 pretty JSON array a few at a time, in the layout
/// `serde_json::to_string_pretty` gives a whole list.
#[derive(Default)]
struct JsonArray {
    /// Whether any value has been written yet.
    started: bool,
}

impl JsonArray {
    /// The text that adds `items` to the array.
    fn items<T: serde::Serialize>(&mut self, items: &[T]) -> Result<String> {
        let mut text = String::new();
        for item in items {
            let json = serde_json::to_string_pretty(item)
                .map_err(|err| anyhow::anyhow!("Failed to serialize indexing requests: {err}"))?;
            text.push_str(if self.started { ",\n" } else { "[\n" });
            self.started = true;
            text.push_str("  ");
            text.push_str(&json.replace('\n', "\n  "));
        }
        Ok(text)
    }

    /// The text that closes the array.
    fn end(&self) -> &'static str {
        if self.started { "\n]" } else { "[]" }
    }
}

/// Parses the status to list requests by.
fn parse_status(s: &str) -> std::result::Result<IndexingRequestStatus, anyhow::Error> {
    match s.to_ascii_lowercase().as_str() {
        "open" => Ok(IndexingRequestStatus::Open),
        "canceled" | "cancelled" => Ok(IndexingRequestStatus::Canceled),
        _ => Err(anyhow::anyhow!(
            "Invalid status '{s}': expected open or canceled"
        )),
    }
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
    let signer_eip712_domain = signer::eip712_domain(protocol_chain_id(&conf)?);

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
            signed_at: dipper_core::time::now_secs(),
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
            "no-op: no open indexing request exists for deployment '{request_deployment_id}' on chain {request_chain_id}; \
             if one was made on another chain, `indexings status {request_deployment_id}` shows it, then pass its --chain-id"
        ),
    }

    Ok(())
}

/// The chain to sign a request for. There is no default to fall back on: a dipper refuses a
/// request signed for any chain but the one it serves, so a guess would only be refused.
fn protocol_chain_id(conf: &Config) -> anyhow::Result<ChainId> {
    conf.protocol_chain_id.ok_or_else(|| {
        anyhow::anyhow!(
            "Missing the chain the DIPs contracts are on: pass --protocol-chain-id or set \
             DIPS_PROTOCOL_CHAIN_ID (42161 for Arbitrum One, 421614 for Arbitrum Sepolia)"
        )
    })
}

/// Create the `indexings` DIPs indexing requests admin command
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
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
                .about("List indexing requests, oldest first")
                .args([
                    arg!(--status <STATUS> "Only list requests in this status: open or canceled")
                        .value_parser(parse_status)
                        .required(false),
                    arg!(--limit <N> "Stop after this many requests")
                        .value_parser(value_parser!(u64).range(1..))
                        .required(false),
                ]),
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
                    arg!(--"protocol-chain-id" <ID> "The chain the DIPs contracts are on, which the request is signed for: 42161 for Arbitrum One, 421614 for Arbitrum Sepolia (env DIPS_PROTOCOL_CHAIN_ID)")
                        .value_parser(value_parser!(ChainId))
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

    /// Parse `indexings list` followed by `args`.
    fn parse_list(args: &[&str]) -> std::result::Result<clap::ArgMatches, clap::Error> {
        let base = ["indexings", "--server-url", "http://localhost:9000", "list"];
        cmd().try_get_matches_from(base.into_iter().chain(args.iter().copied()))
    }

    #[test]
    fn list_takes_a_status_and_a_limit_and_needs_neither() {
        let matches = parse_list(&[]).unwrap();
        let (_, matches) = matches.subcommand().unwrap();
        assert_eq!(matches.get_one::<IndexingRequestStatus>("status"), None);
        assert_eq!(matches.get_one::<u64>("limit"), None);

        for (word, status) in [
            ("open", IndexingRequestStatus::Open),
            ("OPEN", IndexingRequestStatus::Open),
            ("canceled", IndexingRequestStatus::Canceled),
            ("cancelled", IndexingRequestStatus::Canceled),
        ] {
            let matches = parse_list(&["--status", word, "--limit", "25"]).unwrap();
            let (_, matches) = matches.subcommand().unwrap();
            assert_eq!(
                matches.get_one::<IndexingRequestStatus>("status"),
                Some(&status),
                "{word}"
            );
            assert_eq!(matches.get_one::<u64>("limit"), Some(&25));
        }
    }

    #[test]
    fn list_refuses_a_status_it_does_not_know_and_a_limit_of_0() {
        assert!(parse_list(&["--status", "pending"]).is_err());
        assert!(parse_list(&["--limit", "0"]).is_err());
    }

    /// A CLI from before paging reads the reply as a bare list. It has to fail on a page, not
    /// take the first page for the whole list.
    #[test]
    fn a_page_of_requests_does_not_read_as_the_bare_list_it_replaced() {
        use dipper_rpc::admin::indexing_requests::{IndexingRequest, IndexingRequestsPage};
        let page = IndexingRequestsPage {
            requests: Vec::new(),
            next: None,
        };
        let reply = serde_json::to_value(&page).unwrap();

        assert_eq!(reply, serde_json::json!({ "requests": [], "next": null }));
        assert!(serde_json::from_value::<Vec<IndexingRequest>>(reply).is_err());
    }

    /// `count` ids, in the order they were made.
    fn ids(count: usize) -> Vec<IndexingRequestId> {
        (0..count).map(|_| IndexingRequestId::new()).collect()
    }

    #[test]
    fn a_listing_with_no_limit_follows_the_pages_to_the_last() {
        let ids = ids(2);
        let mut paging = Paging::new(None);

        assert_eq!(paging.start(), Some(None));
        // The server picks the page size.
        assert_eq!(paging.ask(), None);
        assert_eq!(paging.take(vec![1, 2], Some(ids[0])).unwrap(), [1, 2]);

        assert_eq!(paging.start(), Some(Some(ids[0])));
        // An empty page with a next one is carried on from, not taken as the end.
        assert!(
            paging
                .take(Vec::<i32>::new(), Some(ids[1]))
                .unwrap()
                .is_empty()
        );

        assert_eq!(paging.start(), Some(Some(ids[1])));
        assert_eq!(paging.take(vec![3], None).unwrap(), [3]);

        assert_eq!(paging.start(), None);
    }

    #[test]
    fn a_listing_with_a_limit_asks_for_no_more_than_is_left_and_stops_there() {
        let ids = ids(3);
        let mut paging = Paging::new(Some(2_500));

        assert_eq!(paging.ask(), Some(1_000));
        assert_eq!(
            paging.take(vec![0; 1_000], Some(ids[0])).unwrap().len(),
            1_000
        );
        assert_eq!(paging.ask(), Some(1_000));
        assert_eq!(
            paging.take(vec![0; 1_000], Some(ids[1])).unwrap().len(),
            1_000
        );
        assert_eq!(paging.ask(), Some(500));
        // A server that sends more than was asked for still can't push past the limit, and the
        // listing ends though another page is on offer.
        assert_eq!(paging.take(vec![0; 700], Some(ids[2])).unwrap().len(), 500);

        assert_eq!(paging.start(), None);
    }

    #[test]
    fn a_listing_stops_with_an_error_when_the_server_repeats_a_page() {
        let ids = ids(1);
        let mut paging = Paging::new(None);
        paging.take(vec![1], Some(ids[0])).unwrap();

        let err = paging.take(vec![1], Some(ids[0])).unwrap_err();

        assert!(err.to_string().contains("does not move on"), "{err}");
    }

    #[test]
    fn requests_printed_a_page_at_a_time_read_as_the_whole_list_printed_at_once() {
        let request = |n: u32| serde_json::json!({ "id": n, "status": "OPEN", "tags": [n, n] });
        for pages in [
            vec![],
            vec![vec![]],
            vec![vec![request(1)]],
            vec![vec![request(1), request(2)], vec![], vec![request(3)]],
        ] {
            let mut array = JsonArray::default();
            let mut printed = String::new();
            for page in &pages {
                printed.push_str(&array.items(page).unwrap());
            }
            printed.push_str(array.end());

            let all: Vec<_> = pages.iter().flatten().collect();
            assert_eq!(printed, serde_json::to_string_pretty(&all).unwrap());
        }
    }

    #[test]
    fn test_set_target_candidates_needs_no_chain() {
        let matches = parse_set_target(&[]).unwrap();
        let (_, matches) = matches.subcommand().unwrap();

        assert_eq!(matches.get_one::<String>("chain-name"), None);
        assert_eq!(matches.get_one::<ChainId>("chain-id"), None);
    }

    #[test]
    fn test_set_target_candidates_signs_for_the_protocol_chain_it_is_given() {
        let matches = parse_set_target(&["--protocol-chain-id", "421614"]).unwrap();
        let (_, matches) = matches.subcommand().unwrap();

        let conf = common::load_conf(matches).unwrap();

        assert_eq!(protocol_chain_id(&conf).unwrap(), 421_614);
    }

    #[test]
    fn test_set_target_candidates_asks_for_a_missing_protocol_chain() {
        let matches = parse_set_target(&[]).unwrap();
        let (_, matches) = matches.subcommand().unwrap();
        let conf = common::load_conf(matches).unwrap();

        let err = protocol_chain_id(&conf).unwrap_err().to_string();

        assert!(err.contains("--protocol-chain-id"), "{err}");
        assert!(err.contains("DIPS_PROTOCOL_CHAIN_ID"), "{err}");
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
