use std::path::PathBuf;

use clap::{Arg, ArgMatches, Error, FromArgMatches, arg, value_parser};
use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use figment::{
    Figment,
    providers::{Env, Serialized},
};
use serde_with::{DisplayFromStr, serde_as, skip_serializing_none};
use thegraph_core::alloy::signers::k256::SecretKey;
use url::Url;

use crate::config::Config;

/// The prefix for environment variables
pub const ENV_PREFIX: &str = "DIPS_";

/// Macro rule to prepend the `DIPS_` prefix to environment variable name
#[macro_export]
#[doc(hidden)]
macro_rules! name_prefixed {
    ($name:literal) => {
        concat!("DIPS_", $name)
    };
}

/// Create the `--env-file` CLI argument.
///
/// This argument is used to specify the path to a `.env` file to load configuration from.
///
/// Parse the value as a `PathBuf`.
pub(super) fn env_file_arg() -> Arg {
    arg!(-e --"env-file" <FILE> "The .env file to load configuration from")
        .value_parser(value_parser!(PathBuf))
}

/// Create the `--server-url` CLI argument.
///
/// This argument is used to specify the URL of the DIPs gateway server.
///
/// Parse the value as a `Url`.
pub(super) fn server_url_arg() -> Arg {
    arg!(--"server-url" <URL> "The URL of the DIPs gateway server")
        .env(name_prefixed!("SERVER_URL"))
        .value_parser(value_parser!(Url))
}

/// Create the `--signing-key` CLI argument.
///
/// This argument is used to specify the secret key to sign requests with.
///
/// Parse the value as a `Hidden<SecretKey>`.
pub(super) fn signing_key_arg() -> Arg {
    arg!(--"signing-key" <KEY> "The secret key to sign requests with (hex)")
        .env(name_prefixed!("SIGNING_KEY"))
        .hide_env_values(true)
        .value_parser(dipper_core::config::secret_key_from_str)
}

/// Load the configuration
pub fn load_conf(args: &ArgMatches) -> anyhow::Result<Config> {
    // Load the environment variables from the specified env file
    if let Some(env_file) = args.get_one::<PathBuf>("env-file") {
        if let Err(err) = dotenvy::from_path(env_file) {
            return Err(anyhow::anyhow!(
                "Failed to load the envfile '{}': {}",
                env_file.display(),
                err
            ));
        } else {
            tracing::debug!("Loaded envfile '{}'", env_file.display());
        }
    }

    // Load configuration from the CLI arguments
    let cli_conf = CliConfig::from_arg_matches(args)?;

    // Combine the configuration from the environment and the CLI
    let conf = Figment::new()
        .merge(Env::prefixed(ENV_PREFIX))
        .merge(Serialized::defaults(cli_conf))
        .extract()?;

    Ok(conf)
}

/// The CLI provided config
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct CliConfig {
    /// The URL of the DIPs gateway server
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub server_url: Option<Url>,
    /// The secret key to sign requests with
    #[serde_as(as = "Option<HiddenSecretKeyAsHexStr>")]
    pub signing_key: Option<Hidden<SecretKey>>,
    /// The IPFS API to read subgraph manifests from
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub ipfs_url: Option<Url>,
}

impl FromArgMatches for CliConfig {
    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, Error> {
        Ok(Self {
            server_url: matches.get_one("server-url").cloned(),
            signing_key: matches.get_one("signing-key").cloned(),
            // Only some subcommands define this flag.
            ipfs_url: matches.try_get_one("ipfs-url").ok().flatten().cloned(),
        })
    }

    fn update_from_arg_matches(&mut self, _matches: &ArgMatches) -> Result<(), Error> {
        unimplemented!()
    }
}
