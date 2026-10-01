use std::path::{Path, PathBuf};

use clap::{ArgMatches, Command, arg, command, value_parser};
use thegraph_core::alloy::{primitives::B256, signers::local::PrivateKeySigner};
use url::Url;

use super::{common, result::Result};

/// The `init` command implementation
pub(super) async fn run(args: &ArgMatches) -> Result<()> {
    match args.subcommand() {
        Some(("keygen", matches)) => keygen(matches),
        Some(("placeholder", matches)) => placeholder(matches),
        _ => Err(
            anyhow::anyhow!("One of the subcommands must be provided: keygen or placeholder")
                .into(),
        ),
    }
}

/// The `init keygen` command implementation
fn keygen(args: &ArgMatches) -> Result<()> {
    // Get the arguments
    let server_url = args.get_one::<Url>("server-url");
    let output_path = args.get_one::<PathBuf>("output");

    // Generate a new random key pair
    let signer = PrivateKeySigner::random();

    // Get the public and private keys in hex format
    let public_key = format!("{:#x}", signer.address());
    let private_key = format!("{:#x}", B256::from_slice(signer.to_bytes().as_slice()));

    // Print the keys for human consumption
    indoc::eprintdoc! {r#"
        Generated new key pair:

        Public key:  {public_key}
        Private key: {private_key}

        Make sure to save these keys securely. The private key will be used to sign requests.
    "#};

    // If output path is not provided, do not write the config
    let output_path = if let Some(path) = output_path {
        path
    } else {
        return Ok(());
    };

    // If server URL is not provided, we can't write the config
    let server_url = server_url
        .ok_or_else(|| anyhow::anyhow!("Server URL is required when writing to a file"))?;

    // Print a warning about security risks
    indoc::eprintdoc! {r#"
        ================================================================================
        WARNING: Security Risk ⚠️⚠️⚠️

        You are storing a private key in an environment file.
        This is potentially insecure as environment files may be:
          - Committed to version control accidentally
          - Readable by other users or processes on this system
          - Included in backups or logs

        Consider using a secure credential manager instead.
        ================================================================================
    "#};

    // Create and write the config
    write_config(server_url, &private_key, output_path)?;

    eprintln!("\nConfiguration written to: {}", output_path.display());

    Ok(())
}

/// The `init placeholder` command implementation
fn placeholder(args: &ArgMatches) -> Result<()> {
    // Get the arguments
    let server_url = args.get_one::<Url>("server-url");
    let output_path = args.get_one::<PathBuf>("output");
    let placeholder = args
        .get_one::<String>("PLACEHOLDER")
        .ok_or_else(|| anyhow::anyhow!("Placeholder is required"))?;

    // For 1Password integration, we use the placeholder directly
    // This will be replaced by `op inject` at runtime
    indoc::eprintdoc! {r#"
        Generated placeholder for 1Password's `op inject` integration:

        Secret key placeholder: {placeholder}

        This placeholder will be replaced by `op inject` at runtime
        To use this configuration, run commands with:

          op inject --in-file .env.template --out-file .env.injected -- dipper-cli --env-file .env.injected <command>
    "#};

    // Wrap the placeholder in double curly braces for `op inject` 1Password integration
    let signing_key = format!("{{{{ {} }}}}", placeholder);

    // If output path is not provided, do not write the config
    let output_path = if let Some(path) = output_path {
        path
    } else {
        &PathBuf::from(".env.template")
    };

    // If server URL is not provided, we can't write the config
    let server_url = server_url
        .ok_or_else(|| anyhow::anyhow!("Server URL is required when writing to a file"))?;

    // Create and write the config
    write_config(server_url, &signing_key, output_path)?;

    eprintln!("\nConfiguration written to: {}", output_path.display());

    Ok(())
}

/// Helper function to write the configuration to a file
fn write_config(server_url: &Url, signing_key: &str, output_path: &Path) -> Result<()> {
    /// Internal config struct for the .env file
    #[derive(serde::Serialize)]
    struct EnvFile {
        #[serde(rename = "DIPS_SERVER_URL")]
        server_url: String,
        #[serde(rename = "DIPS_SIGNING_KEY")]
        signing_key: String,
    }

    // Create the env file struct
    let config = EnvFile {
        server_url: server_url.to_string(),
        signing_key: signing_key.to_string(),
    };

    // Serialize the config to env file format
    let env_content = serde_envfile::to_string(&config)
        .map_err(|err| anyhow::anyhow!("Failed to serialize config: {}", err))?;

    // Write the configuration to the output file
    std::fs::write(output_path, env_content)
        .map_err(|err| anyhow::anyhow!("Failed to write config file: {}", err))?;

    Ok(())
}

/// Create the `init` DIPs CLI configuration bootstrap command
#[expect(
    clippy::cognitive_complexity,
    reason = "predates this lint; fix when next touched"
)]
pub(super) fn cmd() -> Command {
    command!("init")
        .about("Bootstrap the DIPs Admin CLI configuration file")
        .subcommands([
            command!("keygen")
                .alias("gen")
                .about("Generate a new secret key to sign requests with")
                .args([
                    common::server_url_arg(),
                    arg!(-o --"output" <FILE> "The output file to write the configuration to")
                        .value_parser(value_parser!(PathBuf)),
                ]),
            command!("placeholder")
                .alias("op")
                .about("Use a placeholder for the secret key (e.g., for 1Password integration)")
                .args([
                    arg!(<PLACEHOLDER> "The placeholder for the secret key, e.g., 'op://mainnet/dips/signing-key-1'")
                        .value_parser(value_parser!(String)),
                    common::server_url_arg(),
                    arg!(-o --"output" <FILE> "The output file to write the configuration to")
                        .value_parser(value_parser!(PathBuf)),
                ]),
        ])
}
