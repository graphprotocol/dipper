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
          - Readable by every process that runs as your user
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
    write_private(output_path, &env_content)
        .map_err(|err| anyhow::anyhow!("Failed to write config file: {}", err))?;

    Ok(())
}

/// Write `content` to the file at `path`, replacing what is there, and leave the file readable
/// and writable by its owner alone, since it holds the key that requests are signed with.
fn write_private(path: &Path, content: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    // Opened without emptying it: if the permissions below can't be set, the file still holds
    // what it held before.
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    // A pipe or a device such as `/dev/stdout` has no contents to replace and no permissions
    // this user can set, so only a file of its own gets either.
    if file.metadata()?.is_file() {
        // The mode above only applies to a file this call creates. A file that was already
        // there keeps its own, so close it to other users before the key goes in.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        file.set_len(0)?;
    }
    file.write_all(content.as_bytes())
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

// 2 attributes, not `cfg(all(test, unix))`: clippy only lets tests unwrap under a plain `cfg(test)`.
#[cfg(test)]
#[cfg(unix)]
mod tests {
    use std::{
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
    };

    use url::Url;

    use super::write_config;

    /// A path for a test's own key file, removed again when the guard is dropped.
    struct KeyFile(PathBuf);

    impl KeyFile {
        fn new(name: &str) -> Self {
            let file = format!("dipper-cli-init-{name}-{}.env", std::process::id());
            Self(std::env::temp_dir().join(file))
        }
    }

    impl Drop for KeyFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    /// Who may read, write or run the file at `path`, as `chmod` would write it: `600` for
    /// the owner alone.
    fn mode(path: &Path) -> String {
        let bits = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        format!("{bits:o}")
    }

    fn write_key_file(path: &Path) {
        let server_url = Url::parse("http://localhost:8545").unwrap();
        write_config(&server_url, "0xsecret", path).unwrap();
    }

    #[test]
    fn test_a_new_key_file_is_readable_only_by_its_owner() {
        let key_file = KeyFile::new("new");

        write_key_file(&key_file.0);

        assert_eq!(mode(&key_file.0), "600");
    }

    /// Output that is not a file of its own, such as a pipe or `/dev/stdout`, has no permissions
    /// this user can set, and still gets the key written to it.
    #[test]
    fn test_the_key_can_be_written_to_output_that_is_not_a_file() {
        write_key_file(Path::new("/dev/null"));
    }

    #[test]
    fn test_overwriting_a_key_file_closes_it_to_other_users() {
        let key_file = KeyFile::new("existing");
        std::fs::write(&key_file.0, "DIPS_SERVER_URL=http://old\n").unwrap();
        std::fs::set_permissions(&key_file.0, std::fs::Permissions::from_mode(0o644)).unwrap();

        write_key_file(&key_file.0);

        assert_eq!(mode(&key_file.0), "600");
        let written = std::fs::read_to_string(&key_file.0).unwrap();
        assert!(written.contains("0xsecret"), "{written}");
        assert!(!written.contains("http://old"), "{written}");
    }
}
