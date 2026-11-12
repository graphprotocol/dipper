use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::{Arg, ArgMatches, arg, parser::ValueSource, value_parser};
use dipper_core::config::secret_key_from_str;
use figment::{Figment, providers::Serialized};
use thegraph_core::alloy::primitives::ChainId;
use url::Url;

use crate::config::Config;

/// The prefix for environment variables
pub const ENV_PREFIX: &str = "DIPS_";

/// The name of the file the CLI looks for in the current directory when `--env-file` is not given.
const ENV_FILE_NAME: &str = ".env";

/// Macro rule to prepend the `DIPS_` prefix to environment variable name
#[macro_export]
#[doc(hidden)]
macro_rules! name_prefixed {
    ($name:literal) => {
        concat!("DIPS_", $name)
    };
}

/// Create the `--env-file` CLI argument: the path to a `.env` file to load configuration from.
pub(super) fn env_file_arg() -> Arg {
    arg!(-e --"env-file" <FILE> "The .env file to load configuration from")
        .value_parser(value_parser!(PathBuf))
}

/// Create the `--server-url` CLI argument: the URL of the DIPs gateway server.
pub(super) fn server_url_arg() -> Arg {
    arg!(--"server-url" <URL> "The URL of the DIPs gateway server")
        .env(name_prefixed!("SERVER_URL"))
        .value_parser(value_parser!(Url))
}

/// Create the `--signing-key` CLI argument: the secret key to sign requests with. It is checked
/// when the settings load, not by clap, whose error for a value it can't parse quotes the value.
/// For the same reason clap does not read the variable; the settings loader does.
pub(super) fn signing_key_arg() -> Arg {
    arg!(--"signing-key" <KEY> "The secret key to sign requests with (hex) [env: DIPS_SIGNING_KEY]")
}

/// Load the configuration from the command line, the shell and a file, printing a warning on
/// stderr for each setting in the `--env-file` file that another source overrides.
pub fn load_conf(args: &ArgMatches) -> anyhow::Result<Config> {
    let shell = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
    let current_dir = std::env::current_dir()
        .map_err(|err| anyhow::anyhow!("Failed to read the current directory: {err}"))?;

    let loaded = load_conf_from(args, shell, &current_dir)?;
    for warning in &loaded.warnings {
        eprintln!("{warning}");
    }
    Ok(loaded.conf)
}

/// The configuration, and the warnings to show the operator about where it came from.
struct Loaded {
    conf: Config,
    warnings: Vec<String>,
}

/// The flags that set a setting each: `--server-url` sets `server_url`.
const SETTING_FLAGS: [&str; 4] = ["server-url", "signing-key", "ipfs-url", "protocol-chain-id"];

/// Where a setting's value came from.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Source {
    /// Typed on the command line as this flag.
    Flag(&'static str),
    /// Set in the shell's environment.
    Shell,
    /// Read from this file, named with `--env-file` or found in the current directory.
    File(PathBuf),
}

impl Source {
    /// Whether 2 settings came from the same place, counting every flag as the command line.
    fn same_place(&self, other: &Self) -> bool {
        matches!((self, other), (Self::Flag(_), Self::Flag(_))) || self == other
    }
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Flag(flag) => write!(f, "the --{flag} flag"),
            Self::Shell => f.write_str("the shell environment"),
            Self::File(path) => write!(f, "{}", path.display()),
        }
    }
}

/// Build the configuration, taking each setting from the first source that has it: a flag typed
/// on the command line, `shell` (the variables the shell set), then 1 file, the one `--env-file`
/// names or else the one in `current_dir`. Warnings name settings, never their values.
fn load_conf_from(
    args: &ArgMatches,
    shell: impl IntoIterator<Item = (String, String)>,
    current_dir: &Path,
) -> anyhow::Result<Loaded> {
    let mut warnings = Vec::new();
    // The named file replaces the one in the directory, so that one can't fill in a setting the
    // named file leaves out, such as a production signing key under a staging server.
    let (file_path, file) = match args.get_one::<PathBuf>("env-file") {
        Some(path) => (path.clone(), read_env_file(path)?),
        None => read_env_file_in(current_dir, &mut warnings),
    };

    let mut chosen: BTreeMap<String, (String, Source)> = BTreeMap::new();
    for flag in SETTING_FLAGS {
        if let Some(value) = typed_value(args, flag) {
            chosen.insert(flag.replace('-', "_"), (value, Source::Flag(flag)));
        }
    }
    for (setting, value) in settings(shell) {
        chosen.entry(setting).or_insert((value, Source::Shell));
    }
    for (setting, value) in file {
        match chosen.get(&setting) {
            Some((used, source)) if !same_value(&setting, used, &value) => warnings.push(format!(
                "warning: ignoring {ENV_PREFIX}{} from {}: {source} sets it",
                setting.to_ascii_uppercase(),
                file_path.display()
            )),
            Some(_) => {}
            None => {
                chosen.insert(setting, (value, Source::File(file_path.clone())));
            }
        }
    }
    if let (Some((_, url_from)), Some((_, key_from))) =
        (chosen.get("server_url"), chosen.get("signing_key"))
        && !url_from.same_place(key_from)
    {
        warnings.push(format!(
            "warning: the server URL comes from {url_from} but the signing key from {key_from}"
        ));
    }
    // The parse error is left out on purpose: it can quote characters of the key.
    if let Some((key, source)) = chosen.get("signing_key")
        && secret_key_from_str(key).is_err()
    {
        anyhow::bail!(
            "The signing key from {source} is not a valid secret key: it must be a secp256k1 \
             private key written as 64 hex digits, with or without 0x in front"
        );
    }

    let values: BTreeMap<&str, &str> = chosen
        .iter()
        .map(|(setting, (value, _))| (setting.as_str(), value.as_str()))
        .collect();
    let conf = Figment::from(Serialized::defaults(values)).extract()?;

    Ok(Loaded { conf, warnings })
}

/// Whether 2 values of `setting` mean the same, so that one written differently, such as a URL
/// without its trailing slash or a key without `0x`, is not reported as overriding the other.
fn same_value(setting: &str, a: &str, b: &str) -> bool {
    match setting {
        "server_url" | "ipfs_url" => match (Url::parse(a), Url::parse(b)) {
            (Ok(a), Ok(b)) => a == b,
            _ => a == b,
        },
        "signing_key" => match (secret_key_from_str(a), secret_key_from_str(b)) {
            (Ok(a), Ok(b)) => a.to_bytes() == b.to_bytes(),
            _ => a == b,
        },
        "protocol_chain_id" => match (a.parse::<ChainId>(), b.parse::<ChainId>()) {
            (Ok(a), Ok(b)) => a == b,
            _ => a == b,
        },
        _ => a == b,
    }
}

/// The path of the file named [`ENV_FILE_NAME`] in `dir`, and its settings. Only `dir` is
/// searched: a file in a directory above, such as a home folder, could point a command at the
/// wrong server. A file that can't be read is skipped as before, now with a warning.
fn read_env_file_in(dir: &Path, warnings: &mut Vec<String>) -> (PathBuf, BTreeMap<String, String>) {
    let path = dir.join(ENV_FILE_NAME);
    let found =
        match dotenvy::from_path_iter(&path).and_then(|vars| vars.collect::<Result<Vec<_>, _>>()) {
            Ok(vars) => {
                tracing::debug!("Loaded envfile '{}'", path.display());
                settings(vars)
            }
            Err(err) if err.not_found() => {
                tracing::debug!("No envfile at '{}'", path.display());
                BTreeMap::new()
            }
            Err(err) => {
                warnings.push(format!(
                    "warning: skipping '{}': {}",
                    path.display(),
                    describe(&err)
                ));
                BTreeMap::new()
            }
        };
    (path, found)
}

/// What went wrong reading an env file, leaving out the text of a line that failed to parse,
/// which may hold a secret.
fn describe(err: &dotenvy::Error) -> String {
    match err {
        dotenvy::Error::LineParse(_, index) => {
            format!("a line is not in KEY=value form (error at character {index} of it)")
        }
        err => err.to_string(),
    }
}

/// The `DIPS_` variables among `vars`, keyed by the setting each names (`DIPS_SERVER_URL` sets
/// `server_url`). The prefix is matched in any case, as for variables read from the shell. When a
/// file sets a variable twice the 1st one counts, as it always has.
fn settings(vars: impl IntoIterator<Item = (String, String)>) -> BTreeMap<String, String> {
    let mut settings = BTreeMap::new();
    for (key, value) in vars {
        let Some(setting) = key
            .get(..ENV_PREFIX.len())
            .filter(|prefix| prefix.eq_ignore_ascii_case(ENV_PREFIX))
            .and_then(|_| key.get(ENV_PREFIX.len()..))
        else {
            continue;
        };
        settings
            .entry(setting.to_ascii_lowercase())
            .or_insert(value);
    }
    settings
}

/// The `DIPS_` settings in the file at `path`.
fn read_env_file(path: &Path) -> anyhow::Result<BTreeMap<String, String>> {
    let vars = dotenvy::from_path_iter(path)
        .and_then(|vars| vars.collect::<Result<Vec<_>, _>>())
        .map_err(|err| {
            anyhow::anyhow!(
                "Failed to load the envfile '{}': {}",
                path.display(),
                describe(&err)
            )
        })?;
    tracing::debug!("Loaded envfile '{}'", path.display());
    Ok(settings(vars))
}

/// The value of the `id` flag when it was typed on the command line, as typed. `None` when it was
/// not, or when this command has no such flag.
fn typed_value(args: &ArgMatches, id: &str) -> Option<String> {
    let raw = args.try_get_raw(id).ok().flatten()?.next_back()?;
    (args.value_source(id) == Some(ValueSource::CommandLine))
        .then(|| raw.to_str().map(str::to_owned))
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    const OTHER_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

    /// A folder of the test's own under the system's temporary folder, removed afterwards.
    struct Folder(PathBuf);

    impl Folder {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("dipper-cli-{}-{name}", std::process::id()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        /// Write `contents` to the file `name` in this folder and return its path.
        fn file(&self, name: &str, contents: &str) -> PathBuf {
            let path = self.0.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        }
    }

    impl Drop for Folder {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Load the configuration for `dipper-cli indexings <args> list`, with `shell` as the shell's
    /// variables and `current_dir` as the current directory.
    fn load(args: &[&str], shell: &[(&str, &str)], current_dir: &Path) -> Loaded {
        let matches = crate::cmd::indexings::cmd()
            .try_get_matches_from(
                ["indexings"]
                    .into_iter()
                    .chain(args.iter().copied())
                    .chain(["list"]),
            )
            .unwrap();
        let (_, list) = matches.subcommand().unwrap();
        let shell = shell
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()));
        load_conf_from(list, shell, current_dir).unwrap()
    }

    fn path(path: &Path) -> &str {
        path.to_str().unwrap()
    }

    fn default_ipfs_url() -> Url {
        crate::chain::DEFAULT_IPFS_URL.parse().unwrap()
    }

    /// The file an operator names is read instead of one in the current directory, so the other
    /// file cannot fill in a setting the named one leaves out either.
    #[test]
    fn a_named_env_file_beats_one_found_in_the_current_directory() {
        let folder = Folder::new("named-beats-found");
        folder.file(
            ENV_FILE_NAME,
            "DIPS_SERVER_URL=https://production.example\nDIPS_IPFS_URL=http://ipfs.production.example\n",
        );
        let staging = folder.file(
            "staging",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n"),
        );

        let loaded = load(&["--env-file", path(&staging)], &[], &folder.0);

        assert_eq!(loaded.conf.server_url.as_str(), "https://staging.example/");
        assert_eq!(loaded.conf.ipfs_url, default_ipfs_url());
        assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
    }

    /// The chain a request is signed for comes from the shell or the env file like any other
    /// setting, and the shell wins with a warning.
    #[test]
    fn the_protocol_chain_is_read_from_the_shell_and_from_an_env_file() {
        let folder = Folder::new("protocol-chain");
        let staging = folder.file(
            "staging",
            &format!(
                "DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n\
                 DIPS_PROTOCOL_CHAIN_ID=421614\n"
            ),
        );

        let from_file = load(&["--env-file", path(&staging)], &[], &folder.0);
        let from_shell = load(
            &["--env-file", path(&staging)],
            &[("DIPS_PROTOCOL_CHAIN_ID", "42161")],
            &folder.0,
        );
        let same_in_both = load(
            &["--env-file", path(&staging)],
            &[("DIPS_PROTOCOL_CHAIN_ID", "0421614")],
            &folder.0,
        );

        assert_eq!(from_file.conf.protocol_chain_id, Some(421_614));
        assert_eq!(from_shell.conf.protocol_chain_id, Some(42_161));
        assert_eq!(
            from_shell.warnings,
            [format!(
                "warning: ignoring DIPS_PROTOCOL_CHAIN_ID from {}: the shell environment sets it",
                staging.display()
            )]
        );
        assert!(
            same_in_both.warnings.is_empty(),
            "{:?}",
            same_in_both.warnings
        );
    }

    /// A file in a directory above the current one is never read, so one left in a home folder or
    /// at a repository's root cannot configure a command run further down.
    #[test]
    fn a_file_in_a_parent_directory_is_not_read() {
        let parent = Folder::new("parent-not-read");
        parent.file(
            ENV_FILE_NAME,
            &format!(
                "DIPS_SERVER_URL=https://production.example\nDIPS_SIGNING_KEY={OTHER_KEY}\n\
                 DIPS_IPFS_URL=http://ipfs.production.example\n"
            ),
        );
        let below = parent.0.join("below");
        std::fs::create_dir(&below).unwrap();
        let staging = parent.file(
            "staging",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n"),
        );

        let named = load(&["--env-file", path(&staging)], &[], &below);
        let unnamed = load(
            &[],
            &[
                ("DIPS_SERVER_URL", "https://shell.example"),
                ("DIPS_SIGNING_KEY", KEY),
            ],
            &below,
        );

        assert_eq!(named.conf.server_url.as_str(), "https://staging.example/");
        assert_eq!(unnamed.conf.ipfs_url, default_ipfs_url());
    }

    /// A variable the shell already set wins over the named file, as in most tools, but the
    /// operator is told which settings the file did not get to set.
    #[test]
    fn the_shell_beats_a_named_env_file_with_a_warning_naming_each_setting() {
        let folder = Folder::new("shell-beats-named");
        let staging = folder.file(
            "staging",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n"),
        );

        let loaded = load(
            &["--env-file", path(&staging)],
            &[
                ("DIPS_SERVER_URL", "https://production.example"),
                ("DIPS_SIGNING_KEY", OTHER_KEY),
            ],
            &folder.0,
        );

        assert_eq!(
            loaded.conf.server_url.as_str(),
            "https://production.example/"
        );
        // Named, never shown: the value may be a secret.
        assert_eq!(
            loaded.warnings,
            [
                format!(
                    "warning: ignoring DIPS_SERVER_URL from {}: the shell environment sets it",
                    staging.display()
                ),
                format!(
                    "warning: ignoring DIPS_SIGNING_KEY from {}: the shell environment sets it",
                    staging.display()
                ),
            ]
        );
    }

    /// A flag wins over the named file and the shell, with a warning naming each file setting it
    /// overrode.
    #[test]
    fn a_flag_beats_a_named_env_file_with_a_warning_naming_each_setting() {
        let folder = Folder::new("flag-beats-named");
        let staging = folder.file(
            "staging",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n"),
        );

        let loaded = load(
            &[
                "--env-file",
                path(&staging),
                "--server-url",
                "https://production.example",
                "--signing-key",
                OTHER_KEY,
            ],
            &[("DIPS_SERVER_URL", "https://shell.example")],
            &folder.0,
        );

        assert_eq!(
            loaded.conf.server_url.as_str(),
            "https://production.example/"
        );
        assert_eq!(
            loaded.warnings,
            [
                format!(
                    "warning: ignoring DIPS_SERVER_URL from {}: the --server-url flag sets it",
                    staging.display()
                ),
                format!(
                    "warning: ignoring DIPS_SIGNING_KEY from {}: the --signing-key flag sets it",
                    staging.display()
                ),
            ]
        );
    }

    /// A shell variable or flag giving the same value as the named file changes nothing, so it
    /// is not worth a warning, even when it is written differently.
    #[test]
    fn no_warning_when_the_shell_or_a_flag_gives_the_named_files_own_value() {
        let folder = Folder::new("same-value");
        let staging = folder.file(
            "staging",
            &format!(
                "DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n\
                 DIPS_IPFS_URL=http://ipfs.staging.example\n"
            ),
        );
        let key_written_differently = KEY[2..].to_ascii_uppercase();

        let loaded = load(
            &[
                "--env-file",
                path(&staging),
                "--server-url",
                "https://staging.example/",
                "--signing-key",
                &key_written_differently,
            ],
            &[("DIPS_IPFS_URL", "http://ipfs.staging.example/")],
            &folder.0,
        );

        assert_eq!(loaded.conf.server_url.as_str(), "https://staging.example/");
        assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
    }

    /// A server URL and a signing key from different places may belong to different
    /// environments, such as a key in a file and a production URL left exported in the shell.
    #[test]
    fn a_server_url_and_signing_key_from_different_places_are_reported() {
        let folder = Folder::new("mixed-sources");
        let key_only = folder.file("key-only", &format!("DIPS_SIGNING_KEY={KEY}\n"));

        let loaded = load(
            &["--env-file", path(&key_only)],
            &[("DIPS_SERVER_URL", "https://production.example")],
            &folder.0,
        );

        assert_eq!(
            loaded.warnings,
            [format!(
                "warning: the server URL comes from the shell environment but the signing key from \
                 {}",
                key_only.display()
            )]
        );
    }

    /// A signing key that is not valid hex is reported with where it came from but not what it
    /// is, whichever way it arrives: a key with 1 wrong character is otherwise nearly all shown.
    #[test]
    fn a_malformed_signing_key_is_reported_without_its_value() {
        let folder = Folder::new("malformed-key");
        let malformed = format!("{}Q", &KEY[..KEY.len() - 1]);
        let url_only = folder.file("url-only", "DIPS_SERVER_URL=https://staging.example\n");
        let bad_key_file = folder.file(
            "bad-key",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={malformed}\n"),
        );
        let error = |args: &[&str], shell: &[(&str, &str)]| -> String {
            let parsed = crate::cmd::indexings::cmd().try_get_matches_from(
                ["indexings"]
                    .into_iter()
                    .chain(args.iter().copied())
                    .chain(["list"]),
            );
            let matches = match parsed {
                Ok(matches) => matches,
                Err(err) => return err.to_string(),
            };
            let shell = shell
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()));
            match load_conf_from(matches.subcommand().unwrap().1, shell, &folder.0) {
                Ok(_) => panic!("a malformed key was accepted"),
                Err(err) => err.to_string(),
            }
        };

        let by_flag = error(
            &["--env-file", path(&url_only), "--signing-key", &malformed],
            &[],
        );
        let by_shell = error(
            &["--env-file", path(&url_only)],
            &[("DIPS_SIGNING_KEY", &malformed)],
        );
        let by_file = error(&["--env-file", path(&bad_key_file)], &[]);

        for (message, source) in [
            (&by_flag, "the --signing-key flag".to_owned()),
            (&by_shell, "the shell environment".to_owned()),
            (&by_file, bad_key_file.display().to_string()),
        ] {
            assert!(
                !message.contains(&malformed[2..40]),
                "the key was shown: {message}"
            );
            assert!(
                message.contains(&format!("The signing key from {source} is not a valid")),
                "{message}"
            );
        }
    }

    /// clap would quote a value from the environment that it fails to parse, so it reads no
    /// signing key from there; the settings loader reads the variable instead, and the help still
    /// names it.
    #[test]
    fn clap_reads_no_signing_key_from_the_environment_but_the_help_names_the_variable() {
        let help = crate::cmd::indexings::cmd().render_long_help().to_string();

        assert_eq!(signing_key_arg().get_env(), None);
        assert!(help.contains("[env: DIPS_SIGNING_KEY]"), "{help}");
    }

    /// A value clap took from the environment for a flag is left out, since the shell's variables
    /// are read separately so they rank the same however they arrive. `PATH` stands in for such a
    /// variable because every test run has it, and this test must not change the environment.
    #[test]
    fn a_flag_value_clap_took_from_the_environment_is_left_out() {
        let folder = Folder::new("clap-env");
        let staging = folder.file(
            "staging",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n"),
        );
        let matches = clap::Command::new("test")
            .args([
                env_file_arg(),
                Arg::new("server-url").long("server-url").env("PATH"),
            ])
            .try_get_matches_from(["test", "--env-file", path(&staging)])
            .unwrap();
        assert_eq!(
            matches.value_source("server-url"),
            Some(ValueSource::EnvVariable)
        );

        let loaded = load_conf_from(&matches, Vec::<(String, String)>::new(), &folder.0).unwrap();

        assert_eq!(loaded.conf.server_url.as_str(), "https://staging.example/");
        assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
    }

    /// The agreements command loads its settings the same way as the indexings command.
    #[test]
    fn the_agreements_command_reports_a_named_file_overridden_by_the_shell() {
        let folder = Folder::new("agreements");
        let staging = folder.file(
            "staging",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n"),
        );
        let matches = crate::cmd::agreements::cmd()
            .try_get_matches_from([
                "agreements",
                "--env-file",
                path(&staging),
                "list",
                "0196f6d2-4ae0-7a1c-9b7e-3d2c1b0a9f8e",
            ])
            .unwrap();

        let loaded = load_conf_from(
            matches.subcommand().unwrap().1,
            [(
                "DIPS_SERVER_URL".to_owned(),
                "https://production.example".to_owned(),
            )],
            &folder.0,
        )
        .unwrap();

        assert_eq!(
            loaded.conf.server_url.as_str(),
            "https://production.example/"
        );
        assert_eq!(
            loaded.warnings,
            [
                format!(
                    "warning: ignoring DIPS_SERVER_URL from {}: the shell environment sets it",
                    staging.display()
                ),
                format!(
                    "warning: the server URL comes from the shell environment but the signing key \
                     from {}",
                    staging.display()
                ),
            ]
        );
    }

    /// A file in the current directory that can't be parsed is skipped, as before, but now with a
    /// warning, and neither that nor the error for a named file repeats the line, which may hold
    /// a secret.
    #[test]
    fn a_file_that_cannot_be_parsed_is_reported_without_its_contents() {
        let folder = Folder::new("unparsable");
        let unparsable = format!("DIPS_SIGNING_KEY=\"{KEY}\n");
        let found = folder.file(ENV_FILE_NAME, &unparsable);
        let named = folder.file("named", &unparsable);

        let loaded = load(
            &[],
            &[
                ("DIPS_SERVER_URL", "https://shell.example"),
                ("DIPS_SIGNING_KEY", OTHER_KEY),
            ],
            &folder.0,
        );
        let matches = crate::cmd::indexings::cmd()
            .try_get_matches_from(["indexings", "--env-file", path(&named), "list"])
            .unwrap();
        let named_error = load_conf_from(
            matches.subcommand().unwrap().1,
            Vec::<(String, String)>::new(),
            &folder.0,
        )
        .err()
        .unwrap()
        .to_string();

        assert_eq!(loaded.warnings.len(), 1, "{:?}", loaded.warnings);
        assert!(
            loaded.warnings[0].starts_with(&format!("warning: skipping '{}': ", found.display())),
            "{}",
            loaded.warnings[0]
        );
        for message in [&loaded.warnings[0], &named_error] {
            assert!(
                !message.contains(&KEY[2..]),
                "a secret was shown: {message}"
            );
        }
        assert!(
            named_error.starts_with(&format!("Failed to load the envfile '{}'", named.display())),
            "{named_error}"
        );
    }

    /// With no file named, one in the current directory still supplies settings, below the shell
    /// and the flags, and a shell variable overriding it is reported just as for a named file:
    /// a production URL left exported would otherwise take a staging folder's command silently.
    #[test]
    fn a_file_in_the_current_directory_is_read_and_overrides_of_it_reported() {
        let folder = Folder::new("found-when-unnamed");
        let found = folder.file(
            ENV_FILE_NAME,
            &format!(
                "DIPS_SERVER_URL=https://found.example\nDIPS_SIGNING_KEY={KEY}\n\
                 DIPS_IPFS_URL=http://ipfs.found.example\n"
            ),
        );

        let loaded = load(
            &[],
            &[("DIPS_SERVER_URL", "https://shell.example")],
            &folder.0,
        );

        assert_eq!(loaded.conf.server_url.as_str(), "https://shell.example/");
        assert_eq!(loaded.conf.ipfs_url.as_str(), "http://ipfs.found.example/");
        assert_eq!(
            loaded.warnings,
            [
                format!(
                    "warning: ignoring DIPS_SERVER_URL from {}: the shell environment sets it",
                    found.display()
                ),
                format!(
                    "warning: the server URL comes from the shell environment but the signing key \
                     from {}",
                    found.display()
                ),
            ]
        );
    }
}
