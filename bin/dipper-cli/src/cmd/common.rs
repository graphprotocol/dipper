use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::{
    Arg, ArgMatches, Error, FromArgMatches, arg,
    error::ErrorKind,
    parser::{MatchesError, ValueSource},
    value_parser,
};
use dipper_core::config::{Hidden, HiddenSecretKeyAsHexStr};
use figment::{Figment, providers::Serialized};
use serde_with::{DisplayFromStr, serde_as, skip_serializing_none};
use thegraph_core::alloy::signers::k256::SecretKey;
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

/// Create the `--signing-key` CLI argument: the secret key to sign requests with.
pub(super) fn signing_key_arg() -> Arg {
    arg!(--"signing-key" <KEY> "The secret key to sign requests with (hex)")
        .env(name_prefixed!("SIGNING_KEY"))
        .hide_env_values(true)
        .value_parser(dipper_core::config::secret_key_from_str)
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

/// Build the configuration, taking each setting from the first source that has it: a flag typed
/// on the command line, `shell` (the variables the shell set), then 1 file, the one `--env-file`
/// names or else the one in `current_dir`.
fn load_conf_from(
    args: &ArgMatches,
    shell: impl IntoIterator<Item = (String, String)>,
    current_dir: &Path,
) -> anyhow::Result<Loaded> {
    let shell = settings(shell);
    let mut warnings = Vec::new();
    // The named file replaces the one in the directory, so that one can't fill in a setting the
    // named file leaves out, such as a production signing key under a staging server.
    let file = match args.get_one::<PathBuf>("env-file") {
        Some(path) => {
            let named = read_env_file(path)?;
            warnings.extend(overridden_settings(path, &named, &shell, args));
            named
        }
        None => read_env_file_in(current_dir, &mut warnings),
    };
    let cli_conf = CliConfig::from_arg_matches(args)?;

    let conf = Figment::new()
        .merge(Serialized::defaults(file))
        .merge(Serialized::defaults(shell))
        .merge(Serialized::defaults(cli_conf))
        .extract()?;

    Ok(Loaded { conf, warnings })
}

/// A warning for each setting in the named file that a flag or the shell gives another value. It
/// names the setting but never shows a value, which may be a secret.
fn overridden_settings(
    path: &Path,
    file: &BTreeMap<String, String>,
    shell: &BTreeMap<String, String>,
    args: &ArgMatches,
) -> Vec<String> {
    file.iter()
        .filter_map(|(setting, value)| {
            let flag = setting.replace('_', "-");
            let overrider = match typed_value(args, &flag) {
                Some(typed) => (typed != *value).then(|| format!("the --{flag} flag")),
                None => shell
                    .get(setting)
                    .filter(|shell_value| *shell_value != value)
                    .map(|_| "the shell environment".to_owned()),
            }?;
            Some(format!(
                "warning: ignoring {ENV_PREFIX}{} from {}: {overrider} sets it",
                setting.to_ascii_uppercase(),
                path.display()
            ))
        })
        .collect()
}

/// The settings in the file named [`ENV_FILE_NAME`] in `dir`. Only `dir` is searched: a file in a
/// directory above, such as a home folder, could point a command at the wrong server. A file that
/// can't be read is skipped as before, but now with a warning instead of only a debug log.
fn read_env_file_in(dir: &Path, warnings: &mut Vec<String>) -> BTreeMap<String, String> {
    let path = dir.join(ENV_FILE_NAME);
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
    }
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

/// The `id` flag's parsed value when it was typed on the command line. Values clap took from the
/// environment are left out: the shell's variables are read separately, so they rank the same
/// wherever they come from.
fn typed<T: Clone + Send + Sync + 'static>(
    args: &ArgMatches,
    id: &str,
) -> Result<Option<T>, Error> {
    match args.try_get_one::<T>(id) {
        Ok(value) => Ok(value
            .filter(|_| args.value_source(id) == Some(ValueSource::CommandLine))
            .cloned()),
        // Only some subcommands define each flag.
        Err(MatchesError::UnknownArgument { .. }) => Ok(None),
        Err(err) => Err(Error::raw(
            ErrorKind::InvalidValue,
            format!("Failed to read --{id}: {err}"),
        )),
    }
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
            server_url: typed(matches, "server-url")?,
            signing_key: typed(matches, "signing-key")?,
            ipfs_url: typed(matches, "ipfs-url")?,
        })
    }

    fn update_from_arg_matches(&mut self, _matches: &ArgMatches) -> Result<(), Error> {
        unimplemented!()
    }
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
    /// is not worth a warning.
    #[test]
    fn no_warning_when_the_shell_or_a_flag_gives_the_named_files_own_value() {
        let folder = Folder::new("same-value");
        let staging = folder.file(
            "staging",
            &format!("DIPS_SERVER_URL=https://staging.example\nDIPS_SIGNING_KEY={KEY}\n"),
        );

        let loaded = load(
            &[
                "--env-file",
                path(&staging),
                "--server-url",
                "https://staging.example",
            ],
            &[("DIPS_SIGNING_KEY", KEY)],
            &folder.0,
        );

        assert_eq!(loaded.conf.server_url.as_str(), "https://staging.example/");
        assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
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
    /// and the flags, and overriding it needs no warning.
    #[test]
    fn a_file_in_the_current_directory_is_read_when_none_is_named() {
        let folder = Folder::new("found-when-unnamed");
        folder.file(
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
        assert!(loaded.warnings.is_empty(), "{:?}", loaded.warnings);
    }
}
