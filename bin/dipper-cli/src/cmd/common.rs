use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use clap::{Arg, ArgMatches, arg, parser::ValueSource, value_parser};
use dipper_core::config::secret_key_from_str;
use figment::{Figment, providers::Serialized};
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
        .value_parser(secret_key_from_str)
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
const SETTING_FLAGS: [&str; 3] = ["server-url", "signing-key", "ipfs-url"];

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
