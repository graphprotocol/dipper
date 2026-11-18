//! Where the configuration comes from besides the JSON file: environment variables that override
//! single fields, and files that each hold a single value, such as a secret mounted from a
//! Kubernetes Secret. Both are applied to the file's JSON before it is read as a [`Config`], so a
//! typo is still refused as an unknown field, and neither ever logs or reports a value it read.
//!
//! [`Config`]: super::Config

use std::{collections::BTreeMap, path::Path};

use serde_json::{Map, Value};

/// Environment variables starting with this override config fields: `DIPPER__DB__URL` sets
/// `db.url`. Each further `__` steps into a section, and names are matched in lower case.
pub const ENV_PREFIX: &str = "DIPPER__";

/// Separates the sections of a field's path in an environment variable's name.
const ENV_SEPARATOR: &str = "__";

/// A field named with this suffix holds the path of a file whose contents are the value of the
/// field without it: `"password_file": "/secrets/db-password"` sets `password`.
pub const FILE_SUFFIX: &str = "_file";

/// A reason the configuration's sources could not be combined. None of them carries a value
/// read from the environment or from a file, as those are often secrets.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SourceError {
    /// An environment override's name does not name a field.
    #[error("environment variable {variable} does not name a config field: {reason}")]
    EnvName {
        /// The variable's name.
        variable: String,
        /// What is wrong with it.
        reason: &'static str,
    },

    /// An environment override steps into a field that is not a section.
    #[error(
        "environment variable {variable} sets a field inside {section}, which is not a section"
    )]
    EnvNotASection {
        /// The variable's name.
        variable: String,
        /// The field it steps into, as a dotted path.
        section: String,
    },

    /// A field and its `_file` twin are both set, so it is unclear which to use.
    #[error("{field} and {field}{FILE_SUFFIX} are both set; set only one of them")]
    BothSet {
        /// The field, as a dotted path.
        field: String,
    },

    /// A `_file` field holds something other than a path.
    #[error("{field} must be the path of a file, as a string")]
    NotAPath {
        /// The `_file` field, as a dotted path.
        field: String,
    },

    /// A `_file` field names a file that cannot be read.
    #[error("{field} names {path}, which cannot be read: {error}")]
    Unreadable {
        /// The `_file` field, as a dotted path.
        field: String,
        /// The path it holds.
        path: String,
        /// Why reading failed.
        error: String,
    },
}

/// Applies the environment overrides in `env`, then the `_file` fields, to the config file's
/// `json`. Returns whether either changed it, so a caller can keep the file's own parse (and its
/// line numbers in errors) when nothing did.
pub fn apply(
    json: &mut Value,
    env: impl IntoIterator<Item = (String, String)>,
    read: impl Fn(&Path) -> std::io::Result<String>,
) -> Result<bool, SourceError> {
    let overridden = apply_env(json, env)?;
    let from_files = apply_files(json, &mut Vec::new(), &read)?;
    Ok(overridden || from_files)
}

/// Sets each field an environment override names. A value is read as JSON (a number, a boolean,
/// a list or a section), unless the file already holds a string there, the field is a `_file`
/// path, or the value is not JSON at all; in each of those cases it is taken as a string.
fn apply_env(
    json: &mut Value,
    env: impl IntoIterator<Item = (String, String)>,
) -> Result<bool, SourceError> {
    // Sorted, so overrides apply in the same order on every start whatever the environment's.
    let overrides: BTreeMap<String, String> = env
        .into_iter()
        .filter(|(name, _)| name.starts_with(ENV_PREFIX))
        .collect();
    for (variable, raw) in &overrides {
        let path: Vec<String> = variable[ENV_PREFIX.len()..]
            .split(ENV_SEPARATOR)
            .map(str::to_lowercase)
            .collect();
        if path.iter().any(String::is_empty) {
            return Err(SourceError::EnvName {
                variable: variable.clone(),
                reason: "a section or field name between separators is empty",
            });
        }
        let Some((field, sections)) = path.split_last() else {
            return Err(SourceError::EnvName {
                variable: variable.clone(),
                reason: "it names no field",
            });
        };
        let mut target = &mut *json;
        for (depth, section) in sections.iter().enumerate() {
            let not_a_section = || SourceError::EnvNotASection {
                variable: variable.clone(),
                section: path[..=depth].join("."),
            };
            let object = target.as_object_mut().ok_or_else(not_a_section)?;
            target = object
                .entry(section.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            if !target.is_object() {
                return Err(not_a_section());
            }
        }
        let Some(object) = target.as_object_mut() else {
            // Only the root can get here as anything but a section, when the file holds no object.
            return Err(SourceError::EnvNotASection {
                variable: variable.clone(),
                section: "the config file's root".to_string(),
            });
        };
        let keeps_a_string = field.ends_with(FILE_SUFFIX)
            || object.get(field.as_str()).is_some_and(Value::is_string);
        let value = if keeps_a_string {
            Value::String(raw.clone())
        } else {
            serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()))
        };
        object.insert(field.clone(), value);
    }
    Ok(!overrides.is_empty())
}

/// Replaces each `<field>_file` member, at any depth, by `<field>` holding the named file's
/// contents. One final line break is dropped, as editors and `echo` add one to secrets.
fn apply_files(
    json: &mut Value,
    path: &mut Vec<String>,
    read: &impl Fn(&Path) -> std::io::Result<String>,
) -> Result<bool, SourceError> {
    let mut changed = false;
    match json {
        Value::Object(object) => {
            let file_fields: Vec<String> = object
                .keys()
                .filter(|key| key.len() > FILE_SUFFIX.len() && key.ends_with(FILE_SUFFIX))
                .cloned()
                .collect();
            for file_field in file_fields {
                let field = file_field[..file_field.len() - FILE_SUFFIX.len()].to_string();
                let dotted = |name: &str| {
                    let mut parts = path.clone();
                    parts.push(name.to_string());
                    parts.join(".")
                };
                if object.contains_key(&field) {
                    return Err(SourceError::BothSet {
                        field: dotted(&field),
                    });
                }
                let file = match object.remove(&file_field) {
                    Some(Value::String(file)) => file,
                    _ => {
                        return Err(SourceError::NotAPath {
                            field: dotted(&file_field),
                        });
                    }
                };
                let mut contents =
                    read(Path::new(&file)).map_err(|err| SourceError::Unreadable {
                        field: dotted(&file_field),
                        path: file.clone(),
                        error: err.to_string(),
                    })?;
                if contents.ends_with('\n') {
                    contents.pop();
                    if contents.ends_with('\r') {
                        contents.pop();
                    }
                }
                object.insert(field, Value::String(contents));
                changed = true;
            }
            for (key, value) in object.iter_mut() {
                path.push(key.clone());
                changed |= apply_files(value, path, read)?;
                path.pop();
            }
        }
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                path.push(index.to_string());
                changed |= apply_files(item, path, read)?;
                path.pop();
            }
        }
        _ => {}
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io};

    use serde_json::json;

    use super::*;

    fn env(vars: &[(&str, &str)]) -> Vec<(String, String)> {
        vars.iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect()
    }

    /// A reader over the given files, refusing any other path.
    fn files(files: &[(&str, &str)]) -> impl Fn(&Path) -> io::Result<String> {
        let files: HashMap<String, String> = files
            .iter()
            .map(|(path, contents)| ((*path).to_string(), (*contents).to_string()))
            .collect();
        move |path: &Path| {
            files
                .get(path.to_str().unwrap())
                .cloned()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn apply_to(
        mut json: Value,
        vars: &[(&str, &str)],
        file_list: &[(&str, &str)],
    ) -> Result<(Value, bool), SourceError> {
        let changed = apply(&mut json, env(vars), files(file_list))?;
        Ok((json, changed))
    }

    #[test]
    fn a_config_without_overrides_or_files_is_left_as_it_is() {
        let config = json!({"db": {"url": "postgres://db:5432/dipper", "password": "x"}});
        let (json, changed) = apply_to(config.clone(), &[("HOME", "/root")], &[]).unwrap();
        assert_eq!(json, config);
        assert!(!changed, "an unrelated variable changes nothing");
    }

    #[test]
    fn an_override_replaces_a_field_in_a_section() {
        let (json, changed) = apply_to(
            json!({"db": {"url": "postgres://localhost:5432/dipper"}}),
            &[("DIPPER__DB__URL", "postgres://db:5432/dipper")],
            &[],
        )
        .unwrap();
        assert_eq!(json, json!({"db": {"url": "postgres://db:5432/dipper"}}));
        assert!(changed);
    }

    #[test]
    fn an_override_is_read_as_json_where_the_file_holds_no_string() {
        let (json, _) = apply_to(
            json!({"db": {"max_connections": 10}}),
            &[
                ("DIPPER__DB__MAX_CONNECTIONS", "40"),
                ("DIPPER__WORKER_CONCURRENCY", "4"),
                ("DIPPER__HEALTH__ENABLED", "false"),
            ],
            &[],
        )
        .unwrap();
        assert_eq!(
            json,
            json!({"db": {"max_connections": 40}, "worker_concurrency": 4, "health": {"enabled": false}}),
            "numbers and booleans keep their type, and a missing section is created"
        );
    }

    #[test]
    fn an_override_of_a_string_stays_a_string_even_when_it_looks_like_a_number() {
        let (json, _) = apply_to(
            json!({"db": {"password": "placeholder"}}),
            &[("DIPPER__DB__PASSWORD", "1234")],
            &[],
        )
        .unwrap();
        assert_eq!(json, json!({"db": {"password": "1234"}}));
    }

    #[test]
    fn an_override_that_is_not_json_is_a_string() {
        let (json, _) = apply_to(
            json!({}),
            &[("DIPPER__IISA__ENDPOINT", "http://iisa:8080")],
            &[],
        )
        .unwrap();
        assert_eq!(json, json!({"iisa": {"endpoint": "http://iisa:8080"}}));
    }

    #[test]
    fn an_override_can_name_a_file_to_read_the_field_from() {
        let (json, _) = apply_to(
            json!({"db": {"url": "postgres://db:5432/dipper"}}),
            &[("DIPPER__DB__PASSWORD_FILE", "/secrets/db-password")],
            &[("/secrets/db-password", "s3cret\n")],
        )
        .unwrap();
        assert_eq!(
            json,
            json!({"db": {"url": "postgres://db:5432/dipper", "password": "s3cret"}})
        );
    }

    #[test]
    fn an_override_with_an_empty_name_part_is_refused() {
        let err = apply_to(json!({}), &[("DIPPER__DB____URL", "x")], &[]).unwrap_err();
        assert!(matches!(err, SourceError::EnvName { .. }), "{err}");
    }

    #[test]
    fn an_override_inside_a_field_that_is_not_a_section_is_refused() {
        let err = apply_to(
            json!({"db": {"url": "postgres://db:5432/dipper"}}),
            &[("DIPPER__DB__URL__HOST", "x")],
            &[],
        )
        .unwrap_err();
        assert_eq!(
            err,
            SourceError::EnvNotASection {
                variable: "DIPPER__DB__URL__HOST".into(),
                section: "db.url".into(),
            }
        );
    }

    #[test]
    fn a_file_field_is_replaced_by_the_file_contents_at_any_depth() {
        let (json, changed) = apply_to(
            json!({
                "signer": {"secret_key_file": "/secrets/signer"},
                "chain_client": {"providers": [{"url_file": "/secrets/rpc"}]},
            }),
            &[],
            &[
                ("/secrets/signer", "0xabc\r\n"),
                ("/secrets/rpc", "https://rpc/key"),
            ],
        )
        .unwrap();
        assert_eq!(
            json,
            json!({
                "signer": {"secret_key": "0xabc"},
                "chain_client": {"providers": [{"url": "https://rpc/key"}]},
            }),
            "1 final line break, CRLF included, is dropped"
        );
        assert!(changed);
    }

    #[test]
    fn only_one_final_line_break_is_dropped() {
        let (json, _) = apply_to(
            json!({"db": {"password_file": "/p"}}),
            &[],
            &[("/p", "a\n\n")],
        )
        .unwrap();
        assert_eq!(json, json!({"db": {"password": "a\n"}}));
    }

    #[test]
    fn a_field_set_both_directly_and_from_a_file_is_refused() {
        let err = apply_to(
            json!({"db": {"password": "x", "password_file": "/p"}}),
            &[],
            &[("/p", "y")],
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "db.password and db.password_file are both set; set only one of them"
        );
    }

    #[test]
    fn a_file_field_that_is_not_a_string_is_refused() {
        let err = apply_to(json!({"db": {"password_file": 1}}), &[], &[]).unwrap_err();
        assert_eq!(
            err,
            SourceError::NotAPath {
                field: "db.password_file".into()
            }
        );
    }

    #[test]
    fn an_unreadable_file_is_reported_without_any_contents() {
        let err = apply_to(json!({"db": {"password_file": "/missing"}}), &[], &[]).unwrap_err();
        let SourceError::Unreadable { field, path, .. } = &err else {
            panic!("expected Unreadable, got {err}");
        };
        assert_eq!(
            (field.as_str(), path.as_str()),
            ("db.password_file", "/missing")
        );
    }

    #[test]
    fn a_field_named_only_by_the_suffix_is_left_for_the_config_to_refuse() {
        // `_file` alone names no field, so it stays and the config refuses it as unknown.
        let (json, changed) = apply_to(json!({"_file": "/p"}), &[], &[]).unwrap();
        assert_eq!(json, json!({"_file": "/p"}));
        assert!(!changed);
    }
}
