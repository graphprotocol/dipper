//! Checks that what the client sends to IISA, and what it reads from IISA's replies, match IISA's
//! own description of its API: the OpenAPI schema in `iisa_openapi.json`, written from an IISA
//! checkout by `scripts/snapshot-iisa-openapi.py`, or the recording `RECORDING` names.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::de::{DeserializeOwned, Visitor};
use serde_json::{Value, json};
use thegraph_core::{DeploymentId, IndexerId};
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path},
};

use super::*;

/// IISA's OpenAPI schema, with the IISA commit it was written from.
const SNAPSHOT: &str = include_str!("iisa_openapi.json");

/// Names a recording to check against instead of the committed one. The scheduled drift check
/// sets it to one written from IISA's main branch that day.
const RECORDING: &str = "IISA_OPENAPI_RECORDING";

/// The client's own source, to find every type it sends or reads and every path it calls.
const CLIENT: &str = include_str!("../http_client.rs");

/// What to do when a check fails because IISA, not dipper, is the side that changed.
const IF_IISA_CHANGED: &str = "If IISA is the side that changed, write the snapshot again with \
                               scripts/snapshot-iisa-openapi.py and bring the client in line.";

fn api() -> Value {
    let snapshot = match std::env::var(RECORDING) {
        Ok(path) => std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("{RECORDING} names {path}, which cannot be read: {err}")),
        Err(_) => SNAPSHOT.to_string(),
    };
    let snapshot: Value = serde_json::from_str(&snapshot).expect("the recording is JSON");
    snapshot["schema"].clone()
}

/// The schema IISA gives `name` in its components.
fn component<'a>(api: &'a Value, name: &str) -> &'a Value {
    let schema = &api["components"]["schemas"][name];
    assert!(
        schema.is_object(),
        "IISA's schema has no {name}. {IF_IISA_CHANGED}"
    );
    schema
}

/// The schema a `$ref` points at, or `schema` itself when it is not a reference.
fn resolved<'a>(schema: &'a Value, api: &'a Value) -> &'a Value {
    match schema["$ref"].as_str() {
        Some(reference) => component(api, reference.rsplit('/').next().unwrap_or(reference)),
        None => schema,
    }
}

/// The keywords of IISA's schema these checks act on.
const CHECKED: [&str; 9] = [
    "type",
    "anyOf",
    "$ref",
    "items",
    "properties",
    "additionalProperties",
    "required",
    "minimum",
    "minLength",
];

/// The keywords that only describe, which no value can break.
const DESCRIPTIVE: [&str; 3] = ["title", "description", "default"];

/// Stops a check that would otherwise pass a value by ignoring part of IISA's schema it does not
/// understand, such as `oneOf`, `pattern` or a list of types.
fn understood(schema: &Value, at: &str) {
    let Some(keywords) = schema.as_object() else {
        panic!("{at}: these checks do not understand IISA's schema {schema}");
    };
    for keyword in keywords.keys() {
        assert!(
            CHECKED.contains(&keyword.as_str()) || DESCRIPTIVE.contains(&keyword.as_str()),
            "{at}: IISA's schema uses `{keyword}`, which these checks do not understand; teach \
             `misfit` and `example` what it means before trusting them: {schema}"
        );
    }
    assert!(
        schema.get("type").is_none_or(Value::is_string),
        "{at}: IISA's schema gives a list of types, which these checks do not understand: {schema}"
    );
}

/// Why `value` does not fit `schema`, or `None` when it does. `at` names it in the message.
fn misfit(value: &Value, schema: &Value, api: &Value, at: &str) -> Option<String> {
    understood(schema, at);
    let schema = resolved(schema, api);
    understood(schema, at);
    if let Some(alternatives) = schema["anyOf"].as_array() {
        let misfits: Vec<_> = alternatives
            .iter()
            .filter_map(|alternative| misfit(value, alternative, api, at))
            .collect();
        return (misfits.len() == alternatives.len()).then(|| misfits.join(", or "));
    }
    let fits = match schema["type"].as_str() {
        Some("array") => return array_misfit(value, schema, api, at),
        Some("object") => return object_misfit(value, schema, api, at),
        Some("string") => value.is_string(),
        Some("number") => value.is_number(),
        Some("integer") => value.is_i64() || value.is_u64(),
        Some("boolean") => value.is_boolean(),
        Some("null") => value.is_null(),
        Some(other) => {
            return Some(format!(
                "{at}: this check does not know IISA's type {other}"
            ));
        }
        None => true,
    };
    if !fits {
        return Some(format!(
            "{at} is {value} where IISA expects {}",
            schema["type"]
        ));
    }
    bound_misfit(value, schema, at)
}

/// Why `value` is outside the least value or length IISA's schema sets, if it is.
fn bound_misfit(value: &Value, schema: &Value, at: &str) -> Option<String> {
    if let (Some(number), Some(minimum)) = (value.as_f64(), schema["minimum"].as_f64())
        && number < minimum
    {
        return Some(format!(
            "{at} is {value}, below IISA's minimum of {minimum}"
        ));
    }
    if let (Some(text), Some(least)) = (value.as_str(), schema["minLength"].as_u64())
        && (text.chars().count() as u64) < least
    {
        return Some(format!(
            "{at} is {value}, shorter than the {least} characters IISA needs"
        ));
    }
    None
}

fn array_misfit(value: &Value, schema: &Value, api: &Value, at: &str) -> Option<String> {
    let Some(items) = value.as_array() else {
        return Some(format!("{at} is {value} where IISA expects an array"));
    };
    items
        .iter()
        .enumerate()
        .find_map(|(i, item)| misfit(item, &schema["items"], api, &format!("{at}[{i}]")))
}

/// A field IISA does not name fails even where its schema would let it through, since IISA is
/// about to refuse fields it does not know.
fn object_misfit(value: &Value, schema: &Value, api: &Value, at: &str) -> Option<String> {
    let Some(fields) = value.as_object() else {
        return Some(format!("{at} is {value} where IISA expects an object"));
    };
    let required = schema["required"].as_array().into_iter().flatten();
    if let Some(name) = required
        .filter_map(Value::as_str)
        .find(|name| !fields.contains_key(*name))
    {
        return Some(format!("{at} leaves out `{name}`, which IISA requires"));
    }
    fields.iter().find_map(|(name, field)| {
        let at = format!("{at}.{name}");
        let named = schema["properties"].get(name);
        let any_key = schema
            .get("additionalProperties")
            .filter(|extra| extra.is_object());
        match named.or(any_key) {
            Some(field_schema) => misfit(field, field_schema, api, &at),
            None => Some(format!(
                "dipper sends {at}, which IISA's schema does not have"
            )),
        }
    })
}

/// A value for every field `schema` describes, as a reply that leaves nothing out.
fn example(schema: &Value, api: &Value) -> Value {
    understood(schema, "an example reply");
    let schema = resolved(schema, api);
    understood(schema, "an example reply");
    if let Some(alternatives) = schema["anyOf"].as_array() {
        let not_null = alternatives
            .iter()
            .find(|alternative| alternative["type"] != "null");
        return not_null.map_or(Value::Null, |alternative| example(alternative, api));
    }
    let least = schema["minimum"].as_f64().unwrap_or(f64::MIN);
    let least_length = schema["minLength"].as_u64().unwrap_or(0);
    match schema["type"].as_str() {
        Some("string") => {
            let mut text = "0x00000000000000000000000000000000000000aa".to_string();
            while (text.chars().count() as u64) < least_length {
                text.push('a');
            }
            json!(text)
        }
        Some("number") => json!(least.max(1.5)),
        Some("integer") => json!(schema["minimum"].as_i64().unwrap_or(1).max(1)),
        Some("boolean") => json!(true),
        Some("null") => Value::Null,
        Some("array") => json!([example(&schema["items"], api)]),
        Some("object") => match schema["properties"].as_object() {
            Some(properties) => properties
                .iter()
                .map(|(name, field)| (name.clone(), example(field, api)))
                .collect(),
            None => json!({ "key": example(&schema["additionalProperties"], api) }),
        },
        None => Value::Null,
        Some(other) => panic!("these checks cannot build a value of IISA's type {other}"),
    }
}

/// Replies of every shape `schema` allows, 1 place at a time: [`example`] first, then 1 for each
/// choice of each `anyOf`, null included, and 1 with each list empty.
fn examples(schema: &Value, api: &Value) -> Vec<Value> {
    let schema = resolved(schema, api);
    let base = example(schema, api);
    let mut shapes = vec![base.clone()];
    if let Some(alternatives) = schema["anyOf"].as_array() {
        for alternative in alternatives {
            shapes.extend(examples(alternative, api));
        }
        return shapes;
    }
    match schema["type"].as_str() {
        Some("array") => {
            shapes.push(json!([]));
            let items = examples(&schema["items"], api).into_iter().skip(1);
            shapes.extend(items.map(|item| json!([item])));
        }
        Some("object") => {
            let fields: Vec<(String, &Value)> = match schema["properties"].as_object() {
                Some(properties) => properties
                    .iter()
                    .map(|(name, field)| (name.clone(), field))
                    .collect(),
                None => vec![("key".to_string(), &schema["additionalProperties"])],
            };
            for (name, field) in fields {
                for shape in examples(field, api).into_iter().skip(1) {
                    let mut variant = base.clone();
                    variant[&name] = shape;
                    shapes.push(variant);
                }
            }
        }
        _ => {}
    }
    shapes
}

fn keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect()
}

/// What serde reads a struct from: asked for the struct, it hands over the field names serde
/// would look for, renames applied, and stops.
struct FieldNames;

#[derive(Debug)]
enum Probe {
    Fields(&'static [&'static str]),
    NotAStruct(String),
}

impl std::fmt::Display for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fields(fields) => write!(f, "the fields {fields:?}"),
            Self::NotAStruct(why) => f.write_str(why),
        }
    }
}

impl std::error::Error for Probe {}

impl serde::de::Error for Probe {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self::NotAStruct(msg.to_string())
    }
}

impl<'de> serde::Deserializer<'de> for FieldNames {
    type Error = Probe;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Probe> {
        Err(Probe::NotAStruct(
            "asked for something other than a struct".to_string(),
        ))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Probe> {
        Err(Probe::Fields(fields))
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map enum identifier
        ignored_any
    }
}

/// The fields dipper reads from a reply of type `T`.
fn fields_of<T: DeserializeOwned>() -> &'static [&'static str] {
    match T::deserialize(FieldNames) {
        Err(Probe::Fields(fields)) => fields,
        Err(other) => panic!(
            "{} is not read as a struct: {other}",
            std::any::type_name::<T>()
        ),
        Ok(_) => panic!("{} read a value from nothing", std::any::type_name::<T>()),
    }
}

fn parses<T: DeserializeOwned>(value: Value) -> Result<(), String> {
    serde_json::from_value::<T>(value)
        .map(drop)
        .map_err(|err| err.to_string())
}

/// A reply type dipper reads, beside the schema IISA describes that reply with.
struct Reply {
    dipper: &'static str,
    iisa: &'static str,
    fields: &'static [&'static str],
    parses: fn(Value) -> Result<(), String>,
}

fn reply<T: DeserializeOwned>(dipper: &'static str, iisa: &'static str) -> Reply {
    Reply {
        dipper,
        iisa,
        fields: fields_of::<T>(),
        parses: parses::<T>,
    }
}

fn replies() -> [Reply; 3] {
    [
        reply::<SelectionResponse>("SelectionResponse", "SelectionResponse"),
        reply::<PricedIndexer>("PricedIndexer", "SelectedIndexer"),
        reply::<DipsIndexersResponse>("DipsIndexersResponse", "DipsIndexersResponse"),
    ]
}

/// The client's source above its tests.
fn client_source() -> &'static str {
    CLIENT
        .split("#[cfg(test)]")
        .next()
        .expect("split yields at least 1 part")
}

/// Every source file of this crate, outside its tests, by its path under `src`. Read from disk so
/// a file added later is scanned too.
fn crate_sources() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut folders = vec![root.clone()];
    let mut sources = Vec::new();
    while let Some(folder) = folders.pop() {
        for entry in std::fs::read_dir(&folder).expect("the source folder can be read") {
            let path = entry.expect("a folder entry").path();
            let name = path.to_string_lossy().into_owned();
            if path.is_dir() {
                folders.push(path);
            } else if name.ends_with(".rs") && !name.ends_with("_tests.rs") {
                let text = std::fs::read_to_string(&path).expect("a source file can be read");
                let outside_tests = text.split("#[cfg(test)]\nmod tests {").next();
                let relative = path.strip_prefix(&root).unwrap_or(&path);
                let relative = relative.to_string_lossy().into_owned();
                sources.push((relative, outside_tests.unwrap_or_default().to_string()));
            }
        }
    }
    sources
}

/// Where `position` falls in `text`, as `file:line`.
fn place(file: &str, text: &str, position: usize) -> String {
    format!("{file}:{}", text[..position].matches('\n').count() + 1)
}

/// The positions of `word` in `text` where it stands as a whole name, not part of a longer one.
fn word_positions<'a>(text: &'a str, word: &'a str) -> impl Iterator<Item = usize> + 'a {
    let part_of_a_name = |c: char| c.is_alphanumeric() || c == '_';
    text.match_indices(word)
        .map(|(at, _)| at)
        .filter(move |&at| {
            let before = text[..at].chars().next_back();
            let after = text[at + word.len()..].chars().next();
            !before.is_some_and(part_of_a_name) && !after.is_some_and(part_of_a_name)
        })
}

/// The name of the struct or enum that `text` declares after any attributes and comments, or
/// `None` when what follows is not one.
fn item_declared(text: &str) -> Option<&str> {
    let mut rest = text.trim_start();
    loop {
        if rest.starts_with("//") {
            rest = rest
                .split_once('\n')
                .map_or("", |(_, after)| after)
                .trim_start();
        } else if rest.starts_with("#[") {
            let mut depth = 0;
            let end = rest.char_indices().find_map(|(at, c)| {
                depth += match c {
                    '[' => 1,
                    ']' => -1,
                    _ => 0,
                };
                (c == ']' && depth == 0).then_some(at)
            })?;
            rest = rest[end + 1..].trim_start();
        } else {
            break;
        }
    }
    if let Some(after) = rest.strip_prefix("pub") {
        rest = after.trim_start();
        if rest.starts_with('(') {
            rest = rest[rest.find(')')? + 1..].trim_start();
        }
    }
    let rest = rest
        .strip_prefix("struct ")
        .or_else(|| rest.strip_prefix("enum "))?
        .trim_start();
    let end = rest
        .find(|c: char| !(c.is_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    (end > 0).then(|| &rest[..end])
}

/// Every type in this crate that serde writes or reads. Any mention of serde's derives it cannot
/// account for, such as a derive behind `cfg_attr` or a hand-written impl, fails the scan rather
/// than leave a type out.
fn serde_types_in_crate() -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    for (file, text) in crate_sources() {
        let mut derives = Vec::new();
        for (start, _) in text.match_indices("#[derive(") {
            let end = start + text[start..].find(")]").expect("a derive attribute ends") + 2;
            let attribute = &text[start..end];
            if !attribute.contains("Serialize") && !attribute.contains("Deserialize") {
                continue;
            }
            let name = item_declared(&text[end..]).unwrap_or_else(|| {
                panic!(
                    "{}: cannot tell which type this serde derive is on",
                    place(&file, &text, start)
                )
            });
            names.insert(name.to_string());
            derives.push(start..end);
        }
        for word in ["Serialize", "Deserialize"] {
            for at in word_positions(&text, word) {
                let line_start = text[..at].rfind('\n').map_or(0, |i| i + 1);
                let line = text[line_start..].lines().next().unwrap_or_default().trim();
                let statement = text[..at].rsplit(';').next().unwrap_or_default();
                let in_use = statement
                    .lines()
                    .map(str::trim)
                    .find(|line| !line.is_empty() && !line.starts_with("//"))
                    .is_some_and(|first| {
                        first.starts_with("use ") || first.starts_with("pub use ")
                    });
                let accounted = derives.iter().any(|derive| derive.contains(&at))
                    || in_use
                    || line.starts_with("//")
                    || text[at + word.len()..].starts_with("<'de>");
                assert!(
                    accounted,
                    "{}: cannot tell what this use of `{word}` is, so a type serde reads or writes \
                     could be missed: {line}",
                    place(&file, &text, at)
                );
            }
        }
    }
    names
}

/// The fields `SelectionRequest` declares.
fn request_fields_declared() -> BTreeSet<String> {
    let (_, body) = client_source()
        .split_once("struct SelectionRequest {")
        .expect("the client declares SelectionRequest");
    let (body, _) = body.split_once("\n}").expect("the declaration ends");
    body.lines()
        .filter_map(|line| line.strip_prefix("    "))
        .filter(|line| !line.starts_with(['/', '#', ' ']))
        .filter_map(|line| line.split_once(':'))
        .map(|(name, _)| name.to_string())
        .collect()
}

/// The paths the client builds onto IISA's address. Any use of the address other than
/// `format!("{}<path>", self.endpoint)`, however it is laid out, fails the scan rather than leave
/// a call out.
fn paths_the_client_builds() -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for (file, text) in crate_sources() {
        for (at, use_of_address) in text.match_indices("self.endpoint") {
            let end = at + use_of_address.len();
            let start = text[..at].rfind("format!(").unwrap_or(at);
            let mut written: String = text[start..end].split_whitespace().collect();
            written.extend(text[end..].trim_start().chars().next());
            let path = written
                .strip_prefix("format!(\"{}")
                .and_then(|rest| rest.strip_suffix("\",self.endpoint)"))
                .filter(|path| !path.contains(['"', '{', '}']));
            let path = path.unwrap_or_else(|| {
                panic!(
                    "{}: cannot read which path this use of IISA's address calls: {written}",
                    place(&file, &text, at)
                )
            });
            paths.insert(format!("/{path}"));
        }
    }
    paths
}

fn deployment() -> DeploymentId {
    "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"
        .parse()
        .expect("a valid deployment ID")
}

fn indexer(last_byte: u8) -> IndexerId {
    let mut address = [0u8; 20];
    address[19] = last_byte;
    IndexerId::from(thegraph_core::alloy::primitives::Address::from(address))
}

/// A context that fills in every field, so the request carries every field the client can send.
fn full_context() -> SelectionContext {
    SelectionContext {
        existing_indexers: vec![indexer(1)],
        pending_agreements: HashMap::from([(deployment(), vec![indexer(2)])]),
        indexer_denylist: vec![indexer(3)],
        declined_indexers: HashMap::from([(deployment(), vec![indexer(4)])]),
        chain_id: Some("arbitrum-one".to_string()),
        max_grt_per_30_days: Some(4500.0),
        max_grt_per_billion_entities_per_30_days: Some(2000.0),
        optimistic_dips_fees: HashMap::from([(indexer(5), 12.5)]),
    }
}

/// Every request the client makes, each with all it can carry, recorded by a stand-in for IISA
/// that answers with replies built from IISA's schema.
async fn calls_to_iisa() -> Vec<Request> {
    let api = api();
    let server = MockServer::start().await;
    for (verb, route, schema) in [
        ("POST", "/select-indexers", "SelectionResponse"),
        ("GET", "/dips-indexers", "DipsIndexersResponse"),
        ("GET", "/health", "HealthResponse"),
    ] {
        Mock::given(method(verb))
            .and(path(route))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(example(component(&api, schema), &api)),
            )
            .mount(&server)
            .await;
    }
    let config = HttpClientConfig {
        push_token: Some("token".to_string()),
        max_retries: 0,
        ..HttpClientConfig::default()
    };
    let client = HttpIisaClient::with_config(server.uri(), config);
    let ceilings = PriceCeilings {
        max_grt_per_30_days: Some(4500.0),
        max_grt_per_billion_entities_per_30_days: Some(2000.0),
    };

    assert!(client.health_check().await.expect("health check"));
    client
        .select_indexers(deployment(), 3, &full_context())
        .await
        .expect("selection");
    client
        .dips_accepting_indexers("arbitrum-one", ceilings)
        .await
        .expect("DIPs-accepting indexers");
    server
        .received_requests()
        .await
        .expect("requests are recorded")
}

/// A query value as JSON, so a number can be checked as one: text that is not JSON is a string.
fn query_value(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
}

/// The query parameters and headers `operation` takes, by where they go and their name.
fn parameters(operation: &Value) -> BTreeMap<(String, String), Value> {
    operation["parameters"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|parameter| {
            let place = parameter["in"].as_str().unwrap_or_default().to_string();
            let name = parameter["name"].as_str().unwrap_or_default().to_string();
            ((place, name), parameter.clone())
        })
        .collect()
}

/// Every path and method dipper calls is one IISA serves, taking each query parameter dipper
/// sends with a value of the right type, and the bearer token where dipper sends one.
#[tokio::test]
async fn every_call_dipper_makes_is_one_iisa_serves() {
    let api = api();
    for request in calls_to_iisa().await {
        let route = request.url.path().to_string();
        let verb = request.method.as_str().to_lowercase();
        let operation = &api["paths"][&route][&verb];
        assert!(
            operation.is_object(),
            "dipper calls {} {route}, which IISA does not serve. {IF_IISA_CHANGED}",
            verb.to_uppercase()
        );
        let parameters = parameters(operation);
        let sent: BTreeMap<String, String> = request.url.query_pairs().into_owned().collect();
        for (name, text) in &sent {
            let parameter = parameters
                .get(&("query".to_string(), name.clone()))
                .unwrap_or_else(|| {
                    panic!("dipper sends ?{name} to {route}, which IISA does not take. {IF_IISA_CHANGED}")
                });
            let at = format!("?{name} on {route}");
            if let Some(misfit) = misfit(&query_value(text), &parameter["schema"], &api, &at) {
                panic!("{misfit}. {IF_IISA_CHANGED}");
            }
        }
        for ((place, name), parameter) in &parameters {
            let required = place == "query" && parameter["required"] == true;
            assert!(
                !required || sent.contains_key(name),
                "IISA requires ?{name} on {route}, which dipper does not send. {IF_IISA_CHANGED}"
            );
        }
        let authorization = ("header".to_string(), "authorization".to_string());
        assert!(
            !request.headers.contains_key("authorization")
                || parameters.contains_key(&authorization),
            "dipper sends a bearer token to {route}, which IISA does not read. {IF_IISA_CHANGED}"
        );
    }
}

/// Wherever IISA asks for the bearer token, by reading the authorization header or by answering
/// 401, a client given a token sends it; without it every such call would be refused.
#[tokio::test]
async fn dipper_sends_its_token_wherever_iisa_asks_for_one() {
    let api = api();
    for request in calls_to_iisa().await {
        let route = request.url.path().to_string();
        let operation = &api["paths"][&route][request.method.as_str().to_lowercase()];
        let authorization = ("header".to_string(), "authorization".to_string());
        let asks = parameters(operation).contains_key(&authorization)
            || operation["responses"].get("401").is_some();
        let sent = request
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok());

        assert!(
            !asks || sent == Some("Bearer token"),
            "IISA asks for the bearer token on {route}, which dipper does not send ({sent:?}). \
             {IF_IISA_CHANGED}"
        );
    }
}

/// The check above only covers the calls it makes, so every path the client builds is one of them.
#[tokio::test]
async fn every_path_the_client_calls_is_checked() {
    let called: BTreeSet<String> = calls_to_iisa()
        .await
        .iter()
        .map(|request| request.url.path().to_string())
        .collect();

    assert_eq!(
        paths_the_client_builds(),
        called,
        "a path the client builds onto IISA's address is missing from `calls_to_iisa`"
    );
}

/// Every field dipper sends to /select-indexers is one IISA's schema names, with a value of the
/// type IISA expects. This matters most, since IISA is about to refuse fields it does not know.
#[tokio::test]
async fn every_field_dipper_sends_is_in_the_request_schema_with_a_compatible_type() {
    let api = api();
    let calls = calls_to_iisa().await;
    let selection = calls
        .iter()
        .find(|request| request.url.path() == "/select-indexers")
        .expect("the client asks IISA to select indexers");
    let body: Value = serde_json::from_slice(&selection.body).expect("a JSON body");
    let operation = &api["paths"]["/select-indexers"]["post"];
    let schema = &operation["requestBody"]["content"]["application/json"]["schema"];

    if let Some(misfit) = misfit(&body, schema, &api, "SelectionRequest") {
        panic!("{misfit}. {IF_IISA_CHANGED}");
    }
    assert_eq!(
        keys(&body),
        request_fields_declared(),
        "`full_context` leaves a field of SelectionRequest unsent, so the check above misses it"
    );
}

/// However little dipper knows, every request it sends to /select-indexers has each field IISA
/// requires, with a value IISA takes: a request IISA would refuse is not sent at all.
#[test]
fn every_request_dipper_can_send_has_what_the_schema_requires() {
    let api = api();
    let operation = &api["paths"]["/select-indexers"]["post"];
    let schema = &operation["requestBody"]["content"]["application/json"]["schema"];
    let with_chain = |chain_id: Option<&str>| SelectionContext {
        chain_id: chain_id.map(str::to_string),
        ..SelectionContext::default()
    };
    let contexts = [
        with_chain(None),
        with_chain(Some("")),
        with_chain(Some("arbitrum-one")),
    ];
    let requests: Vec<_> = contexts
        .iter()
        .filter_map(|context| SelectionRequest::new(deployment(), 1, context).ok())
        .collect();
    assert!(
        !requests.is_empty(),
        "a request with the deployment's chain is sent"
    );

    for request in requests {
        let body = serde_json::to_value(request).expect("the request is JSON");
        if let Some(misfit) = misfit(&body, schema, &api, "SelectionRequest") {
            panic!("{misfit}, in a request dipper can send. {IF_IISA_CHANGED}");
        }
    }
}

/// Every field dipper reads from a reply is one IISA's schema names for that reply, so a field
/// IISA renames or drops cannot be read as missing without a word.
#[test]
fn every_field_dipper_reads_from_a_reply_is_in_the_reply_schema() {
    let api = api();
    let drifted: Vec<String> = replies()
        .iter()
        .filter_map(|reply| {
            let named = keys(&component(&api, reply.iisa)["properties"]);
            let unknown: Vec<_> = reply
                .fields
                .iter()
                .filter(|field| !named.contains(**field))
                .collect();
            (!unknown.is_empty()).then(|| {
                format!(
                    "{} reads {unknown:?}, which IISA's {} does not have (it has {named:?})",
                    reply.dipper, reply.iisa
                )
            })
        })
        .collect();

    assert!(
        drifted.is_empty(),
        "{}\n{IF_IISA_CHANGED}",
        drifted.join("\n")
    );
}

/// A reply of every shape IISA's schema allows is one dipper reads: every field present, each
/// choice of each `anyOf` (null included) and each list empty, at every place they appear.
#[test]
fn every_shape_of_reply_iisa_describes_is_read_by_dipper() {
    let api = api();
    let mut unread = Vec::new();
    for reply in replies() {
        for shape in examples(component(&api, reply.iisa), &api) {
            if let Err(err) = (reply.parses)(shape.clone()) {
                unread.push(format!(
                    "{} cannot read {shape}, which IISA's {} allows: {err}",
                    reply.dipper, reply.iisa
                ));
            }
        }
    }

    assert!(
        unread.is_empty(),
        "{}\n{IF_IISA_CHANGED}",
        unread.join("\n")
    );
}

/// Every reply field dipper cannot read a reply without is one IISA always sends. Dipper fills in
/// a default for the others.
#[test]
fn every_reply_field_dipper_cannot_do_without_is_one_iisa_always_sends() {
    let api = api();
    for reply in replies() {
        let schema = component(&api, reply.iisa);
        let always_sent: BTreeSet<&str> = schema["required"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        for field in reply.fields {
            let mut without = example(schema, &api);
            without.as_object_mut().expect("an object").remove(*field);
            assert!(
                (reply.parses)(without).is_ok() || always_sent.contains(field),
                "{} cannot be read without `{field}`, which IISA's {} may leave out. \
                 {IF_IISA_CHANGED}",
                reply.dipper,
                reply.iisa
            );
        }
    }
}

/// An indexer entry as IISA describes it is read with its prices, not as a bare ID without them.
#[test]
fn a_selected_indexer_iisa_describes_is_read_with_its_prices() {
    let api = api();
    let example = example(component(&api, "SelectedIndexer"), &api);

    match serde_json::from_value::<IndexerEntry>(example.clone()).expect("an indexer entry") {
        IndexerEntry::WithPricing(priced) => {
            let base = priced.min_grt_per_30_days;
            let per_entity = priced.min_grt_per_billion_entities_per_30_days;
            assert!(base.is_some(), "the base price in {example} is not read");
            assert!(
                per_entity.is_some(),
                "the per-entity price in {example} is not read"
            );
        }
        IndexerEntry::LegacyId(id) => panic!("{example} was read as the bare ID {id}"),
    }
}

/// The checks above only cover the types they list, so a new one has to be added to them.
#[test]
fn no_request_or_reply_type_in_the_client_is_left_unchecked() {
    let mut checked: BTreeSet<&str> = replies().iter().map(|reply| reply.dipper).collect();
    checked.insert("SelectionRequest");
    // Only a choice between a priced entry and a bare ID, checked through SelectionResponse and
    // the test of a selected indexer's prices.
    checked.insert("IndexerEntry");

    let checked: BTreeSet<String> = checked.into_iter().map(str::to_string).collect();
    assert_eq!(
        serde_types_in_crate(),
        checked,
        "a type the client sends to or reads from IISA is missing from these checks"
    );
}
