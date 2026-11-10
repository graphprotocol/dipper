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

/// Why `value` does not fit `schema`, or `None` when it does. `at` names it in the message.
fn misfit(value: &Value, schema: &Value, api: &Value, at: &str) -> Option<String> {
    let schema = resolved(schema, api);
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
    (!fits).then(|| format!("{at} is {value} where IISA expects {}", schema["type"]))
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
    let schema = resolved(schema, api);
    if let Some(alternatives) = schema["anyOf"].as_array() {
        let not_null = alternatives
            .iter()
            .find(|alternative| alternative["type"] != "null");
        return not_null.map_or(Value::Null, |alternative| example(alternative, api));
    }
    match schema["type"].as_str() {
        Some("string") => json!("0x00000000000000000000000000000000000000aa"),
        Some("number") => json!(1.5),
        Some("integer") => json!(1),
        Some("boolean") => json!(true),
        Some("array") => json!([example(&schema["items"], api)]),
        Some("object") => match schema["properties"].as_object() {
            Some(properties) => properties
                .iter()
                .map(|(name, field)| (name.clone(), example(field, api)))
                .collect(),
            None => json!({ "key": example(&schema["additionalProperties"], api) }),
        },
        _ => Value::Null,
    }
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

/// Every type in the client that serde writes or reads.
fn serde_types_in_client() -> BTreeSet<&'static str> {
    let mut lines = client_source().lines();
    let mut names = BTreeSet::new();
    while let Some(line) = lines.next() {
        let derives_serde = line.contains("Serialize") || line.contains("Deserialize");
        if !(line.starts_with("#[derive(") && derives_serde) {
            continue;
        }
        let declaration = lines
            .by_ref()
            .find(|line| line.starts_with("struct ") || line.starts_with("enum "))
            .expect("a derive is followed by its type");
        let name = declaration
            .split_whitespace()
            .nth(1)
            .expect("the type is named");
        names.insert(name.trim_end_matches(['{', '(']));
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

/// The paths the client builds onto IISA's address.
fn paths_the_client_builds() -> BTreeSet<String> {
    client_source()
        .split("format!(\"{}")
        .skip(1)
        .filter_map(|rest| {
            let (path, after) = rest.split_once('"')?;
            after
                .starts_with(", self.endpoint)")
                .then(|| format!("/{path}"))
        })
        .collect()
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

/// Every field IISA requires in a request to /select-indexers is in every request dipper sends,
/// however little dipper knows: a request missing 1 is refused.
#[test]
fn every_field_the_request_schema_requires_is_always_sent() {
    let api = api();
    let required: BTreeSet<String> = component(&api, "SelectionRequest")["required"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|name| name.as_str().map(str::to_string))
        .collect();
    let without_chain = SelectionRequest::new(deployment(), 1, &SelectionContext::default());
    let with_chain = SelectionContext {
        chain_id: Some("arbitrum-one".to_string()),
        ..SelectionContext::default()
    };
    let with_chain = SelectionRequest::new(deployment(), 1, &with_chain)
        .expect("a request with the deployment's chain is sent");

    for request in without_chain.into_iter().chain([with_chain]) {
        let sent = keys(&serde_json::to_value(request).expect("the request is JSON"));
        let missing: Vec<_> = required.difference(&sent).collect();
        assert!(
            missing.is_empty(),
            "IISA requires {missing:?} in a request to /select-indexers, which dipper can leave \
             out. {IF_IISA_CHANGED}"
        );
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

/// A reply with every field IISA describes, each of the type IISA gives it, is one dipper reads.
#[test]
fn every_reply_iisa_describes_is_read_by_dipper() {
    let api = api();
    for reply in replies() {
        let example = example(component(&api, reply.iisa), &api);
        if let Err(err) = (reply.parses)(example.clone()) {
            panic!(
                "{} cannot read {example}, which IISA's {} describes: {err}. {IF_IISA_CHANGED}",
                reply.dipper, reply.iisa
            );
        }
    }
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

    assert_eq!(
        serde_types_in_client(),
        checked,
        "a type the client sends to or reads from IISA is missing from these checks"
    );
}
