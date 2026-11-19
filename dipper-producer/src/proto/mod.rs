// Generated protobuf types for indexing agreement events
//
// To regenerate, run: just gen-event-protos
include!("dipper.subgraph.indexing.agreement.events.v1.rs");

/// Subgraph indexing request events that Studio produces and the dipper consumes.
/// Schema vendored in `proto/subgraph-indexing-request-events.proto`.
/// To regenerate, run: just gen-event-protos
pub mod studio {
    include!("studio.subgraph.indexing.requests.events.v1.rs");
}
