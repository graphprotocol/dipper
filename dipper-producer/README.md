# dipper-producer

This crate contains the protobuf definitions and Kafka plumbing for dipper event streaming: the producer side for the agreement lifecycle events the dipper emits, and the consumer side for the subgraph indexing request events Studio emits. The generated Rust bindings are committed to the repository and only need to be regenerated when a `.proto` file changes.

## Protobuf Generation

The build script uses a configuration flag `gen_event_proto` that enables protobuf code generation via `prost-build`. When enabled, the build script compiles the schemas under `proto/` into Rust types under `src/proto/`:

- `proto/indexing-agreement-events.proto`, owned by this repo, generates `src/proto/dipper.subgraph.indexing.agreement.events.v1.rs`.
- `proto/subgraph-indexing-request-events.proto`, vendored from the subgraph-studio repo (`packages/shared/src/helpers/dips/proto/SubgraphIndexingRequest.proto`), generates `src/proto/studio.subgraph.indexing.requests.events.v1.rs`. When Studio's copy changes, re-vendor it here and regenerate.

To regenerate protobuf bindings, run:

```bash
just gen-event-protos
```

Or using the full `cargo` command:

```bash
RUSTFLAGS="--cfg gen_event_proto" cargo check -p dipper-producer
```
