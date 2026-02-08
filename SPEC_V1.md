# SparkEx V1 Specification (Option A: Native Spark Connect Client)

## 1. Purpose

Build an Elixir library (`spark_ex`) that talks to Spark via Spark Connect gRPC, with a PySpark-like DataFrame API adapted to idiomatic Elixir and first-class Livebook support.

This spec targets Option A from `HLD.md`: native Spark Connect client (no JVM sidecar).

## 2. Design Principles

- Keep the protocol boundary strict: Elixir IR -> Spark Connect protobuf -> gRPC.
- Keep execution lazy: transforms build plans, actions execute.
- Prefer explicit, functional Elixir API first; DSL macros are optional sugar.
- Make Livebook safe by default: bounded previews and materialization limits.
- Validate behavior against real Spark Connect endpoints from day one.

## 3. Scope

### 3.1 V1 Goals

- Native Spark Connect protocol client in Elixir using generated protobuf + gRPC stubs.
- Core lazy DataFrame API (read, transform, action).
- Interoperability with:
  - gRPC client stack
  - protobuf generated modules
  - `Explorer.DataFrame` for local tabular results
  - `Kino` for Livebook rendering
- Real-cluster testable milestones with incremental delivery.
- API shape guided by PySpark Connect, but idiomatic for Elixir.

### 3.2 Non-goals for V1

- Full PySpark parity.
- RDD APIs / SparkContext-like low-level APIs.
- Arbitrary Elixir code-shipping UDFs.
- Structured Streaming parity.
- Catalog API parity (deferred to 1.1).

## 4. External Protocol Constraints

Based on Spark Connect docs and source in `/Users/lukaszsamson/claude_fun/spark`:

- Connection URI format: `sc://host:port/;key=value`.
- Path component must be empty.
- Default port: `15002`.
- Core RPCs for V1: `ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt`, `ReleaseSession`, `CloneSession`, `FetchErrorDetails`.
- Session scoping: `session_id` + `user_context.user_id`.
- Request idempotency/session continuity: track `server_side_session_id` from responses and send back via `client_observed_server_side_session_id`.
- Results: streamed Arrow batches via `ExecutePlanResponse.arrow_batch`.
- SQL/DataFrame operations: unresolved logical plans (`Relation`, `Expression`).

## 5. High-level Architecture

### 5.1 Public Modules

- `SparkEx`
  - Entrypoints (`connect/1`, convenience wrappers).
- `SparkEx.Session`
  - Session lifecycle, config, interrupt, clone.
- `SparkEx.DataFrame`
  - Immutable lazy logical plan handle.
- `SparkEx.Column`
  - Expression wrapper.
- `SparkEx.Functions`
  - Expression constructors (`col/1`, `lit/1`, `expr/1`, aggregates).
- `SparkEx.Reader`
  - Read/data source builder APIs.
- `SparkEx.Livebook`
  - Notebook-focused helpers.

### 5.2 Internal Modules

- `SparkEx.Connect.Channel`
  - URI parsing, TLS/token config, metadata, channel creation.
- `SparkEx.Connect.Client`
  - RPC orchestration, retries, deadlines, per-request metadata.
- `SparkEx.Connect.PlanEncoder`
  - Elixir IR -> Spark Connect protobuf (`Plan`, `Relation`, `Expression`).
- `SparkEx.Connect.ResultDecoder`
  - Execute stream decode, Arrow chunk reassembly, data materialization.
- `SparkEx.Connect.Errors`
  - gRPC + Spark structured error mapping, `FetchErrorDetails` integration.
- `SparkEx.Livebook.Renderer`
  - `Kino.Render` integration for DataFrame preview/explain/schema.

### 5.3 Session Process Model

`SparkEx.Session` will be implemented as a `GenServer` to safely manage mutable protocol state:

- gRPC channel handle
- `session_id`
- `server_side_session_id`
- connection defaults (timeouts, size limits)
- monotonic `plan_id` counter

## 6. API Design (Idiomatic Elixir)

### 6.1 Connection and Session

```elixir
{:ok, session} =
  SparkEx.connect(
    url: "sc://spark-host:15002/;use_ssl=true;token=...",
    user_id: "lukasz",
    client_type: "spark_ex/0.1"
  )
```

Session APIs (V1):

- `SparkEx.Session.stop/1`
- `SparkEx.Session.clone/2`
- `SparkEx.Session.interrupt_all/1`
- `SparkEx.Session.interrupt_operation/2`
- `SparkEx.Session.config_get/2`
- `SparkEx.Session.config_set/3`

### 6.2 Read APIs

Use valid Elixir module-function style:

- `SparkEx.Reader.table(session, table_name, opts \\ [])`
- `SparkEx.Reader.parquet(session, paths, opts \\ [])`
- `SparkEx.Reader.csv(session, paths, opts \\ [])`
- `SparkEx.Reader.json(session, paths, opts \\ [])`
- `SparkEx.sql(session, statement, args \\ [])`
- `SparkEx.range(session, start_or_end, end \\ nil, step \\ 1, opts \\ [])`

Optional convenience wrappers may be exposed from `SparkEx`.

### 6.3 DataFrame Transforms (V1)

- `SparkEx.DataFrame.select/2`
- `SparkEx.DataFrame.select_expr/2`
- `SparkEx.DataFrame.filter/2`
- `SparkEx.DataFrame.with_column/3`
- `SparkEx.DataFrame.drop/2`
- `SparkEx.DataFrame.join/4`
- `SparkEx.DataFrame.group_by/2`
- `SparkEx.DataFrame.agg/2`
- `SparkEx.DataFrame.order_by/2`
- `SparkEx.DataFrame.limit/2`
- `SparkEx.DataFrame.distinct/1`
- `SparkEx.DataFrame.union/2`

### 6.4 DataFrame Actions

- `SparkEx.DataFrame.collect/2`
- `SparkEx.DataFrame.take/2`
- `SparkEx.DataFrame.count/1`
- `SparkEx.DataFrame.schema/1`
- `SparkEx.DataFrame.explain/2`
- `SparkEx.DataFrame.show/2`
- `SparkEx.DataFrame.to_explorer/2`

`explain/2` supports Spark Connect explain modes:

- `:simple`
- `:extended`
- `:codegen`
- `:cost`
- `:formatted`

### 6.5 SQL Args Encoding

`SparkEx.sql/3` supports both:

- positional args -> `Relation.SQL.pos_arguments`
- named args (map) -> `Relation.SQL.named_arguments`

Do not use deprecated literal-only SQL arg fields.

### 6.6 Expression API

`SparkEx.Functions` and `SparkEx.Column` provide the canonical V1 expression API:

- constructors: `col/1`, `lit/1`, `expr/1`
- comparisons/boolean: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `and`, `or`, `not`
- null/string basics: `is_null/1`, `is_not_null/1`, `contains/2`, `starts_with/2`
- aggregates: `count/1`, `sum/1`, `avg/1`, `min/1`, `max/1`, `count_distinct/1`
- cast/sort helpers: `cast/2`, `asc/1`, `desc/1`

### 6.7 DSL Scope

V1 source of truth is the functional API above.

Optional macro DSL (`SparkEx.Query`) is limited in V1:

- minimal `where`/`select` sugar only
- no attempt to mirror full Ecto macro semantics yet
- explicitly optional and may move to 1.1 if it slows delivery

## 7. IR and Proto Mapping

### 7.1 Relation IR (V1)

IR nodes map to Spark Connect relations:

- `Read.NamedTable`, `Read.DataSource`
- `SQL`
- `Range`
- `Project`
- `Filter`
- `WithColumns`
- `Drop`
- `Join`
- `Aggregate`
- `Sort`
- `Limit`
- `Deduplicate`
- `SetOperation` (union)
- `ShowString`

### 7.2 Expression IR (V1)

- `Literal` (null, bool, int, long, double, decimal, string, date, timestamp)
- `UnresolvedAttribute`
- `UnresolvedFunction`
- `Alias`
- `SortOrder`
- `ExpressionString`
- `Cast`
- `UnresolvedStar`

### 7.3 Plan IDs and References

- Every relation emitted by `PlanEncoder` gets `RelationCommon.plan_id`.
- `WithRelations` support is included for query shapes that require relation references (subquery/reference cases).
- For simple tree-only plans, encoder may emit plain rooted relation.

## 8. ExecutePlan Response Handling

`SparkEx.Connect.ResultDecoder` handles full stream semantics:

- Arrow batches (`arrow_batch.data`)
- chunked Arrow batches (`chunk_index`, `num_chunks_in_batch`) with strict reassembly
- optional `schema` field usage
- `execution_progress` events (telemetry + optional Livebook display)
- `result_complete` handling for reattachable flows

`ExecutePlan` requests set result chunking options with `allow_arrow_batch_chunking=true` by default, so oversized Arrow batches can be safely split and reassembled.

Result decoder invariants:

- validate chunk ordering
- validate row count consistency
- fail with structured decode errors on malformed stream state

## 9. Arrow to Explorer Strategy

### 9.1 Chosen Approach

- Primary path: in-memory Arrow IPC stream decode using Explorer binary loaders.
- Concretely, decode each reassembled Spark Arrow payload with:
  - `Explorer.DataFrame.load_ipc_stream!/2` for IPC stream bytes
  - (fallback) `Explorer.DataFrame.load_ipc!/2` for IPC file bytes when applicable
- Batch assembly:
  - reassemble gRPC chunks into a full `arrow_batch.data` binary
  - decode each Arrow batch to `Explorer.DataFrame`
  - combine with `Explorer.DataFrame.concat_rows/1`
- Last-resort fallback (debug only): temp-file path with `Explorer.DataFrame.from_ipc_stream!/2`.

### 9.2 Guardrails

All local materialization paths are bounded by defaults:

- `preview_rows_default`: 100
- `collect_rows_max`: 10_000
- `collect_bytes_max`: 64 MB
- `max_batches`: configurable cap

If exceeded, return `SparkEx.Error.LimitExceeded` with remediation guidance.

Materialization policy:

- `to_explorer/2` always pushes `LIMIT` into the remote Spark plan unless `unsafe: true`.
- renderer paths (`Kino.Render`, `SparkEx.Livebook.preview/2`) always operate in safe mode.
- if limits are exceeded mid-stream, abort decode and return partial-free error (no silently truncated dataframe).

### 9.3 Type Mapping

Define explicit Spark type -> Explorer dtype mapping for primitives.
For unsupported nested/complex types in V1, use deterministic fallback (JSON string).

Minimum mapping contract:

- Spark integral types -> Explorer signed integer dtypes
- Spark float/double -> Explorer float dtypes
- Spark decimal -> Explorer decimal or string fallback (explicitly documented)
- Spark date/timestamp/timestamp_ntz -> Explorer date/datetime dtypes where supported
- Spark binary/map/struct/array (unsupported in V1 display path) -> JSON string

### 9.4 Explorer Backend and Lazy Policy

- Default to eager `Explorer.DataFrame` for notebook previews.
- Do not rely on lazy IPC-stream reads for performance assumptions in V1.
- Optional `backend:` override is supported but not required for core SparkEx behavior.

## 10. Transport, Auth, Reliability, Error Handling

### 10.1 gRPC Client Stack

Milestone 0 includes a client-library validation spike:

- verify server-streaming robustness (`ExecutePlan`)
- verify metadata + token support
- verify TLS + deadlines + cancellation

Primary path is whichever stack passes these constraints in the spike.

### 10.2 Auth and Metadata

- Connection-string `token` is used as bearer auth in gRPC credentials/metadata per chosen client stack.
- `client_type` is sent on all requests (e.g. `elixir/<otp> spark_ex/<version>`).
- support additional custom metadata headers via options.

### 10.3 Retry Policy

- Retry transient transport errors only (e.g. `UNAVAILABLE`, `DEADLINE_EXCEEDED`).
- No retry for semantic/query errors.
- Exponential backoff with jitter.
- optional server-retry-delay awareness if available.

### 10.4 Reattach and Release (V1.1)

- Reattachable execute is deferred to 1.1 unless needed to stabilize V1 against network instability.
- When enabled: use `operation_id`, `ReattachExecute`, `ReleaseExecute`.

### 10.5 Error Enrichment

`SparkEx.Connect.Errors` flow:

1. Parse gRPC error status/metadata.
2. If `error_id` is present, call `FetchErrorDetails`.
3. Map into `SparkEx.Error.Remote` fields:
   - `error_class`
   - `sql_state`
   - `message_parameters`
   - `query_contexts`

### 10.6 Plan Compression

- V1 default: plan compression disabled.
- V1.1 target: optional zstd compressed plans via `Plan.compressed_operation`.
- Compression must be feature-gated and enabled only after compatibility tests against Spark `v4.1.1`.

## 11. Livebook / Kino Integration

### 11.1 `Kino.Render` for `SparkEx.DataFrame`

Render tabs:

- Schema (`AnalyzePlan.Schema`)
- Preview (`Kino.DataTable.new(explorer_df, num_rows: N)`)
- Explain (`explain(:extended)` default)
- Raw (`Kino.Inspect.new(df)`) for debugging internals

Implementation note:

- `Explorer.DataFrame` already implements `Table.Reader`, so it is directly compatible with `Kino.DataTable`.
- `SparkEx.DataFrame` does not implement `Table.Reader`; it needs a dedicated `defimpl Kino.Render`.

### 11.2 Helpers

- `SparkEx.Livebook.preview/2`
- `SparkEx.Livebook.explain/2`
- `SparkEx.Livebook.sample/2`
- `SparkEx.Livebook.schema/1`

Helper behavior contract:

- helpers return Kino terms, not raw strings/lists
- helpers do not call `Kino.render/1` internally (caller controls rendering)
- preview helper uses `Kino.DataTable` options passthrough:
  - `:name`
  - `:num_rows`
  - `:sorting_enabled`
  - optional `:formatter`

### 11.3 Safety

Livebook rendering always executes bounded preview queries; never unbounded collect by default.

### 11.4 Progress and Long-running Queries

- `execution_progress` events from Spark are surfaced via telemetry.
- optional notebook helper (`SparkEx.Livebook.run_with_progress/2`) may render a `Kino.Frame` and update progress text/metrics while fetching batches.
- progress UI is best-effort and must not block result decoding.

## 12. Observability

Define telemetry events:

- `[:spark_ex, :rpc, :start]`
- `[:spark_ex, :rpc, :stop]`
- `[:spark_ex, :rpc, :exception]`
- `[:spark_ex, :result, :batch]`
- `[:spark_ex, :retry, :attempt]`

Metadata includes RPC name, duration, row_count, bytes, attempt index, and status.

## 13. Dependencies and Proto Generation

### 13.1 Dependencies (initial)

- protobuf generation/runtime libraries
- chosen gRPC client stack from Milestone 0 spike
- `:nimble_options`
- `:telemetry`
- `:jason`
- optional (feature-gated): `:explorer`, `:kino`

### 13.2 Proto Generation

- Pin proto source to Spark `v4.1.1`.
- Vendor all required Spark Connect protos and required Google protobuf dependencies.
- Generate modules into `lib/spark_ex/proto/**`.
- Provide `mix spark_ex.gen_proto` task for repeatable regeneration.

## 14. Repository Layout

```text
lib/
  spark_ex.ex
  spark_ex/session.ex
  spark_ex/data_frame.ex
  spark_ex/column.ex
  spark_ex/functions.ex
  spark_ex/reader.ex
  spark_ex/livebook.ex
  spark_ex/livebook/renderer.ex
  spark_ex/connect/channel.ex
  spark_ex/connect/client.ex
  spark_ex/connect/plan_encoder.ex
  spark_ex/connect/result_decoder.ex
  spark_ex/connect/errors.ex
  spark_ex/proto/** (generated)

priv/proto/spark/connect/*.proto

test/
  unit/
  contract/
  integration/
  livebook/
  fixtures/proto/
```

## 15. Testing Strategy

### 15.1 Unit Tests

- URI parsing and connection options
- plan/expression encoding
- literal/type conversion
- error mapping behavior

### 15.2 Contract Tests

- golden proto snapshots for key operations
- proto serialize/parse roundtrip checks

### 15.3 Integration Tests (real Spark)

- `AnalyzePlan.spark_version`
- SQL query/collect/count flows
- read/filter/project/aggregate/join workflows
- interrupt behavior
- auth failure behavior
- Arrow chunking reassembly path

### 15.4 PySpark Parity Checks (selected subset)

Run equivalent PySpark Connect and SparkEx queries against the same cluster and compare:

- row counts
- key values
- schema shape

### 15.5 Integration Test Environment

- Primary mode: run tests against externally provided `SPARK_REMOTE`.
- Local CI/dev fallback: Dockerized Spark Connect server (`apache/spark:4.1.1`) exposing port `15002`.
- Integration tests are tagged and skipped automatically when no Spark Connect endpoint is available.

### 15.6 Livebook/Kino/Explorer Tests

- `Kino.Render` implementation tests:
  - returns `:tabs` output shape with expected labels
  - preview tab uses `Kino.DataTable`
- Explorer decode tests:
  - `load_ipc_stream!/2` on assembled Arrow payloads
  - `concat_rows/1` preserves row order across multiple Spark batches
- limit safety tests:
  - preview refuses unbounded paths
  - oversized payload triggers `SparkEx.Error.LimitExceeded`

## 16. Incremental Implementation Plan

### Milestone 0 - Foundations

Deliverables:

- proto vendor + generation pipeline
- gRPC client stack spike and decision
- session process skeleton

Acceptance:

- connect and fetch Spark version via `AnalyzePlan.spark_version`

### Milestone 1 - Minimal Query Path

Deliverables:

- `SparkEx.connect/1`
- `SparkEx.sql/3` with named/positional args
- `collect/2`, `count/1`
- `Config` get/set subset
- server-side session ID tracking
- structured error enrichment (`FetchErrorDetails`)

Acceptance:

- `SELECT 1` and range query equivalent succeed end-to-end
- invalid SQL returns enriched structured error

### Milestone 2 - DataFrame Core

Deliverables:

- reader APIs (table/parquet/csv/json)
- core transforms (`select/filter/order_by/limit/with_column/drop`)
- `schema/1`, `explain/2`, `show/2`
- plan ID assignment + reference support

Acceptance:

- read parquet and run projection/filter/order/limit
- all explain modes return non-empty output

### Milestone 3 - Joins, Aggregates, Retry

Deliverables:

- `join`, `group_by`, `agg`, `distinct`, `union`
- aggregate and predicate functions
- transient retry/backoff

Acceptance:

- join + aggregate query matches expected result set
- retry succeeds on simulated transient error

### Milestone 4 - Explorer + Kino + Chunking

Deliverables:

- robust Explorer decode path using `load_ipc_stream!/2`
- chunking reassembly + dataframe row concatenation
- `Kino.Render` tabs (`Schema`, `Preview`, `Explain`, `Raw`)
- Livebook helpers with `Kino.DataTable` option passthrough
- optional progress frame integration

Acceptance:

- Livebook preview works on real cluster data without OOM
- chunked Arrow responses decode correctly
- preview table supports sorting/pagination in notebook UI

### Milestone 5 - Session Controls and Interrupts

Deliverables:

- `stop/release_session`, interrupt APIs
- telemetry coverage for RPC and streaming

Acceptance:

- long-running query can be interrupted deterministically
- released session rejects further requests

### Milestone 6 - Reattachable Execute (optional V1.1)

Deliverables:

- `ReattachExecute` + `ReleaseExecute` implementation

Acceptance:

- mid-stream disconnect can resume without duplicate rows

## 17. Open Decisions (settled)

- Spark baseline version for generated proto (4.0 vs 4.1): use 4.1, pinned to Spark `v4.1.1`.
- Preferred Arrow decode path: target in-memory decode; MVP fallback is temp-file IPC with Explorer loader.
- DSL scope for V1: keep small subset only; functional API remains canonical.
- Catalog API placement: defer to 1.1 / experimental namespace.

## 18. Success Criteria for V1

- Users can run realistic read-transform-aggregate workflows from Elixir/Livebook against real Spark clusters.
- API is Elixir-idiomatic while remaining familiar to Spark users.
- Reliability basics are covered: timeouts, retries, interruption, session lifecycle, enriched errors.
- The architecture leaves clear upgrade paths for reattach, broader parity, and advanced extensions.
