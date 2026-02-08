# SparkEx V1 Specification (Option A: Native Spark Connect Client)

## 1. Purpose

Build an Elixir library (`spark_ex`) that talks to Spark via Spark Connect gRPC, with a PySpark-like DataFrame API adapted to idiomatic Elixir and first-class Livebook support.

This spec targets Option A from `HLD.md`: native Spark Connect client (no JVM sidecar).

## 2. Scope

### 2.1 V1 Goals

- Native Spark Connect protocol client in Elixir using generated protobuf + gRPC stubs.
- Core lazy DataFrame API (read, transform, action).
- Interoperability with:
  - `:grpc` (transport)
  - `:protobuf` (proto structs)
  - `:explorer` (local tabular representation)
  - `:kino` (Livebook rendering/UX)
- Real-cluster testable milestones with incremental delivery.
- API shape guided by PySpark Connect, with Elixir ergonomics.

### 2.2 Non-goals for V1

- Full PySpark parity.
- RDD APIs / SparkContext-like low-level APIs.
- General UDF shipping from Elixir runtime.
- Structured Streaming full support (deferred).

## 3. External Protocol and Behavioral Constraints

Based on Spark Connect docs and source in `/Users/lukaszsamson/claude_fun/spark`:

- Connection URI format: `sc://host:port/;key=value`.
- Default port: `15002`.
- Core RPCs: `ExecutePlan` (streaming), `AnalyzePlan`, `Config`, `Interrupt`, `ReleaseSession`, plus `ReattachExecute`/`ReleaseExecute`.
- Requests are scoped by `user_context.user_id` + `session_id`.
- Results are streamed as Arrow batches (`ExecutePlanResponse.arrow_batch`).
- SQL/DataFrame operations are sent as unresolved logical plans (`Relation`, `Expression`).
- Catalog proto is explicitly marked unstable; keep catalog API in V1 as experimental.

## 4. High-level Architecture

### 4.1 Public Modules

- `SparkEx`
  - User entrypoints (`connect/1`, convenience helpers).
- `SparkEx.Session`
  - Session identity, connection options, lifecycle (`stop/1`, `clone/1`).
- `SparkEx.DataFrame`
  - Immutable lazy logical plan handle.
- `SparkEx.Column`
  - Expression wrapper.
- `SparkEx.Functions`
  - Function constructors (`col/1`, `lit/1`, `sum/1`, `count/1`, `expr/1`, etc.).
- `SparkEx.Reader`
  - `table/2`, `parquet/2`, `csv/2`, `json/2`, `load/2`.

### 4.2 Internal Modules

- `SparkEx.Connect.Channel`
  - URI parsing, TLS/token setup, headers/metadata.
- `SparkEx.Connect.Client`
  - gRPC stubs, retries, timeout handling, operation IDs.
- `SparkEx.Connect.PlanEncoder`
  - Elixir plan/expression structs -> Spark Connect protobuf.
- `SparkEx.Connect.ResultDecoder`
  - ExecutePlan stream handling, Arrow batch assembly, schema extraction.
- `SparkEx.Connect.Errors`
  - gRPC + Spark structured error mapping.
- `SparkEx.Livebook.Renderer`
  - Kino integration and notebook UX wrappers.

### 4.3 Plan Representation

Use internal Elixir structs (IR), then encode to proto at action time.

Examples:

- Relations: `Read`, `Project`, `Filter`, `Join`, `Aggregate`, `Sort`, `Limit`, `SQL`.
- Expressions: `Literal`, `ColumnRef`, `UnresolvedFunction`, `Alias`, `SortOrder`, `ExprString`.

This mirrors PySpark Connect architecture (`plan.py`, `expressions.py`) while keeping BEAM-native internals.

## 5. API Design

## 5.1 Session and Connection

```elixir
{:ok, spark} = SparkEx.connect(
  url: "sc://spark-host:15002/;use_ssl=true;token=...",
  user_id: "lukasz",
  user_agent: "spark_ex/0.1",
  grpc_max_message_size: 128 * 1024 * 1024
)
```

- Respect Spark Connect URI semantics.
- If token is provided, set authorization metadata (`Bearer <token>`).
- Auto-generate `session_id` UUID if not provided.

## 5.2 DataFrame Operations (V1 subset)

Read APIs:

- `SparkEx.read.table(session, table_name, opts \\ [])`
- `SparkEx.read.parquet(session, paths, opts \\ [])`
- `SparkEx.read.csv(session, paths, opts \\ [])`
- `SparkEx.read.json(session, paths, opts \\ [])`
- `SparkEx.sql(session, sql, args \\ [])`

Transforms:

- `select/2`, `select_expr/2`
- `filter/2`
- `with_column/3`
- `drop/2`
- `join/4`
- `group_by/2` + `agg/2`
- `order_by/2`
- `limit/2`

Actions:

- `collect/2`
- `take/2`
- `count/1`
- `schema/1`
- `explain/2`
- `to_explorer/2`
- `show/2`

## 5.3 Column and Functions

- `SparkEx.Functions.col/1`, `lit/1`, `expr/1`
- Aggregations: `count/1`, `sum/1`, `avg/1`, `min/1`, `max/1`
- Predicates/string basics: `is_null/1`, `is_not_null/1`, `contains/2`, `starts_with/2`

## 5.4 Idiomatic Elixir DSL (optional but recommended)

Provide macro DSL in `SparkEx.Query`:

```elixir
import SparkEx.Query

spark
|> SparkEx.read.table("events")
|> where(col("country") == ^"PL" and col("amount") > ^100)
|> select([:country, :amount])
|> limit(100)
|> SparkEx.to_explorer()
```

Guidelines:

- Macros only as syntax sugar over stable function API.
- Pinned values (`^value`) become literals.
- Unknown AST forms fail fast with descriptive compile errors.

## 6. Livebook Interoperability

### 6.1 Kino Rendering

Implement `Kino.Render` for `SparkEx.DataFrame` with:

- lazy schema panel (`schema/1` via `AnalyzePlan`)
- preview table (`to_explorer(limit: N)`)
- optional explain section

### 6.2 Explorer Integration

- Primary local representation: `Explorer.DataFrame`.
- `to_explorer/2` defaults:
  - bounded row count (`limit`) to avoid notebook OOM
  - configurable max bytes/rows
- For large data, keep remote and render sampled preview only.

### 6.3 Notebook UX

Add helpers:

- `SparkEx.Livebook.preview(df, opts \\ [])`
- `SparkEx.Livebook.explain(df, mode \\ :extended)`
- `SparkEx.Livebook.sample(df, n \\ 1000)`

## 7. Transport, Reliability, and Error Handling

### 7.1 gRPC

- Unary-stream handling for `ExecutePlan`.
- Propagate metadata (token, custom headers, user-agent).
- Configurable per-call timeout.

### 7.2 Retry Policy

Adopt PySpark-inspired retry categories:

- Retry transient network issues (`UNAVAILABLE`, etc.) with capped exponential backoff.
- Do not retry semantic Spark errors.
- Optional support for server-provided retry delay metadata.

### 7.3 Reattachable Execute (Phase 3+)

- Support operation IDs.
- On stream breaks, call `ReattachExecute` with last response ID.
- Use `ReleaseExecute` to free server buffers.

### 7.4 Error Model

Normalize into:

- `SparkEx.Error.Transport`
- `SparkEx.Error.Remote` (error class/sql state/message params when available)
- `SparkEx.Error.InvalidPlan`
- `SparkEx.Error.Unsupported`

## 8. Versioning and Compatibility

- Pin protocol generation to one Spark baseline (initially Spark 4.x from checked-in proto set).
- Publish compatibility matrix:
  - `spark_ex` version
  - proto baseline
  - validated Spark server versions
- Treat unstable proto surfaces (`Catalog`) as experimental namespace:
  - `SparkEx.Experimental.Catalog`

## 9. Dependencies (initial)

- `:grpc`
- `:protobuf`
- `:explorer`
- `:kino`
- `:telemetry`
- `:nimble_options`

Optional later:

- `:jason` for metadata/JSON utilities
- dedicated Arrow IPC helper if needed by decoder path

## 10. Testing Strategy (guided by PySpark)

PySpark guidance applied:

- API surface parity checks for selected subset.
- Signature and behavior compatibility checks where meaningful.
- Protocol-level encode/decode tests.
- End-to-end tests against a real Spark Connect server.

### 10.1 Test Layers

1. Unit tests
- Expression -> proto mapping.
- Relation plan assembly for each DataFrame op.
- URI parser, option normalization, metadata injection.

2. Contract tests
- Golden protobuf snapshots for critical operations.
- Error mapping from known gRPC status payloads.

3. Integration tests (real Spark)
- ExecutePlan/AnalyzePlan roundtrips.
- Arrow streaming + chunk assembly.
- Retry + reconnect behavior.

4. Livebook integration tests
- `Kino.Render` smoke tests.
- `to_explorer/2` schema + value conversion checks.

### 10.2 Coverage Matrix (V1 minimum)

- Session lifecycle: connect, config get/set subset, release session.
- Reader: table/parquet/csv/json.
- DataFrame ops: select/filter/with_column/join/group_by+agg/order_by/limit.
- Actions: count/take/collect/schema/explain/to_explorer.
- Error paths: invalid SQL, bad column, interrupted operation, auth failure.

## 11. Incremental Implementation Plan

## Milestone 0 - Foundations

Deliverables:

- Proto/vendor setup and code generation scripts.
- Basic project structure and types.

Acceptance (real cluster):

- Connect to Spark Connect endpoint and fetch Spark version (`AnalyzePlan.spark_version`).

## Milestone 1 - Minimal Query Path

Deliverables:

- `SparkEx.connect/1`
- `SparkEx.sql/2`
- `SparkEx.count/1`, `SparkEx.collect/1` (small results)

Acceptance:

- `SELECT 1` and `spark.range(...).count()` equivalent query works end-to-end.

## Milestone 2 - DataFrame Core

Deliverables:

- Reader + core transforms (`select`, `filter`, `limit`, `order_by`).
- `schema/1`, `explain/2`.

Acceptance:

- Read parquet dataset from cluster storage and run filtered projection.
- Explain plan returns non-empty output for all explain modes supported.

## Milestone 3 - Aggregations and Joins

Deliverables:

- `group_by`/`agg`, `join` variants, `with_column`, `drop`.
- Basic retry/backoff and operation tagging.

Acceptance:

- Join + aggregate query on non-trivial data set matches expected results.
- Simulated transient network failure is retried successfully.

## Milestone 4 - Arrow + Explorer + Kino

Deliverables:

- Stable Arrow streaming decode path.
- `to_explorer/2`.
- `Kino.Render` for DataFrame previews.

Acceptance:

- Livebook notebook can preview schema + sample rows from remote Spark.
- Large result guardrails prevent unbounded materialization.

## Milestone 5 - Session Controls and Interrupts

Deliverables:

- `interrupt_all`, `interrupt_operation`, `stop/release_session`.
- Better structured error reporting.

Acceptance:

- Long query can be interrupted from client and returns deterministic error.
- Session release invalidates further requests as expected.

## Milestone 6 - Reattachable Execution (optional for V1.1)

Deliverables:

- `ReattachExecute` + `ReleaseExecute` path.
- Stream resume on dropped connection.

Acceptance:

- Mid-stream disconnection resumes without data corruption/duplication.

## 12. Repository Layout Proposal

```text
lib/
  spark_ex.ex
  spark_ex/session.ex
  spark_ex/data_frame.ex
  spark_ex/column.ex
  spark_ex/functions.ex
  spark_ex/query.ex
  spark_ex/reader.ex
  spark_ex/livebook/renderer.ex
  spark_ex/connect/channel.ex
  spark_ex/connect/client.ex
  spark_ex/connect/plan_encoder.ex
  spark_ex/connect/result_decoder.ex
  spark_ex/connect/errors.ex

priv/proto/spark/connect/*.proto

test/
  unit/
  contract/
  integration/
  livebook/
```

## 13. Open Decisions (must be settled before Milestone 2)

- Spark baseline version for generated proto (4.0 vs 4.1). Use 4.1. The spark sources are checked out at tag v4.1.1
- Preferred Arrow decode path in Elixir runtime for best Explorer interoperability. Aim for in-memory decoding via arrow-rs/polars. For MVP we may write to temp file and load with Explorer.DataFrame.from_ipc_stream!/2
- DSL scope for V1 (`where`/`select` macros only vs broader macro API). Start with small subset, mimic Ecto API
- Whether catalog APIs are included in V1 or moved fully to experimental. Defer to 1.1

## 14. Success Criteria for V1

- Users can run realistic read-transform-aggregate workflows from Elixir/Livebook against real Spark clusters.
- API feels Elixir-native while remaining familiar to PySpark users.
- Core reliability expectations (timeouts, retries, interruption, session lifecycle) are covered.
- Clear extension path exists for streaming, UDF story, and broader parity.
