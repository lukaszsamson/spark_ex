# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`map_format: :map` option on `DataFrame.collect/2`** (FAB-16): opt-in
  decoding of MAP-typed columns into Elixir maps (recursively through
  arrays/structs), mirroring PySpark's dicts. The default wire representation
  (list of `%{"key" => k, "value" => v}` entries) is unchanged.
- **Out-of-band interrupts** (FAB-15): `interrupt_all/tag/operation` now run
  from the caller's process against an ETS-published connection snapshot, so
  they can actually cancel a running operation instead of queueing behind it
  on the Session GenServer (first slice of the NO_SERIALIZED_RPC plan).

### Fixed

- **Wave 1 triage fixes** (see `BUGS_TRIAGED.txt` T-01..T-27, T-40):
  - `Session.artifact_status/2` validates its `names` argument instead of
    crashing the shared Session process on protobuf encode (T-01).
  - `DataFrame.to_explorer(max_rows: :infinity)` no longer injects the atom
    into the remote int32 `LIMIT`; invalid `max_rows` values return
    `{:error, {:invalid_option, _}}` even with `unsafe: true` (T-02).
  - Explorer's non-finite float sentinels (`:nan`, `:infinity`,
    `:neg_infinity`) encode as DOUBLE literals and infer DOUBLE in
    `create_dataframe`, instead of silently becoming strings (T-04).
  - `count("*")`, `count_distinct("*")`, `GroupedData.count/2` and
    `DataFrame.col("*")`/`col("t.*")` route to star expressions; the
    `count(*)` -> `count(1)` rewrite now applies to every star form, and the
    dict-agg star key is no longer aliased as `count(*)` (T-05, T-40).
  - Integers outside the int64 range inside array/map literals infer a
    DECIMAL element type consistently with their children (T-06).
  - `instr/2` treats its second argument as a literal substring, not a
    column (T-07). `first_value`/`last_value` emit their own Spark functions
    instead of `first`/`last` (T-25).
  - The DDL top-level field splitter honours parentheses, backtick/single/
    double-quoted runs and escapes, so `DECIMAL(10, 2)` and comma-containing
    comments no longer produce phantom fields (T-10).
  - The Spark 4.x parse-schema fallback rewrites every sibling parse
    relation, not just the first changed one (T-12).
  - `Writer.jdbc/2,4` compose with an existing Writer builder and reject
    partitioning/bucketing/clustering like Spark's `DataFrameWriter.jdbc`
    (T-20). `Catalog.create_function` emits grammatical SQL for
    `IF NOT EXISTS`, multiple resources and empty resource lists (T-21).
  - `NA.replace` / `DataFrame.replace` raise when the replacement value is
    omitted for a non-map `to_replace`, instead of replacing with `nil`
    (T-24). `GroupedData.agg(%{})` and the numeric shortcuts accept an empty
    aggregate set (distinct grouping keys), matching PySpark (T-26).
  - Decimal types with unset precision/scale serialize as `decimal(10,0)`
    rather than `decimal(,)` (T-27).

- **`lateral_join/4` kept TVF right-sides correlated** (FAB-13): TVF plans were
  silently downgraded to a regular join, so outer column references
  (`tvf.explode(col("arr"))` against the left side) always failed with
  `UNRESOLVED_COLUMN`. LateralJoin is now always encoded (PySpark parity); the
  Spark 3.5 regular-join downgrade moved into the empty-relation legacy
  fallback. `DataFrame.schema/dtypes` on `parse` plans also gained the UNPARSED
  fallback (FAB-14), completing the FAB-11 fix.

- **Legacy collect-retry rewrites were dead code** (FAB-11): the `{:plan_id, n, _}`
  plan envelope broke pattern matching in every legacy fallback rewriter
  (UNPARSED `parse` rewrite, grouping-sets, transpose/table-function/as-of-join
  empty-relation rewrites). `DataFrame.parse/3,4` with a DDL string schema now
  works again on Spark 4.x, including under filters/aggregates and via
  `DataFrame.count/1` (the parse rewrite is now a deep tree rewrite).
- **Timed `await_termination` returned a transport error instead of
  `{:ok, false}`** (FAB-7): the client gRPC deadline equalled the server-side
  wait, so the reply lost the race. The call deadline now carries 30s headroom.
  Affects `StreamingQuery.await_termination/2` and
  `StreamingQueryManager.await_any_termination/2`.
- **Writer format shortcuts compose with the builder** (FAB-10):
  `df |> DataFrame.write() |> Writer.mode(:overwrite) |> Writer.parquet(path)`
  (the README idiom) crashed with a `KeyError`; `parquet/csv/json/orc/avro/xml/
  text` now accept either a DataFrame or a Writer builder.
- **`Column.asc/desc` (and `*_nulls_*` variants) accept column-name strings and
  atoms** (FAB-12, V02_BLOCKERS L1) — PySpark parity; the README quick-start
  `order_by([desc("salary")])` no longer raises.
- **Reader option-collision handling matches Writer** (FAB-3): passing the same
  option both top-level and inside `:options` now raises `ArgumentError`
  (previously the generic reader path silently let `:options` win and
  `Reader.csv/3` silently let top-level win).
- **`create_dataframe` accepts keyword-list rows** (FAB-2) as the idiomatic
  analogue of PySpark dict rows (previously a leaked `Jason.Encoder` error),
  and rejects integers outside the 64-bit range with a clear message instead
  of an opaque server-side `MALFORMED_RECORD_IN_PARSING` failure (FAB-6).
- **`collect_as_map/2` errors on duplicate column names** (FAB-1) instead of
  silently returning `key => key` pairs.
- **`TableValuedFunction` accepts `%SparkEx.TableArg{}` arguments** (FAB-4)
  directly (previously required hand-wrapping in a subquery expression).
- **`lit/1` encodes atoms as strings and raises a descriptive `ArgumentError`
  for unsupported literal shapes** (FAB-5) instead of leaking a
  `FunctionClauseError` from the plan encoder.
- **`trigger(once: true)` warning corrected** (FAB-8): Spark 4.x still accepts
  `:once`; the warning now marks it deprecated rather than removed. Documented
  the per-event-type `:data` shape of streaming listener events (FAB-9).

## [0.2.0] - 2026-06-20

This release is a large correctness and parity pass over the v0.1.x client,
driven by several rounds of PySpark-parity audits. Highlights: a reattachable
execution lifecycle, TLS/keepalive channel hardening, richer remote errors,
and ~100 individual bug fixes across the DataFrame, function, writer, and
streaming surfaces.

### Added

- **Reattachable execution lifecycle.** Lazy response streaming with
  halt-on-complete, EOF backoff escalation, a reattach retry budget that resets
  on progress, and forward-compatible skipping of unknown response variants.
- **`allow_reconnect` session option** and bounded, best-effort resource
  release on shutdown (release tasks are time-boxed and never block termination
  indefinitely).
- **TLS channel controls.** New `ssl_cacert`, `ssl_servername`, and `ssl_verify`
  URI parameters (and corresponding connect options) for CA pinning, SNI, and
  peer verification. Supplying `ssl_cacert` enables `verify_peer` automatically.
- **gRPC channel hardening.** `grpc_keepalive_time_ms` keepalive interval,
  configurable user-agent, IPv6 loopback support, and telemetry hardening.
- **Richer remote errors.** `SparkEx.Error.Remote` now surfaces the server
  error cause chain, error classes, inline stack traces, and
  `breaking_change_info`.
- **Local-data inference & encoding parity** with PySpark for
  `create_dataframe`, including chunked cached local relations and UDT registry
  scoping.
- Expanded built-in function registry and Column API to close PySpark parity
  gaps; hardened `SparkEx.DataFrame.Stat` (e.g. `approx_quantile`).
- `tools/check.sh` local CI gate mirroring the GitHub Actions workflow.

### Changed

- Type rendering and `SqlCommand` encoding aligned with PySpark (F2).
- `config_is_modifiable` now returns a boolean.
- `order_by` rejects integer (ordinal) sort keys in its ascending branches.
- The `:once` streaming trigger now warns instead of raising (forward-compat
  with Spark's deprecation).
- `to_local_iterator` emission shape updated.
- gRPC keepalive: `grpc_keepalive_timeout_ms` and
  `grpc_keepalive_permit_without_calls` URI parameters are now accepted for
  Spark Connect URI compatibility but ignored — the underlying gun HTTP/2
  adapter exposes no equivalent (only a ping interval plus a count-based
  tolerance), so they are stripped rather than silently half-wired.
- **Stable plan_ids assigned at DataFrame construction.** Every `SparkEx.DataFrame`
  now allocates a stable `plan_id` (0-based, matching PySpark's
  `_fresh_plan_id`) when the plan tuple is wrapped by the internal plan-id
  allocator. The allocator is session-scoped: each
  `SparkEx.Session` registers an `:atomics` counter ref in the
  `:spark_ex_plan_id_counters` ETS table (owned by a dedicated owner process),
  and `DataFrame.new/2` + the plan encoder both reserve ids from
  that atomic, so caller-side construction and encoder-side allocation
  share one namespace and cannot collide. Non-session pseudo-sessions
  (test fixtures using `self()`) fall back to a per-process `:counters`
  ref. `DataFrame.col/2`, `col_regex/2`, and `metadata_column/2` capture
  the wrapped plan so column references carry their source DataFrame's id
  end-to-end. This resolves the BUGS_PLAN_5 Stream A "known limitation"
  (same DataFrame as join child + subquery reference) — both sites
  resolve through the same id, no synthetic remap.
- **Self-join wire format.** `df.join(df, …)` now produces
  `join.left.common.plan_id == join.right.common.plan_id` (matches PySpark's
  per-LogicalPlan-instance plan_id). Previously each side received a
  distinct counter id from the encoder. Tests pinning distinct ids should
  match through `SparkEx.Test.PlanHelpers.unwrap_plan/1` or use a `_`
  wildcard.
- DataFrames must be constructed via `SparkEx.DataFrame.new/2` (or one of
  the public session APIs that wrap it) rather than `%DataFrame{plan: …}`
  struct literals — `col/2`, `col_regex/2`, `metadata_column/2` now raise
  `ArgumentError` on unstamped plans with a message pointing at `new/2`.

### Fixed

- **57 PySpark-parity issues** from the BUGS_FABLE audit, including: swapped
  `ltrim`/`rtrim` trim-string argument order; `mask/2` NULL defaults leaving
  text unmasked; Explorer `collect` crashes on interval/UDT/map/timezone and
  `:time` columns; window-frame boundary `0` mapping to `current_row`;
  `select("*")` and bare-string TVF arguments mis-treated as literals;
  `ascending: true` overriding per-column `.desc()`; and unbounded observation
  ETS growth.
- **Session crash-hardening.** Validation errors inside the `SparkEx.Session`
  GenServer (`create_dataframe`, `await_termination` with no timeout) no longer
  crash the whole session; `trap_exit` ensures cleanup runs on abnormal exit.
- **Plan-encoder correctness.** Expression rewrite (plan-id rebinding) now
  covers `sort`, `unpivot`, `collect_metrics`, `call_function`, and
  `named_argument`; top-level expressions are remapped to the encoded child
  input id; self-join and subquery references resolve through one id namespace.
- **Literal vs. column classification.** `raise_error`, `array_insert`,
  `regexp_instr`, and related functions treat bare-string arguments as
  literals instead of column names; `reduce/3,4` emits the `reduce` function
  (was `aggregate`).
- **`DataFrame.sample/2-4` accepts integer fractions** (e.g. `sample(df, 1)`),
  coercing to float to match PySpark.
- **`GroupedData.agg/2` validates aggregate function names** in pair form,
  raising locally instead of issuing a remote call to a bogus function name
  (e.g. `"true"`).
- **`StreamWriter.partition_by/2` and `cluster_by/2`** validation brought to
  parity with the batch writer (reject empty column lists).
- Retry backoff treats the server's `RetryInfo.retry_delay` as a floor rather
  than replacing exponential backoff; reattach budget resets on progress.
- Crash-hardening: `FunctionClauseError`/`CaseClauseError` paths converted to
  typed errors; `analyze_*` responses handle unexpected oneof variants and nil
  nested `DataType` fields without crashing.
- `VARCHAR` trim handling, `TIME` literal sub-second scaling, and
  `CHAR`/`VARCHAR` length preservation in DDL; full signed Long sampling seeds.
- Listener-bus `DOWN` handler no longer races stream restart (no leaked
  server-side listener); exponential backoff for listener reconnect.
- Empty `session_id` responses are treated as absent rather than triggering a
  false session-integrity mismatch.
- Credo baseline cleaned to zero violations; multiple dialyzer fixes.

## [0.1.1] - 2026-03-06

### Changed

- Explorer is now a required dependency (needed for Arrow IPC result decoding)
- `decimal` is now an explicit required dependency (was only transitive)
- `Session.stop/1` is now idempotent — stopping an already-stopped session returns `:ok`
- `Channel.connect/2` accepts extra gRPC options for passthrough to the adapter

### Fixed

- Compilation failure when installing without optional Explorer dependency
- Integration test compatibility with Spark 3.5 (nil `error_class` in remote errors)
- Structured streaming result collection via proper gRPC streaming
- Flaky test cleanup race condition in `on_exit` callbacks
- Multiple reward-hacked tests with tautological assertions, swallowed errors, and contradictory conditions

### Removed

- `connect_timeout` option (was never wired into anything; use `grpc_opts` passthrough instead)

## [0.1.0] - 2026-03-02

### Added

- Native Elixir client for Apache Spark via the Spark Connect protocol
- gRPC-based communication with Spark Connect server
- DataFrame API with support for selects, filters, joins, aggregations, and sorting
- Column expression DSL for building Spark expressions
- Streaming query support with retry policies
- Session management (create, attach, reset, release)
- Telemetry integration for observability
- Optional Explorer integration for converting Spark results to Explorer DataFrames
- Optional Kino integration for Livebook rendering
- Function registry for built-in Spark functions
- Livebook demo notebook

[0.2.0]: https://github.com/lukaszsamson/spark_ex/releases/tag/v0.2.0
[0.1.1]: https://github.com/lukaszsamson/spark_ex/releases/tag/v0.1.1
[0.1.0]: https://github.com/lukaszsamson/spark_ex/releases/tag/v0.1.0
