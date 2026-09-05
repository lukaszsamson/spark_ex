# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Wave 5 behaviour changes** (see `BUGS_TRIAGED.txt` T-28, T-38, T-47..T-49,
  T-56, T-58, T-59):
  - `DataFrame.dtypes/1` returns PySpark `simpleString` (lowercase, no spaces:
    `bigint`, `array<string>`, `map<string,int>`, `struct<a:int,b:string>`,
    `decimal(10,2)`, `void`) instead of uppercase DDL. New
    `TypeMapper.simple_string/1`; `data_type_to_ddl/1` is unchanged (T-59).
  - `UDFRegistration.register_udtf/4` defaults `:deterministic` to `false`,
    matching PySpark's `UserDefinedTableFunction`. Pass `deterministic: true`
    to keep the previous behaviour (T-28).
  - `create_dataframe/3` with list rows and no `:schema` returns
    `{:error, {:cannot_determine_type, column}}` when a column (or nested
    element type) is `nil` in every row, matching PySpark's
    `CANNOT_DETERMINE_TYPE`, instead of silently inferring STRING (T-48).
  - `create_dataframe/3` map rows without an explicit schema order columns by
    the sorted union of row keys (PySpark parity); previously first-seen order,
    which was non-deterministic for more than 32 keys or mixed key types
    (T-47). A `:schema` column-name list shorter than the tuple width is padded
    with `_N` (1-based, continuing after the supplied names) instead of being
    rejected; longer lists remain an error (T-49).
  - `Functions.col/1`, `DataFrame.col/2`, `with_column(s)`, `drop/2`, `NA`
    subsets and fill keys, `GroupedData` shortcuts and window specs accept atom
    column names through a shared normaliser (`SparkEx.Internal.ColumnName`)
    and raise `ArgumentError` for `nil`/`true`/`false` instead of producing a
    column named `"nil"` (T-38). Other name-taking helpers (`Stat`, `TableArg`,
    `WriterV2.partitioned_by`) still stringify any atom. `Window.partition_by/2` on an existing `WindowSpec`
    and `DataFrame.sort/3` (options like `order_by/3`) were added.
  - `Functions.uuid/0` bakes a random seed (`uuid(<seed>)`) like PySpark, so
    plans are no longer byte-identical across calls (T-58).
  - `DataFrame.sample/4` raises for a non-keyword option list instead of
    silently ignoring it (T-56).

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

- **Wave 6 triage fixes — Spark 3.5 compatibility and parity polish** (see
  `BUGS_TRIAGED.txt` T-29, T-34, T-63, T-64):
  - Spark 3.5 fallbacks for 4.x-only relations (lateral join, as-of join,
    transpose, table-valued functions) rewrite the unsupported node at any
    depth of the plan (for example under `select/2` or `filter/2`) and apply to
    `count/1` and `schema/1` as well as `collect/1` (T-34). Table-valued
    functions anywhere in such a plan are rendered as SQL during the rewrite.
  - Cross-DataFrame column references inside `direct_shuffle_partition_id/2`
    are hoisted into `with_relations.references` instead of failing with
    "plan not found"; `SparkEx.sql/3` arguments accept every supported
    expression form (`call_function`, named arguments, `outer`,
    `update_fields`, aliases with metadata) instead of mis-encoding them as
    literals (T-29).
  - Removing the last streaming query listener drains events still in flight
    and waits (default 300 ms, `config :spark_ex, listener_bus_drain_timeout_ms:`)
    for the server to close the stream before killing the reader task; the bus
    drains to remaining listeners on shutdown too (T-63).
  - `create_dataframe/3` reads `spark.sql.session.localRelationCacheThreshold`,
    `localRelationChunkSizeRows`, `localRelationChunkSizeBytes` and
    `localRelationBatchOfChunksSizeBytes` once per session (bounded 5 s RPC,
    not cached on failure; skipped when `:cache_threshold` and
    `:cache_chunk_size` are both given). On Spark 4.1+ the default threshold
    therefore drops from the client's 4 MiB to the server's 1 MiB, payloads at
    or above it are cached (PySpark `>=` boundary), chunks are capped by rows
    and bytes, and uploads are batched. Servers without the 4.1 chunking
    configs (Spark 3.5) keep the 4 MiB client default so payloads stay inlined.
    New `:cache_chunk_rows` option (T-64).

### Fixed

- **Wave 5 triage fixes** (see `BUGS_TRIAGED.txt` T-55, T-57, T-60, T-62):
  - Window frame bounds clamp only in the direction they face:
    `rows_between(MAX_INT64, 0)` no longer becomes an unbounded-following lower
    bound (T-57).
  - `Stat.freq_items/3` validates `support` client-side (at least `1.0e-4`,
    at most `1.0`, the server's own bounds) with a clear `ArgumentError`
    instead of an opaque server error (T-55).
  - `on_query_idle/1` is an optional `StreamingQueryListener` callback; the bus
    dispatches it only to listeners that implement it and no longer warns
    (T-60).
  - Docs: `DataFrame.alias_/2` example and the `StreamWriter.start/2` path note
    (T-62).

- **Wave 4 triage fixes — decoder and result-shape correctness** (see
  `BUGS_TRIAGED.txt` T-30..T-33, T-35, T-36, T-61):
  - `to_explorer/2` now applies the TIMESTAMP schema policy on non-empty
    frames: TIMESTAMP columns are always `{:datetime, :microsecond, "Etc/UTC"}`
    (previously the session time zone on non-empty frames and UTC on empty
    ones), and empty MAP/VARIANT/GEOMETRY/GEOGRAPHY/UDT columns get
    structurally correct dtypes instead of `:null` (T-30).
    `TypeMapper.to_explorer_dtype/1` returns those structural dtypes instead of
    `nil`. Year-month/calendar interval columns still map to `nil` (Explorer
    has no interval dtype).
  - UDT deserialisers now apply to UDTs nested in arrays, maps and structs on
    the collect, `to_local_iterator` and `to_explorer` paths (T-31).
  - The JSON-projection fallback decodes nested DECIMAL (exact digits, via
    `Jason` `floats: :decimals`), TIMESTAMP (`DateTime`), TIMESTAMP_NTZ
    (`NaiveDateTime`), DATE (`Date`) and BINARY (raw bytes) leaves instead of
    leaving JSON scalars. Spark's `to_json` truncates timestamps to
    milliseconds on this path (T-32).
  - `to_explorer/2` and `to_local_iterator/2` apply collect's Arrow preflight
    (duplicate-column renaming; JSON/STRING projection for shapes the decoder
    cannot handle) instead of surfacing `arrow_decode_failed`. The Explorer
    path projects only nested maps and unsupported scalars, so top-level
    struct/map/array columns stay native containers. `to_local_iterator`
    honours `map_format: :map` on projected plans. This adds one AnalyzePlan
    round trip for SQL/read plans on those two paths (T-33).
  - Explorer-mode decoding no longer drops command-result response variants;
    results gain `command_result`/`command_results` (T-61).
  - Progress handlers: payloads gain `stages`, `inflight_tasks`,
    `operation_id` and `done`; handlers are not invoked for zero-task frames,
    and exactly one `done: true` event fires at the end of every execution
    (including failures), mirroring PySpark. Handlers that counted invocations
    see one extra call per execution (T-36).
  - `FetchErrorDetails` enrichment is lazy: retried RPC attempts no longer pay
    the up-to-5 s round trip; only the terminal error is enriched, once.
    `%SparkEx.Error.Remote{}` gains `error_id` and `enriched?` (T-35).

### Fixed

- **Wave 3 triage fixes — Session plumbing and option handling** (see
  `BUGS_TRIAGED.txt` T-11, T-13, T-14, T-22, T-23, T-39, T-44..T-46,
  T-50..T-54):
  - Out-of-band interrupts observe the server-side session id through the
    same integrity check as every other RPC: the first id learned via
    `Interrupt` republishes the connection snapshot and a rotated id closes
    the session (T-11).
  - `map_format: :map` survives the unique-column and legacy collect retry
    paths (T-13), is validated (`{:error, {:invalid_option, {:map_format, _}}}`
    for typos) and is honoured by `to_local_iterator/2` (T-39).
  - `DataFrame.count/2`, `show/2` and `html_string/2` forward DataFrame tags
    and accept `:timeout` (T-14).
  - Stream APIs no longer block behind a running execute when probing the
    Session process (T-44); `Session.is_stopped/1` returns `true` for a
    stopped session instead of exiting (T-45); tags are deduplicated (T-46).
  - Reader/Writer/StreamReader/StreamWriter share one option-normalization
    path: keyword-list `:options` no longer crash `Reader.csv/3` and
    `StreamReader.rate/2`, equal `sep`/`separator` aliases are accepted,
    duplicate top-level/nested options raise consistently in all four
    builders (spelling-insensitively, so `multi_line:` and `"multiLine"`
    collide), and lowercase snake_case atom keys such as `multi_line:` are
    sent as Spark's camelCase names instead of being silently ignored
    (T-22, T-23, T-52). Dotted, uppercase or otherwise non-snake_case atoms,
    JDBC connection `properties` and `WriterV2` table properties are sent
    verbatim. Singular `option/3` accepts atom keys; `Writer.jdbc/4` accepts
    `properties:` like the reader.
  - Explicit `format: nil` / `schema: nil` on `load/3` keep the builder's
    value (T-50); `StreamWriter.option(key, nil)` clears the option and `[]`
    clears partition/bucket/cluster configuration (T-51).
  - `StreamReader.schema/2` accepts a protobuf `DataType`; the
    non-partitioned `Reader.jdbc/4` accepts a `properties` map;
    `Reader.table/3` rejects a `%Reader{}` passed as the session with a clear
    error (T-52, T-53).
  - Catalog database DDL quotes dotted names per component and alter
    builders accept keyword-list properties (T-54).

- **Wave 2 triage fixes — transport, retry and stream lifecycle** (see
  `BUGS_TRIAGED.txt` T-03, T-08, T-09, T-15..T-19, T-37, T-41..T-43):
  - Managed streams (streaming listener event streams) now release their
    server-side execution when the owner process exits abnormally: the
    controller is started unlinked and relies on the owner monitor (T-03).
    They also go through the reattachable execution machinery, so a graceful
    EOF or transient transport loss reattaches instead of truncating (T-09).
  - The initial reattachable `ExecutePlan` RPC is retried with the reattach
    policy like every other RPC (T-08); a fresh `ExecutePlan` after
    `OPERATION_NOT_FOUND` now honours the retry budget instead of looping
    (T-15); jitter is applied after the server-provided `RetryInfo` floor so
    throttled clients no longer retry in lockstep (T-16).
  - Streaming listener bus: stopping the event stream no longer leaves the
    bus permanently in a "closing" state (T-17), and a crashed reader task
    reconnects with the same backoff as a transport error (T-18).
  - Best-effort release and error-enrichment tasks tolerate
    `SparkEx.TaskSupervisor` being down during shutdown instead of taking a
    successful result down with them (T-19).
  - TLS connections without `ssl_cacert` now explicitly request
    `verify_peer` with the OS CA store and HTTPS hostname checking, rather
    than relying on OTP/gun defaults (which only verify on OTP 26+);
    `ssl_verify=none` remains an explicit opt-out (T-37).
  - `DataFrame.to_arrow/2` forwards `max_rows`/`max_bytes` (T-41); the raw
    reattachable stream halts at `ResultComplete` (T-42);
    `SparkEx.Error.Remote` messages prefer the full server-side error text
    over the truncated gRPC status message (T-43).

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
