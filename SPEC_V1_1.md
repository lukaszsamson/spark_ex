# SparkEx V1.1 Specification
## PySpark Connect Parity Expansion Plan

## 1. Goal

Define the missing functionality required for `spark_ex` to claim practical parity with `pyspark.sql.connect` on Spark 4.1.1, building on completed milestones 0-6.

This document focuses on parity gaps and an implementation/testing roadmap, not on already-delivered foundations.

## 2. Baseline (Implemented)

Current `spark_ex` includes:

- Native Spark Connect client with session lifecycle
- Core lazy DataFrame API (SQL/range/read/select/filter/join/group/set ops)
- Explain/schema/count/collect/show
- Explorer/Kino/Livebook integration
- Interrupt + release session
- Reattachable execute + release_execute
- Unit + integration tests against real Spark Connect server

## 3. Parity Audit Summary

Audit sources:

- `spark_ex` code in `/Users/lukaszsamson/claude_fun/spark_ex/lib`
- PySpark Connect API in `/Users/lukaszsamson/claude_fun/spark/python/pyspark/sql/connect`
- Spark Connect protos in `/Users/lukaszsamson/claude_fun/spark/sql/connect/common/src/main/protobuf/spark/connect`
- PySpark Connect tests in `/Users/lukaszsamson/claude_fun/spark/python/pyspark/sql/tests/connect`

Current rough coverage indicators (from local code scan):

- Relation encoder coverage: `15 / 59` `Relation.rel_type` variants
- Expression encoder coverage: `8 / 22` `Expression.expr_type` variants
- Command encoder coverage: `0 / 19` concrete `Command.command_type` variants (`+ extension`)
- AnalyzePlan coverage: `3 / 14` analyze operations
- Config coverage: `2 / 7` operations
- RPC gaps: `AddArtifacts`, `ArtifactStatus`
- Function-surface gap: `SparkEx.Functions` exposes a small subset vs `pyspark.sql.connect.functions.builtin`

Notes:

- These metrics are indicators, not contractual claims.
- SQL relation parameterization (`args` positional/named) is already implemented and not a parity gap.
- Before public parity claims, add a reproducible in-repo parity-audit task that computes these counts from pinned Spark/PySpark sources.

## 4. Missing Feature Inventory

## 4.1 Protocol/RPC Coverage Gaps

Missing or partial RPC/API coverage:

- `AddArtifacts` RPC (artifact upload)
- `ArtifactStatus` RPC (artifact existence checks)
- `ExecutePlanRequest.ResultChunkingOptions.preferred_arrow_chunk_size` not configurable
- Request/user context extensions (`UserContext.extensions`) not exposed
- Authentication transport contract is not explicit (call credentials vs metadata fallback behavior)
- `FetchErrorDetails` is used internally for error enrichment, but has no explicit public-facing contract/tests in parity profiles
- SQL execution contract should explicitly require `Relation.SQL` with positional/named `args` and avoid deprecated SQL fields for parity-grade compatibility

Required protocol invariants to codify:

- Execute stream reassembly contract: Arrow and row-chunk payloads must be reassembled deterministically before materialization
- `ResultComplete` contract: completion semantics, terminal metadata handling, and stream-finalization behavior must be documented and tested
- Chunking defaults contract: define default `ResultChunkingOptions` policy (enabled/disabled and default size) and override precedence at API/request level

## 4.2 Command Surface Gaps

`Plan.command` support is currently missing. This is a major parity blocker because many PySpark APIs are command-driven (not relation-driven).

Missing command families:

- DataFrame writers: `write_operation`, `write_operation_v2`
- View creation: `create_dataframe_view`
- UDF/UDTF and data source registration: `register_function`, `register_table_function`, `register_data_source`
- Checkpoint/cache management commands: `checkpoint_command`, `remove_cached_remote_relation_command`
- Merge command: `merge_into_table_command`
- Streaming command family: `write_stream_operation_start`, `streaming_query_command`, `streaming_query_manager_command`, `streaming_query_listener_bus_command`
- Operational commands: `get_resources_command`, `create_resource_profile_command`, `execute_external_command`
- Additional command tags to decide scope for in parity claims: `sql_command`, `ml_command`, `pipeline_command`

## 4.3 AnalyzePlan and Config Gaps

Missing `AnalyzePlan` operations:

- `tree_string`, `is_local`, `is_streaming`, `input_files`
- `ddl_parse`, `json_to_ddl`
- `same_semantics`, `semantic_hash`
- `persist`, `unpersist`, `get_storage_level`

Missing config operations:

- `get_with_default`, `get_option`, `get_all`, `unset`, `is_modifiable`

## 4.4 DataFrame API Gaps (Batch)

Major missing DataFrame APIs vs PySpark Connect:

- Projection/shape: `selectExpr`, `toDF`, `alias`, `withColumnRenamed`, `withColumns`, `withColumnsRenamed`
- Row ops: `head`, `first`, `tail`, `offset`, `isEmpty`
- Set semantics: `unionByName`, `exceptAll`, `intersectAll`
- Partitioning: `repartition`, `repartitionByRange`, `repartitionById`, `coalesce`, `sortWithinPartitions`
- Sampling/statistical: `sample`, `randomSplit`
- Convenience aliases: `where`, `unionAll`, `crossJoin`
- Grouping features: `rollup`, `cube`, `pivot`, `groupingSets`
- Metadata/views: `createTempView`, `createOrReplaceTempView`, global temp view variants
- Metadata/query shaping: `hint`, `withMetadata`, `withWatermark`, `dropDuplicatesWithinWatermark`, `to(schema)`
- Dedup variants: `dropDuplicates(subset)` parity (current `distinct` is only full-row dedup)
- Metadata columns/subquery helpers: `colRegex`, `metadataColumn`, `asTable`, `scalar`, `exists`
- Display/introspection parity: `printSchema` tree-string mode and `HtmlString` path
- Transformation helpers: `transform` functional combinator
- Local extraction/iteration helpers: `toArrow`, `toLocalIterator`, `foreach`, `foreachPartition`
- Python/Arrow mapped transforms (profile-gated): `mapInPandas`, `mapInArrow`
- Cache/persistence: `cache`, `persist`, `unpersist`, `storageLevel`, `checkpoint/localCheckpoint`
- Semantics helpers: `sameSemantics`, `semanticHash`, `inputFiles`, `isLocal`, `isStreaming`
- Execution metadata parity: `executionInfo`
- NA/stat sub-APIs: `na.fill/drop/replace`, `describe/summary/corr/cov/crosstab/freqItems/approxQuantile/sampleBy`
- Advanced joins: `asOfJoin`, `lateralJoin`
- Advanced reshaping: `unpivot`, `transpose`
- Observation: `observe`
- Cross-session semantics: explicit error contract/tests for same-session requirements (already enforced for key ops in implementation)

## 4.5 Column and Expression Gaps

Missing common `Column` capabilities:

- `eqNullSafe`, `isin`, `between`
- `endswith`, `rlike`, `ilike`, `substr`
- `getItem`, `getField`, `withField`, `dropFields`
- `isNaN`, `when`, `otherwise`, `outer`, column-level `transform`
- bitwise operators, modulo/pow, unary negative
- `try_cast`
- window binding (`over(window_spec)`)

Expression-level gaps in encoder:

- Window, lambda, update fields, regex extract, subquery expressions
- CallFunction / NamedArgumentExpression support
- Merge actions for MERGE INTO
- Metadata/regex-related expression support for `metadataColumn`/`colRegex`

## 4.6 GroupedData and Window API Gaps

Missing GroupedData expansion:

- Numeric aggs and helpers beyond `agg`: `count`, `min`, `max`, `sum`, `avg`, `pivot`
- Advanced grouped execution: `applyInPandas`, `applyInArrow`, stateful grouped operations (profile-gated)

Missing Window API surface:

- `Window.partitionBy/orderBy/rangeBetween/rowsBetween`
- `WindowSpec` equivalents required by `Column.over/1`

## 4.7 Functions/Aggregations Gaps

Current `SparkEx.Functions` is minimal (`col/lit/expr` + a few aggs).

Missing parity area:

- Most scalar SQL functions
- Most aggregate/window functions
- Array/map/struct/higher-order helpers
- Null/date/time/string/binary/math/stat families
- Critical ETL blockers to prioritize early: conditional (`when/otherwise`), regex, date/time parsing/formatting, struct/map/array constructors, window rank/lag/lead family

## 4.8 Reader/Writer Gaps

Reader gaps:

- Generic `format/load/option/options/schema`
- Additional formats: `text`, `orc`, `xml`, `avro`, `jdbc`

Writer gaps:

- DataFrameWriter: `format`, `mode`, `option(s)`, `save`, `saveAsTable`, `insertInto`, `partitionBy`, `bucketBy`, `sortBy`, `clusterBy`, file-format writers
- DataFrameWriterV2: `writeTo(...).using(...).option(s).tableProperty(...).partitionedBy(...).clusterBy(...).create/replace/createOrReplace/append/overwrite/overwritePartitions`

## 4.9 Catalog API Gaps

No `SparkEx.Catalog` parity module yet for:

- current/set catalog and database
- list/get/exists for catalogs/databases/tables/functions/columns
- createTable/createExternalTable
- drop temp views
- cache/uncache/clear/refresh/recover partitions

## 4.10 Artifact + Local Data Gaps

Missing critical parity path used heavily by PySpark Connect:

- Artifact manager for jars/pyfiles/files/archives/cached chunks
- `SparkSession.addArtifacts` equivalent
- `copyFromLocalToFs` equivalent
- Local relation creation path (`createDataFrame`) and chunked local relation caching protocol
- Explicit note: without artifacts + local relation path, `createDataFrame` parity and many UDF/data-source workflows cannot be claimed

## 4.11 UDF/UDTF/Data Source Gaps

Missing:

- `udf` registration APIs (Python/Java UDF registration surface)
- `udtf` registration and invocation
- Python data source registration hooks
- TVF ergonomics (`spark.tvf.*`)

Note:

- Python-runtime-specific execution modes (Pandas/Arrow Python workers) require explicit cross-runtime strategy in Elixir and are a separate complexity tier.

## 4.12 Streaming Gaps

Missing full structured streaming parity:

- `readStream` / `DataStreamReader`
- `writeStream` / `DataStreamWriter`
- `StreamingQuery`, `StreamingQueryManager`, listener bus
- `foreach`/`foreachBatch` protocol bindings

## 4.13 Type, Literal, and Structure Mapping Gaps

Gaps for parity-grade type fidelity:

- Decimal round-trip as typed decimal (currently fallback to string in Explorer mapping)
- Interval types fidelity
- Complex/nested map/array/struct fidelity in `to_explorer/2` display path
- Geography/Geometry/Variant validation coverage
- End-to-end parity tests for all Spark `DataType` variants, not only decode success
- Literal encoding parity gap for `lit/1`: beyond primitive bool/int/float/string/null to date/timestamp/decimal/binary/interval/complex literals

## 4.14 Session/Progress/Error Handling Gaps

Missing or partial parity semantics:

- Session-level tag management (`addTag/removeTag/getTags/clearTags`) compared with PySpark session API
- Progress-handler registration API parity (SparkEx currently exposes telemetry, but no direct handler registration contract)
- Error-handling parity contract: retry behavior, enriched error surface, and idempotency/session-continuity behaviors need explicit profile-level acceptance tests
- Session builder ergonomics (`SparkSession.Builder` style) are not mirrored in SparkEx API
- Plan compression parity (`Plan.compressed_operation` / ZSTD) is not implemented as a supported feature

## 4.15 Explicit Out-of-Scope Areas (for V1.1)

Out of scope unless explicitly promoted in later milestones:

- Full ML/Pipeline command parity (`ml_command`, `pipeline_command`)
- Python-worker-specific parity guarantees beyond documented profile scope

## 5. Parity Profiles

To avoid false parity claims, use explicit profiles with concrete contracts:

- `Batch Core Parity`
- Included APIs: core DataFrame/Column transforms and actions, Reader/Writer basics, Catalog basics, core function surface
- Required protocol coverage: core relations, command encoder foundation, AnalyzePlan/Config core set
- Acceptance tests: mapped subset of `test_parity_dataframe.py`, `test_parity_column.py`, `test_parity_readwriter.py`, `test_parity_catalog.py`
- Explicit unsupported list: documented per release notes

- `Batch Advanced Parity`
- Included APIs: UDF/UDTF/TVF, NA/stat, merge, local relation + artifact path, advanced grouped/window features
- Required protocol coverage: command families for registration/merge/checkpoint, expanded expression/literal coverage
- Acceptance tests: mapped subset of `test_parity_udf.py`, `test_parity_udtf.py`, `test_parity_stat.py`, `client/test_artifact.py`
- Explicit unsupported list: Python-worker-specific behavior not guaranteed unless `Python Worker Parity` is also claimed

- `Streaming Parity`
- Included APIs: `readStream`, `writeStream`, `StreamingQuery`, `StreamingQueryManager`, listener bus lifecycle
- Required protocol coverage: streaming command families and listener event handling
- Acceptance tests: mapped subset of `streaming/test_parity_streaming.py`, `streaming/test_parity_listener.py`, `streaming/test_parity_foreach_batch.py`
- Explicit unsupported list: advanced stateful streaming APIs until explicitly implemented

- `Interactive/Notebook Parity`
- Included APIs: `show`/`print_schema` style output, Explorer materialization, Kino/Livebook rendering helpers
- Required protocol coverage: `ShowString` and optional `HtmlString`, safe preview defaults
- Acceptance tests: SparkEx unit/integration notebook tests plus smoke checks against representative parity notebooks
- Explicit unsupported list: byte-for-byte equality with PySpark HTML rendering

- `Python Worker Parity` (optional)
- Included APIs: pandas/Arrow Python-worker-dependent operations
- Required protocol coverage: Python UDF/UDTF execution modes and worker-compatible data exchange
- Acceptance tests: mapped subset of `tests/connect/pandas/*` and `tests/connect/arrow/*`
- Explicit unsupported list: features intentionally omitted for native Elixir-first runtime

- `Error Handling Parity` (optional but recommended)
- Included APIs: enriched remote errors, retry policy controls, session continuity/idempotency safety behavior
- Required protocol coverage: `FetchErrorDetails`, structured error mapping, retry/reattach semantics
- Acceptance tests: mapped subset of `test_parity_errors.py`, `test_connect_error.py`, `client/test_client_retries.py`
- Explicit unsupported list: server-specific non-portable error enrichments

- `Session Management Parity` (optional but recommended)
- Included APIs: clone/release/interrupt/tag lifecycle and progress handler contracts
- Required protocol coverage: `CloneSession`, `ReleaseSession`, `Interrupt`, progress event handling
- Acceptance tests: mapped subset of `test_connect_clone_session.py`, `test_connect_session.py`, job-cancellation scenarios
- Explicit unsupported list: session-builder UX exact equivalence with PySpark

`spark_ex` should not claim “full PySpark parity” until `Batch Core`, `Batch Advanced`, and `Streaming` profiles pass for the pinned Spark/PySpark revision.

## 6. Implementation Plan (V1.1 Milestones)

## Milestone 7 - Protocol Completion Foundation

Deliverables:

- Add RPC support for `AddArtifacts` and `ArtifactStatus`
- Expand `AnalyzePlan` + `Config` APIs to full proto operation set
- Add configurable execute request options (`preferred_arrow_chunk_size`, chunking toggle)
- Define and implement auth/token transport precedence contract:
  - preferred path via call credentials when configured
  - metadata-header fallback path when call credentials are unavailable
- Codify SQL request encoding contract:
  - use `Relation.SQL` as the canonical SQL path
  - keep positional/named SQL argument mapping aligned with Spark Connect semantics
  - avoid deprecated SQL command encodings in default execution path
- Publish execute stream invariants:
  - chunk reassembly rules
  - `ResultComplete` terminal semantics
  - default chunking policy and override behavior
- Add reproducible parity-audit task/script for coverage metrics against pinned Spark/PySpark sources
- Add proto pin/regeneration appendix (pinned Spark proto revision, transitive proto dependency list, and regeneration workflow)

Acceptance:

- Integration tests for every Analyze/Config operation against real Spark cluster
- Artifact status check passes for uploaded and missing files
- Auth transport behavior is validated with both call-credentials and metadata-fallback configurations
- SQL encoded requests are validated against canonical `Relation.SQL` payload expectations
- Stream completion/reassembly tests validate deterministic handling of chunked responses and `ResultComplete`
- Coverage audit output is generated in CI and attached to release artifacts

## Milestone 8 - Local Data + Artifact Manager

Deliverables:

- `SparkEx.Session.add_artifacts/2` and helpers for file/jar/archive categories
- `copy_from_local_to_fs/3`
- `SparkEx.create_dataframe/3` initial local relation support (prioritize `Explorer.DataFrame`, then list/map inputs)
- Chunked local relation cache upload path
- Local relation cache keying and server artifact lifecycle semantics documented/tested

Acceptance:

- Create DataFrame from local Elixir data and execute SQL/filter/join server-side
- Chunked upload path exercised with payloads exceeding single-message limits

## Milestone 9 - Reader/Writer + Command Encoder

Deliverables:

- Introduce `SparkEx.Connect.CommandEncoder`
- Implement `create_dataframe_view` command path for temp/global view APIs
- Implement DataFrameWriter and DataFrameWriterV2 API subset to match PySpark signatures
- Extend Reader to generic `format/load/option(s)/schema` and more formats

Acceptance:

- Roundtrip tests: read -> transform -> write -> read-back equality
- Save modes and table writes verified in integration tests

## Milestone 10 - DataFrame Parity Pack A (High Use)

Deliverables:

- `select_expr`, rename APIs, `union_by_name`, `except_all`, `intersect_all`
- repartition/coalesce/sortWithinPartitions/sample/randomSplit
- convenience aliases and ergonomics: `where`, `union_all`, explicit `cross_join`
- temp/global view creation APIs (via command encoder)
- metadata/query shaping APIs: `hint`, `with_metadata`, `with_watermark`, `drop_duplicates_within_watermark`
- subquery/dataframe expression helpers: `as_table`, `scalar`, `exists`
- display parity helpers: `print_schema` tree-string mode and optional HTML-string parity path
- semantic/hash/local/streaming/inputFiles introspection APIs

Acceptance:

- Behavior parity tests mirrored from PySpark connect dataframe/readwriter parity suites

## Milestone 11 - Column + Functions Expansion

Deliverables:

- Expand `SparkEx.Column` operators and predicates to common PySpark set
- Generate large portion of `SparkEx.Functions` wrappers from a declarative function registry
- Add window API (`Window`, `WindowSpec`) and `over/1` support
- Expand GroupedData API beyond `agg` (`count/min/max/sum/avg/pivot` baseline)

Acceptance:

- Function/column parity test suite against canonical examples
- No hand-maintained 500-function drift (registry/codegen-based approach)

## Milestone 12 - Catalog API

Deliverables:

- New `SparkEx.Catalog` module covering all catalog proto operations
- Typed result structs for catalog/database/table/function/column metadata

Acceptance:

- Integration parity tests matching `test_parity_catalog.py` functional coverage

## Milestone 13 - DataFrame Parity Pack B (NA/Stat/Merge/UDF)

Deliverables:

- `na` and `stat` APIs as explicit modules/sub-APIs
- `merge_into` command surface
- UDF/UDTF registration APIs (minimum: Java + inline registration protocol)
- TVF entrypoints

Acceptance:

- End-to-end tests for NA/stat/merge/UDF basic flows on real cluster

## Milestone 14 - Streaming Parity Baseline

Deliverables:

- DataStreamReader/DataStreamWriter minimal API
- StreamingQuery and StreamingQueryManager controls
- Listener bus wiring for progress/termination events

Acceptance:

- Start/await/stop/query-status integration tests
- Listener event flow validated

## 7. Testing Strategy for Parity Claims

Use PySpark Connect test organization as the mapping template.

Suite mapping:

- DataFrame/Column/Functions: `test_parity_dataframe.py`, `test_parity_column.py`, `test_parity_functions.py`
- Reader/Writer: `test_parity_readwriter.py`, `test_parity_datasources.py`
- Catalog: `test_parity_catalog.py`
- Types/serde/literals: `test_parity_types.py`, `test_parity_serde.py`
- UDF/UDTF: `test_parity_udf.py`, `test_parity_udtf.py`
- Streaming: `streaming/test_parity_streaming.py`, `streaming/test_parity_listener.py`
- Client artifacts/retry/reattach: `client/test_artifact.py`, `client/test_reattach.py`, `client/test_client_retries.py`

Execution model:

- Keep existing unit tests and integration smoke tests
- Add parity integration suites grouped by parity profile
- For each parity case, compare SparkEx output to expected golden result or equivalent PySpark Connect output on the same cluster and dataset
- Run negative tests for invalid expressions/types/options and verify structured errors
- Run concurrency tests for concurrent operations within a single session and across cloned sessions
- Run large-result tests for Arrow chunking and reassembly behavior
- Add encoder regression tests:
  - golden protobuf snapshots for representative relation/expression/command trees
  - proto encode/decode roundtrip checks to detect serialization drift
- Add property-style roundtrip checks where practical (encoding semantics and deterministic output checks)
- Cross-version compatibility: Spark 4.1.1 is the only parity baseline for V1.1; Spark 3.x compatibility is out of scope unless explicitly added
- Add error-parity tests that assert surfaced `FetchErrorDetails` fields:
  - `error_class`
  - `sql_state`
  - `query_contexts`
  - message parameter map used for rendered diagnostics

Pass criteria per mapped suite:

- A mapped suite is considered passing when all included parity scenarios pass and all explicitly unsupported scenarios are listed in the profile exception list
- New unsupported scenarios discovered during implementation must be added to profile documentation before release

## 8. Definition of Done (Parity-Oriented)

`spark_ex` may declare parity profile completion only when:

- Public API surface for that profile is implemented
- Required relation/expression/command/RPC coverage for that profile is implemented
- Corresponding integration parity suites pass on Spark 4.1.1
- Unsupported features are explicitly documented with rationale
- No silent type degradation in tested paths without explicit documentation
- Coverage metrics are generated by the reproducible parity-audit task against pinned source revisions
- SQL, auth transport, and execute-stream invariants are documented as explicit public contracts and validated by parity tests
- Error surface includes validated `FetchErrorDetails` fields for supported failure classes
- Proto pin/regeneration workflow is documented and reproducible from the repository

## 9. Immediate Next Priorities

1. Milestones 7-9 (`Protocol`, `Local Data/Artifacts`, `Command Encoder + Writer`) are the structural blockers.
2. Start function-registry/codegen work in parallel with Milestone 11 to avoid manual drift on the long function tail.
3. Treat command-surface completeness and DataFrame high-use parity (Milestone 10) as the immediate user-visible credibility target.
