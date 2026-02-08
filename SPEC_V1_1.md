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

High-level gap counts (approximate, code-derived):

- Spark Connect `Relation` variants encoded in `spark_ex`: `15 / 59`
- `AnalyzePlan` modes exposed: `3 / 14`
- `Config` operations exposed: `2 / 7`
- Spark service RPCs missing in `spark_ex` client: `AddArtifacts`, `ArtifactStatus`
- `pyspark.sql.connect.functions.builtin` wrappers in `spark_ex`: `9 / 509`

## 4. Missing Feature Inventory

## 4.1 Protocol/RPC Coverage Gaps

Missing or partial RPC/API coverage:

- `AddArtifacts` RPC (artifact upload)
- `ArtifactStatus` RPC (artifact existence checks)
- `ExecutePlanRequest.ResultChunkingOptions.preferred_arrow_chunk_size` not configurable
- Request/user context extensions (`UserContext.extensions`) not exposed
- Full command execution surface via `Plan.command` is not implemented (current encoder is relation-focused)

## 4.2 AnalyzePlan and Config Gaps

Missing `AnalyzePlan` operations:

- `tree_string`, `is_local`, `is_streaming`, `input_files`
- `ddl_parse`, `json_to_ddl`
- `same_semantics`, `semantic_hash`
- `persist`, `unpersist`, `get_storage_level`

Missing config operations:

- `get_with_default`, `get_option`, `get_all`, `unset`, `is_modifiable`

## 4.3 DataFrame API Gaps (Batch)

Major missing DataFrame APIs vs PySpark Connect:

- Projection/shape: `selectExpr`, `toDF`, `alias`, `withColumnRenamed`, `withColumns`, `withColumnsRenamed`
- Row ops: `head`, `first`, `tail`, `offset`, `isEmpty`
- Set semantics: `unionByName`, `exceptAll`, `intersectAll`
- Partitioning: `repartition`, `repartitionByRange`, `coalesce`, `sortWithinPartitions`
- Sampling/statistical: `sample`, `randomSplit`
- Grouping features: `rollup`, `cube`, `pivot`, `groupingSets`
- Metadata/views: `createTempView`, `createOrReplaceTempView`, global temp view variants
- Cache/persistence: `cache`, `persist`, `unpersist`, `storageLevel`, `checkpoint/localCheckpoint`
- Semantics helpers: `sameSemantics`, `semanticHash`, `inputFiles`, `isLocal`, `isStreaming`
- NA/stat sub-APIs: `na.fill/drop/replace`, `describe/summary/corr/cov/crosstab/freqItems/approxQuantile/sampleBy`
- Advanced joins: `asOfJoin`, `lateralJoin`
- Advanced reshaping: `unpivot`, `transpose`
- Observation: `observe`

## 4.4 Column and Expression Gaps

Missing common `Column` capabilities:

- `eqNullSafe`, `isin`, `between`
- `endswith`, `rlike`, `ilike`, `substr`
- `getItem`, `getField`, `withField`, `dropFields`
- bitwise operators, modulo/pow, unary negative
- `try_cast`
- window binding (`over(window_spec)`)

Expression-level gaps in encoder:

- Window, lambda, update fields, regex extract, subquery expressions
- CallFunction / NamedArgumentExpression support
- Merge actions for MERGE INTO

## 4.5 Functions/Aggregations Gaps

Current `SparkEx.Functions` is minimal (`col/lit/expr` + a few aggs).

Missing parity area:

- Most scalar SQL functions
- Most aggregate/window functions
- Array/map/struct/higher-order helpers
- Null/date/time/string/binary/math/stat families

## 4.6 Reader/Writer Gaps

Reader gaps:

- Generic `format/load/option/options/schema`
- Additional formats: `text`, `orc`, `xml`, `avro`, `jdbc`

Writer gaps:

- DataFrameWriter: `format`, `mode`, `option(s)`, `save`, `saveAsTable`, `insertInto`, `partitionBy`, `bucketBy`, `sortBy`, `clusterBy`, file-format writers
- DataFrameWriterV2: `writeTo(...).using(...).option(s).tableProperty(...).partitionedBy(...).clusterBy(...).create/replace/createOrReplace/append/overwrite/overwritePartitions`

## 4.7 Catalog API Gaps

No `SparkEx.Catalog` parity module yet for:

- current/set catalog and database
- list/get/exists for catalogs/databases/tables/functions/columns
- createTable/createExternalTable
- drop temp views
- cache/uncache/clear/refresh/recover partitions

## 4.8 Artifact + Local Data Gaps

Missing critical parity path used heavily by PySpark Connect:

- Artifact manager for jars/pyfiles/files/archives/cached chunks
- `SparkSession.addArtifacts` equivalent
- `copyFromLocalToFs` equivalent
- Local relation creation path (`createDataFrame`) and chunked local relation caching protocol

## 4.9 UDF/UDTF/Data Source Gaps

Missing:

- `udf` registration APIs (Python/Java UDF registration surface)
- `udtf` registration and invocation
- Python data source registration hooks
- TVF ergonomics (`spark.tvf.*`)

Note:

- Python-runtime-specific execution modes (Pandas/Arrow Python workers) require explicit cross-runtime strategy in Elixir and are a separate complexity tier.

## 4.10 Streaming Gaps

Missing full structured streaming parity:

- `readStream` / `DataStreamReader`
- `writeStream` / `DataStreamWriter`
- `StreamingQuery`, `StreamingQueryManager`, listener bus
- `foreach`/`foreachBatch` protocol bindings

## 4.11 Type and Structure Mapping Gaps

Gaps for parity-grade type fidelity:

- Decimal round-trip as typed decimal (currently fallback to string in Explorer mapping)
- Interval types fidelity
- Complex/nested map/array/struct fidelity in `to_explorer/2` display path
- Geography/Geometry/Variant validation coverage
- End-to-end parity tests for all Spark `DataType` variants, not only decode success

## 5. Parity Profiles

To avoid false parity claims, use explicit profiles:

- `Batch Core Parity`: DataFrame + Column + Reader/Writer + Catalog + core functions
- `Batch Advanced Parity`: UDF/UDTF/TVF + full stats/NA + local relation/artifacts
- `Streaming Parity`: DataStreamReader/Writer + query lifecycle + listeners
- `Python Worker Parity` (optional): pandas/pyarrow Python-worker execution behaviors

`spark_ex` should not claim “full PySpark parity” until the first three profiles pass.

## 6. Implementation Plan (V1.1 Milestones)

## Milestone 7 - Protocol Completion Foundation

Deliverables:

- Add RPC support for `AddArtifacts` and `ArtifactStatus`
- Expand `AnalyzePlan` + `Config` APIs to full proto operation set
- Add configurable execute request options (`preferred_arrow_chunk_size`, chunking toggle)

Acceptance:

- Integration tests for every Analyze/Config operation against real Spark cluster
- Artifact status check passes for uploaded and missing files

## Milestone 8 - Local Data + Artifact Manager

Deliverables:

- `SparkEx.Session.add_artifacts/2` and helpers for file/jar/archive categories
- `copy_from_local_to_fs/3`
- `SparkEx.create_dataframe/3` initial local relation support (Explorer + list/map inputs)
- Chunked local relation cache upload path

Acceptance:

- Create DataFrame from local Elixir data and execute SQL/filter/join server-side
- Chunked upload path exercised with payloads exceeding single-message limits

## Milestone 9 - Reader/Writer + Command Encoder

Deliverables:

- Introduce `SparkEx.Connect.CommandEncoder`
- Implement DataFrameWriter and DataFrameWriterV2 API subset to match PySpark signatures
- Extend Reader to generic `format/load/option(s)/schema` and more formats

Acceptance:

- Roundtrip tests: read -> transform -> write -> read-back equality
- Save modes and table writes verified in integration tests

## Milestone 10 - DataFrame Parity Pack A (High Use)

Deliverables:

- `select_expr`, rename APIs, `union_by_name`, `except_all`, `intersect_all`
- repartition/coalesce/sortWithinPartitions/sample/randomSplit
- temp/global view creation APIs
- semantic/hash/local/streaming/inputFiles introspection APIs

Acceptance:

- Behavior parity tests mirrored from PySpark connect dataframe/readwriter parity suites

## Milestone 11 - Column + Functions Expansion

Deliverables:

- Expand `SparkEx.Column` operators and predicates to common PySpark set
- Generate large portion of `SparkEx.Functions` wrappers from a declarative function registry
- Add window-spec scaffolding for `over/1`

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

- `na` and `stat` APIs
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

Use PySpark Connect test organization as coverage template:

- DataFrame/Column/Functions: `test_parity_dataframe.py`, `test_parity_column.py`, `test_parity_functions.py`
- Reader/Writer: `test_parity_readwriter.py`, `test_parity_datasources.py`
- Catalog: `test_parity_catalog.py`
- Types/serde: `test_parity_types.py`, `test_parity_serde.py`
- UDF/UDTF: `test_parity_udf.py`, `test_parity_udtf.py`
- Streaming: `streaming/test_parity_streaming.py`
- Client artifacts/retry/reattach: `client/test_artifact.py`, `client/test_reattach.py`

Execution model:

- Keep existing unit tests
- Add parity integration suites grouped by profile
- For each parity case, compare SparkEx output to either:
  - expected golden result, or
  - equivalent PySpark Connect query result against the same cluster/test data

## 8. Definition of Done (Parity-Oriented)

`spark_ex` may declare parity profile completion only when:

- Public API surface for that profile is implemented
- Corresponding integration parity suites pass on Spark 4.1.1
- Unsupported features are explicitly documented with rationale
- No silent type degradation in tested paths without explicit documentation

## 9. Immediate Next Priorities

1. Milestone 7 (protocol completion) and Milestone 8 (artifact + local relation) are the highest leverage blockers.
2. Milestone 11 should start early with a registry/codegen strategy to avoid manual function drift.
3. Catalog and writer support (Milestones 9 and 12) are required before claiming broad batch parity.
