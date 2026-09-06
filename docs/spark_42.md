# Spark 4.2 APIs and compatibility

SparkEx retains Spark 3.5–4.2 support. New SQL functions, relations, and RPCs
require a server that implements them. SparkEx preserves the server's errors;
it does not probe the version before each call.

## DataFrames and schemas

`group_by`, `rollup`, `cube`, and `grouping_sets` accept 1-based column ordinals.
Ordinals require a schema lookup; zero, negative, and out-of-range positions are
errors. String names remain lazy and unbound, while explicit DataFrame columns
retain their originating relation. Explicit numeric aggregation names can refer
to nested fields; quote literal dots with backticks.

`DataFrame.zip_with_index/2` appends consecutive zero-based long indices using
Spark's distributed sequence expression. It is tested on Spark 4.1.1 and 4.2.0.
Assignments are not guaranteed stable after repartitioning or recomputation.
`SparkEx.empty_dataframe/2` accepts an explicit DDL or Types struct schema.

Zero columns do not imply zero rows. Local lists of empty maps, tuples, or lists
preserve their length; collection and iteration return that many empty maps.
Arrow results retain their cardinality, and single-batch Explorer results keep
their native height. When Explorer's zero-column batch concatenation would drop
rows, `to_explorer` returns `{:unsupported_zero_column_explorer, %{row_count: n}}`
inside an error tuple. Use row collection or raw Arrow for those results.

`Reader.json/csv/xml` accept a DataFrame containing one string column, as well as
their existing file inputs. Reader options and schemas carry through to Parse;
input and reader must belong to the same session. DDL normalization uses a
schema-only analysis RPC, and does not execute the input data. Typed schemas
preserve field metadata. Input shape and malformed records are checked by Spark.

## Catalog

Catalog DDL helpers use SQL by default for older-server compatibility. Pass
`backend: :catalog` to use the Spark 4.2 structured protocol. New helpers include
`drop_view`, `list_views`, `list_partitions`, `get_table_properties`,
`get_create_table_string`, `analyze_table`, and `truncate_table`.

Native create-database requests cannot represent location or comment options;
use the default SQL backend for those. List views returns existing `Table`
records, partitions return `TablePartition` records, properties return a string
map, and void actions return `:ok`.

## Provider-dependent features

`Reader.changes/2` and `StreamReader.changes/2` require a Spark 4.2 table provider
with change-feed support. They preserve reader options such as
`startingVersion`, `endingVersion`, `startingTimestamp`, `endingTimestamp`,
`startingBoundInclusive`, `endingBoundInclusive`, `deduplicationMode`, and
`computeUpdates`. The provider defines the schema; a reader schema is rejected.

`StreamReader.name/2`, `StreamWriter.trigger(real_time: duration)`, and
`Writer.with_schema_evolution/2` / `WriterV2.with_schema_evolution/2` use new wire
fields. **Enabling them asserts that the caller has selected a compatible Spark
4.2 server and provider.** Older servers may ignore unknown fields; these APIs
do not promise a client-side version check or safe downgrade. A source name is a
checkpoint identity, separate from a query name; enable
`spark.sql.streaming.queryEvolution.enableSourceEvolution` in that session.
Real-time mode requires a
compatible source and sink. Schema evolution is not the provider's `mergeSchema`
option, and compatible servers reject unsupported V1/create/replace operations.

## Operation status and uploads

`SparkEx.get_operation_statuses(session, ids, opts)` is experimental and requires
Spark 4.2. Empty IDs request all operations. It returns the complete
`Spark.Connect.GetStatusResponse`, including extension messages and unknown
numeric state values, without consuming or releasing result streams. Status and
interrupt RPCs can run while the session is executing a blocking query.
The RPC inspects an existing server session; a connection that has not yet made
an ordinary RPC can return `SESSION_NOT_FOUND`.

Cached local relations serialize and upload incrementally. The cache threshold
now uses estimated native DataFrame size, following PySpark's table-size policy,
instead of first serializing the entire input. Only the current upload batch,
ordered hashes, and deduplication identities are retained beyond each chunk.
The native input itself remains resident. A single row or schema larger than a
batch budget is uploaded alone. The cumulative size limit counts schema bytes
and repeated chunks before deduplication. Earlier batches may already exist on
the server if a later chunk exceeds the limit; no incomplete DataFrame is returned.

## Server behavior

Spark 4.2 strict column resolution does not permit unrelated DataFrame origins.
The permissive configuration can resolve some shadowed original columns; it is
not enabled automatically. Empty grouping sets now produce the grand-total row
on empty input, unless the server's legacy configuration is explicitly selected.

Arrow field metadata may include `SPARK::metadata::json`. Spark schema metadata
remains authoritative when Explorer drops custom Arrow metadata. TIME tests must
enable `spark.sql.timeType.enabled`; geospatial SQL is enabled by default in 4.2.

Use SQL expressions for the new aggregate NULL treatments:

```elixir
DataFrame.agg(df, [Functions.expr("array_agg(value) RESPECT NULLS AS values")])
DataFrame.agg(df, [Functions.expr("collect_list(value) IGNORE NULLS AS values")])
```

The same clauses apply to `collect_set`. Aggregate element order is unspecified.
Binary gRPC metadata values ending in `-bin` are raw bytes; the installed adapter
performs base64 encoding once. Do not pre-encode those values.

## SQL vector functions

Spark 4.2 adds server-side vector SQL functions that have no PySpark wrapper:
`vector_cosine_similarity`, `vector_inner_product`, `vector_l2_distance`,
`vector_norm`, `vector_normalize`, `vector_avg`, and `vector_sum`. SparkEx exposes
them through `SparkEx.Functions`. Vectors must be `ARRAY<FLOAT>`; cast ordinary
numeric arrays explicitly, for example `expr("CAST(array(3.0, 4.0) AS ARRAY<FLOAT>)")`.
`vector_norm/1` and `vector_normalize/1` encode the default L2 degree as a FLOAT;
their optional literal degree must be an Elixir float, while a `Column` can supply
a dynamically typed degree. Spark evaluates all vector operations, including
dimension and degree errors.

## Nearest-by joins

`DataFrame.nearest_by_join(left, right, ranking, k, opts)` ranks right-hand rows
separately for each left-hand row. Both inputs must use the same session.
Specify `mode: :exact` or `:approx` and `direction: :distance` (smallest first)
or `:similarity` (largest first). `k` is between 1 and 100,000;
`join_type: :left_outer` retains left-hand rows without a match.

For vector search, build a ranking expression with
`Functions.vector_l2_distance(DataFrame.col(left, "embedding"),
DataFrame.col(right, "embedding"))`. Alias reused DataFrames to distinguish
their column references. Approximate execution depends on Spark's supported
ranking expressions and may return different matches from exact execution.
Ties do not promise stable ordering; exact mode also permits nondeterministic
ranking expressions.

## Client call-site diagnostics

Set `config :spark_ex, :debug_client_call_stack, true`, or the environment
variable `SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK=true`, to attach available
Elixir caller frames to ExecutePlan, AnalyzePlan, and Config requests.
This is disabled by default because the metadata includes local filenames and
line numbers. Frames are captured before the Session process boundary, with
SparkEx internals filtered out; BEAM tail-call optimization can remove frames.

The extension uses Spark's `FetchErrorDetailsResponse.Error` message and is
available to server instrumentation. Stock Spark does not automatically log
this extension. This option does not change server-side error stack traces.

Artifact helpers continue to accept native filesystem paths, with literal
spaces and percent characters. They do not decode `file://` URIs or expand
Windows drive/UNC paths using POSIX rules; native path interpretation belongs
to the host filesystem.
