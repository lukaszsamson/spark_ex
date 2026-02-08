# SparkEx

Elixir client for Apache Spark via the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) protocol (gRPC).

Targets Spark 4.1.1. See `SPEC_V1.md` for the full design.

## Status

Milestones 0 through 6 are complete (355 tests passing against Spark 4.1.1).

### Milestone 0 &mdash; Foundations

- Generated protobuf + gRPC stubs from vendored Spark Connect protos (v4.1.1)
- `sc://` URI parsing (`SparkEx.Connect.Channel`)
- Session GenServer with plan ID counter and server-side session ID tracking (`SparkEx.Session`)

### Milestone 1 &mdash; Minimal Query Path

- `SparkEx.sql/3` &mdash; create DataFrames from SQL queries (with positional/named args)
- `SparkEx.range/3` &mdash; create DataFrames from integer ranges
- `DataFrame.collect/2`, `count/1`, `take/3` &mdash; execute plans and return rows
- `DataFrame.schema/1`, `explain/2` &mdash; analyze plans via `AnalyzePlan` RPC
- `SparkEx.config_set/2`, `config_get/2` &mdash; Spark configuration management
- Arrow IPC stream decoding via Explorer
- Structured error handling with `FetchErrorDetails` RPC (`SparkEx.Error.Remote`)

### Milestone 2 &mdash; Core Transforms + Expressions

- `DataFrame.select/2`, `filter/2`, `with_column/3`, `drop/2`, `order_by/2`, `limit/2` &mdash; lazy transforms
- `DataFrame.show/2` &mdash; formatted string output (like PySpark's `show()`)
- `SparkEx.Column` &mdash; expression wrapper with comparisons, boolean ops, null checks, string ops (`contains`, `starts_with`, `like`), arithmetic, `cast`, sort ordering, `alias_`
- `SparkEx.Functions` &mdash; `col/1`, `lit/1`, `expr/1`, `star/0`, `asc/1`, `desc/1`, `count/1`, `sum/1`, `avg/1`, `min/1`, `max/1`, `count_distinct/1`
- `SparkEx.Reader` &mdash; `table/2`, `parquet/2`, `csv/2`, `json/2`

### Milestone 3 &mdash; Joins, Aggregates, Set Operations

- `DataFrame.group_by/2` + `SparkEx.GroupedData.agg/2` &mdash; group-by aggregation
- `DataFrame.join/4` &mdash; inner, left, right, full, cross, left_semi, left_anti joins (with condition or USING columns)
- `DataFrame.distinct/1` &mdash; deduplicate rows
- `DataFrame.union/2`, `union_distinct/2`, `intersect/2`, `except/2` &mdash; set operations
- Retry with exponential backoff + jitter for transient gRPC errors (UNAVAILABLE, DEADLINE_EXCEEDED)
- Same-session guards for join and set operations
- Join type normalization (`:outer` &rarr; `:full`, `:semi` &rarr; `:left_semi`, string types, etc.)

### Milestone 4 &mdash; Explorer + Kino + Observability

- `DataFrame.to_explorer/2` &mdash; materialize as `Explorer.DataFrame` with bounded row/byte limits (default: 10,000 rows / 64 MB); injects LIMIT unless `unsafe: true` (which skips LIMIT injection only)
- `SparkEx.Connect.TypeMapper` &mdash; maps all 28 Spark `DataType` proto variants to Explorer dtypes
- `SparkEx.Error.LimitExceeded` &mdash; structured error for exceeded materialization bounds with remediation guidance
- `SparkEx.Livebook` &mdash; helpers returning Kino terms: `preview/2`, `explain/2`, `sample/2`, `schema/1`
- `Kino.Render` protocol for `SparkEx.DataFrame` &mdash; tabs: Schema, Preview (`Kino.DataTable`), Explain, Raw
- Telemetry events: `[:spark_ex, :rpc, :start/stop/exception]`, `[:spark_ex, :result, :batch]`, `[:spark_ex, :result, :progress]`, `[:spark_ex, :retry, :attempt]`
- Arrow batch chunking reassembly with Explorer-native decode path (`Explorer.DataFrame.concat_rows/1`)

### Milestone 5 &mdash; Session Lifecycle + Interrupts

- `SparkEx.Session.release/1` &mdash; calls `ReleaseSession` RPC, marks session as released, rejects further RPCs with `{:error, :session_released}`
- `SparkEx.Session.stop/1` &mdash; calls `ReleaseSession` before disconnecting the gRPC channel
- `SparkEx.interrupt_all/1` &mdash; interrupt all running operations via `Interrupt` RPC
- `SparkEx.interrupt_tag/2` &mdash; interrupt operations matching a tag
- `SparkEx.interrupt_operation/2` &mdash; interrupt a specific operation by ID
- `DataFrame.tag/2` &mdash; attach operation tags to DataFrames (propagated to `ExecutePlanRequest.tags`)
- Telemetry spans for `release_session` and `interrupt` RPCs

### Milestone 6 &mdash; Reattachable Execute

- Reattachable execution enabled by default &mdash; `ExecutePlanRequest` includes `operation_id` and `ReattachOptions{reattachable: true}`
- Mid-stream disconnect recovery via `ReattachExecute` RPC with `last_response_id` tracking
- Automatic `ReleaseExecute` RPC after successful stream consumption to free server-side cached results
- Exponential backoff with jitter between reattach attempts (reuses retry parameters)
- Opt-out via `reattachable: false` option (falls back to simple retry-from-scratch)
- Telemetry events: `[:spark_ex, :reattach, :attempt]`, `[:spark_ex, :rpc, :start/stop]` for `release_execute`

## Quick start

```elixir
{:ok, session} = SparkEx.connect(url: "sc://localhost:15002")

# Check connectivity
{:ok, version} = SparkEx.spark_version(session)
# => {:ok, "4.1.1"}

# SQL queries
df = SparkEx.sql(session, "SELECT 1 AS n, 'hello' AS greeting")
{:ok, rows} = SparkEx.DataFrame.collect(df)
# => {:ok, [%{"n" => 1, "greeting" => "hello"}]}

# Range DataFrames
df = SparkEx.range(session, 100, start: 10, step: 5)
{:ok, 18} = SparkEx.DataFrame.count(df)
{:ok, first_3} = SparkEx.DataFrame.take(df, 3)

# Schema and explain
{:ok, schema} = SparkEx.DataFrame.schema(df)
{:ok, plan_str} = SparkEx.DataFrame.explain(df, :extended)

# Spark configuration
:ok = SparkEx.config_set(session, [{"spark.sql.shuffle.partitions", "10"}])
{:ok, [{"spark.sql.shuffle.partitions", "10"}]} =
  SparkEx.config_get(session, ["spark.sql.shuffle.partitions"])

# SQL with arguments
df = SparkEx.sql(session, "SELECT * FROM t WHERE id = :id", args: %{id: 42})
df = SparkEx.sql(session, "SELECT * FROM t WHERE id = ?", args: [42])
```

### Transforms and expressions

```elixir
import SparkEx.Functions
alias SparkEx.{DataFrame, Column}

df =
  SparkEx.sql(session, "SELECT * FROM VALUES (1,'Alice',100), (2,'Bob',200) AS t(id,name,salary)")
  |> DataFrame.select([col("name"), col("salary")])
  |> DataFrame.filter(col("salary") |> Column.gt(lit(150)))
  |> DataFrame.with_column("bonus", col("salary") |> Column.multiply(lit(0.1)))
  |> DataFrame.order_by([col("name") |> Column.asc()])
  |> DataFrame.limit(10)

{:ok, rows} = DataFrame.collect(df)
```

### Joins and aggregates

```elixir
employees = SparkEx.sql(session, "SELECT * FROM VALUES (1,'eng'), (2,'hr') AS t(id,dept)")
departments = SparkEx.sql(session, "SELECT * FROM VALUES ('eng','Engineering') AS t(dept,name)")

df =
  DataFrame.join(employees, departments, ["dept"], :inner)
  |> DataFrame.group_by(["name"])
  |> SparkEx.GroupedData.agg([
    Column.alias_(count(col("id")), "headcount")
  ])

{:ok, rows} = DataFrame.collect(df)
```

### Data sources

```elixir
alias SparkEx.Reader

df = Reader.parquet(session, "/path/to/data.parquet")
df = Reader.csv(session, "/path/to/data.csv", schema: "name STRING, age INT")
df = Reader.json(session, "/path/to/data.json")
df = Reader.table(session, "my_table")
```

### Explorer integration

```elixir
# Materialize as Explorer.DataFrame (bounded by default)
{:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 1_000)

# Skip limit injection (decoder bounds still apply)
{:ok, explorer_df} = DataFrame.to_explorer(df, unsafe: true)

# Fully unbounded collection (use carefully)
{:ok, explorer_df} = DataFrame.to_explorer(df, unsafe: true, max_rows: :infinity, max_bytes: :infinity)
```

### Livebook

In a Livebook cell, `SparkEx.DataFrame` structs render automatically with tabs for Schema, Preview, Explain, and Raw via the `Kino.Render` protocol.

```elixir
# Helpers return Kino terms for explicit rendering
SparkEx.Livebook.preview(df, num_rows: 50)
SparkEx.Livebook.explain(df, mode: :extended)
SparkEx.Livebook.schema(df)
```

See [`notebooks/spark_ex_demo.livemd`](notebooks/spark_ex_demo.livemd) for a full interactive demo.

### Session lifecycle and interrupts

```elixir
# Tag operations for interrupt targeting
df = SparkEx.sql(session, "SELECT * FROM big_table")
|> DataFrame.tag("etl-pipeline")

# From another process: interrupt by tag
SparkEx.interrupt_tag(session, "etl-pipeline")

# Interrupt all running operations
SparkEx.interrupt_all(session)

# Release server-side session (rejects further RPCs)
SparkEx.Session.release(session)

# Stop the session process (calls ReleaseSession + disconnects)
SparkEx.Session.stop(session)
```

## Prerequisites

- Elixir >= 1.19
- `protoc` with `protoc-gen-elixir` (for proto regeneration only)
- Java 17 or 21 (for running the Spark Connect server; Spark 4.1.1 does **not** support Java 25+)
- [Explorer](https://hex.pm/packages/explorer) (optional, for Arrow IPC decoding and `to_explorer/2`)
- [Kino](https://hex.pm/packages/kino) (optional, for Livebook rendering and helpers)

## Setup

```bash
mix deps.get
mix compile
```

### Proto files and code generation

The vendored `.proto` files in `priv/proto/spark/connect/` are the source of truth for the Spark Connect protocol. The generated Elixir modules in `lib/spark_ex/proto/` are checked in so that `protoc` is not needed for normal development.

#### Updating protos from Spark source

Clone the Spark repository at the target tag and copy the proto files:

```bash
git clone --depth 1 --branch v4.1.1 https://github.com/apache/spark.git /tmp/spark
cp /tmp/spark/sql/connect/common/src/main/protobuf/spark/connect/*.proto \
   priv/proto/spark/connect/
```

The proto files live under `sql/connect/common/src/main/protobuf/spark/connect/` in the Spark tree. All 11 files must be copied &mdash; they import each other transitively. Google well-known protos (`google/protobuf/any.proto`, etc.) are resolved from the system `protoc` include path.

#### Regenerating Elixir stubs

After updating the vendored protos, regenerate the Elixir modules:

```bash
# one-time: install the protoc plugin
mix escript.install hex protobuf

# requires: protoc (brew install protobuf)
# requires: protoc-gen-elixir on PATH (~/.mix/escripts/)
mix spark_ex.gen_proto
mix format
```

This runs `protoc --elixir_out=plugins=grpc:...` which produces:

- **Protobuf message modules** (`Spark.Connect.Relation`, `Spark.Connect.Expression`, etc.)
- **gRPC service + stub** (`Spark.Connect.SparkConnectService.Service` and `.Stub`)

The `plugins=grpc` flag tells `protoc-gen-elixir` to also emit the `GRPC.Service` and `GRPC.Stub` modules from the `service SparkConnectService` definition in `base.proto`.

See `priv/scripts/gen_proto.sh` for the exact `protoc` invocation.

## Tests

### Unit tests

```bash
mix test
```

### Integration tests

Integration tests connect to a real Spark Connect server. They are excluded by default.

#### One-command runner

The included script starts a local Spark server, runs the tests, and tears it down:

```bash
./test/run_integration.sh
```

This requires:

- **Java 17 or 21** &mdash; set `JAVA_HOME` or have Homebrew OpenJDK installed (`brew install openjdk@21`)
- **Spark 4.1.1 distribution** &mdash; download and extract the official tarball:

```bash
curl -L -o /tmp/spark.tgz \
  'https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3-connect.tgz'
tar -xzf /tmp/spark.tgz -C test/
```

The script uses `sbin/start-connect-server.sh` and `sbin/stop-connect-server.sh` from the official distribution to manage the daemon.

#### Manual server + tests

Start the server yourself (in a separate terminal):

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
export SPARK_HOME="test/spark-4.1.1-bin-hadoop3-connect"
bash "$SPARK_HOME/sbin/start-connect-server.sh"
```

Then run the tests:

```bash
mix test --include integration
```

Stop the server when done:

```bash
bash "$SPARK_HOME/sbin/stop-connect-server.sh"
```

Or point at a remote server:

```bash
SPARK_REMOTE="sc://my-spark-host:15002" mix test --include integration
```

## Project layout

```
lib/
  spark_ex.ex                      # Public API (connect, sql, range, config)
  spark_ex/
    application.ex                 # OTP application (starts GRPC.Client.Supervisor)
    session.ex                     # Session GenServer
    data_frame.ex                  # Lazy DataFrame struct + actions
    column.ex                      # Expression wrapper (comparisons, arithmetic, etc.)
    functions.ex                   # Column constructors and aggregate functions
    grouped_data.ex                # GroupedData struct (group_by + agg)
    reader.ex                      # Data source readers (parquet, csv, json, table)
    livebook.ex                    # Livebook helpers (preview, explain, sample, schema)
    kino_render.ex                 # Kino.Render protocol for DataFrame
    connect/
      channel.ex                   # sc:// URI parser + gRPC channel management
      client.ex                    # Low-level gRPC RPC calls (with telemetry + retry)
      plan_encoder.ex              # Internal plan -> Spark Connect protobuf encoding
      result_decoder.ex            # Arrow IPC stream decoding (rows + Explorer modes)
      errors.ex                    # Structured errors (Remote, LimitExceeded)
      type_mapper.ex               # Spark DataType -> Explorer dtype mapping
    proto/spark/connect/*.pb.ex    # Generated protobuf modules (do not edit)

priv/
  proto/spark/connect/*.proto      # Vendored Spark Connect protos (v4.1.1)
  scripts/gen_proto.sh             # Proto generation script

notebooks/
  spark_ex_demo.livemd             # Interactive Livebook demo (Kino.Render, preview, etc.)

test/
  unit/                            # Unit tests (no server needed, 263 tests)
  integration/                     # Integration tests (tagged :integration, 92 tests)
  spark-4.1.1-bin-hadoop3-connect/ # Spark distribution (download separately, gitignored)
  run_integration.sh               # One-command integration test runner
```
