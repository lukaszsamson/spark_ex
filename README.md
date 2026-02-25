# SparkEx

[![CI](https://github.com/TODO/spark_ex/actions/workflows/ci.yml/badge.svg)](https://github.com/TODO/spark_ex/actions)

Native Elixir client for [Apache Spark](https://spark.apache.org/) via the
[Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) protocol.

SparkEx communicates with Spark over gRPC using the official Spark Connect
protobuf contract, giving Elixir and Livebook first-class access to
distributed Spark SQL, DataFrames, streaming, and the catalog &mdash; without
a JVM in your application.

Targets **Spark 3.4 - 4.1**.

## Features

- **Lazy DataFrame API** &mdash; select, filter, join, group, window, set ops,
  reshape, sampling, and more
- **900+ Spark SQL functions** &mdash; auto-generated from a declarative
  registry, covering math, string, date/time, array, map, conditional, window,
  and aggregate families
- **Reader/Writer** &mdash; Parquet, CSV, JSON, ORC, text, JDBC, plus generic
  `format`/`load`/`save` and V2 DataSource writes
- **Structured Streaming** &mdash; `DataStreamReader`, `DataStreamWriter`,
  `StreamingQuery`, and `StreamingQueryManager`
- **Catalog API** &mdash; browse and manage databases, tables, functions,
  columns, and cache
- **Explorer integration** &mdash; materialize Spark results as
  `Explorer.DataFrame` with automatic type mapping
- **Livebook / Kino** &mdash; DataFrames render as interactive tables with
  schema, preview, and explain tabs
- **NA/Stat sub-APIs** &mdash; `fillna`, `dropna`, `describe`, `corr`, `cov`,
  `crosstab`, `approx_quantile`, and more
- **MERGE INTO** &mdash; upsert builder with match/not-matched clauses and
  schema evolution
- **UDF/UDTF registration** &mdash; register Java UDFs and Python UDTFs
- **Session lifecycle** &mdash; clone, release, interrupt by tag/operation,
  reattachable execution with automatic mid-stream recovery
- **Observability** &mdash; Telemetry events for every RPC, retry, reattach,
  batch, and progress update

## Installation

Add `spark_ex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:spark_ex, "~> 0.1.0"},

    # Optional — for Arrow decoding and to_explorer/2
    {:explorer, "~> 0.10"},

    # Optional — for Livebook rendering
    {:kino, "~> 0.14"}
  ]
end
```

## Getting started

### Connect to Spark

```elixir
{:ok, session} = SparkEx.connect(url: "sc://localhost:15002")
{:ok, "4.1.1"} = SparkEx.spark_version(session)
```

### Create and query DataFrames

```elixir
import SparkEx.Functions
alias SparkEx.{DataFrame, Column}

df =
  SparkEx.sql(session, """
    SELECT * FROM VALUES
      (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Carol', 150)
    AS t(id, name, salary)
  """)

df
|> DataFrame.filter(Column.gt(col("salary"), lit(120)))
|> DataFrame.select([col("name"), col("salary")])
|> DataFrame.order_by([desc("salary")])
|> DataFrame.collect()
# => {:ok, [%{"name" => "Bob", "salary" => 200}, %{"name" => "Carol", "salary" => 150}]}
```

### Joins and aggregates

```elixir
employees = SparkEx.sql(session, """
  SELECT * FROM VALUES (1, 'eng'), (2, 'hr'), (3, 'eng') AS t(id, dept)
""")

departments = SparkEx.sql(session, """
  SELECT * FROM VALUES ('eng', 'Engineering'), ('hr', 'People') AS t(dept, name)
""")

employees
|> DataFrame.join(departments, ["dept"], :inner)
|> DataFrame.group_by(["name"])
|> SparkEx.GroupedData.agg([
  Column.alias_(count(col("id")), "headcount")
])
|> DataFrame.collect()
# => {:ok, [%{"name" => "Engineering", "headcount" => 2}, %{"name" => "People", "headcount" => 1}]}
```

### Window functions

```elixir
alias SparkEx.{Window, WindowSpec}

df = SparkEx.sql(session, """
  SELECT * FROM VALUES
    ('eng', 'Alice', 100), ('eng', 'Bob', 120), ('hr', 'Carol', 90)
  AS t(dept, name, salary)
""")

w =
  Window.partition_by(["dept"])
  |> WindowSpec.order_by([col("salary") |> Column.desc()])

df
|> DataFrame.with_column("rank", row_number() |> Column.over(w))
|> DataFrame.collect()
```

### Read and write data

```elixir
alias SparkEx.{Reader, Writer}

# Read
df = Reader.parquet(session, "/data/events.parquet")
df = Reader.csv(session, "/data/users.csv", schema: "name STRING, age INT")
df = Reader.json(session, "/data/logs.json")
df = Reader.table(session, "my_catalog.my_table")

# Write
df |> DataFrame.write() |> Writer.parquet("/output/events.parquet")
df |> DataFrame.write() |> Writer.mode("overwrite") |> Writer.save_as_table("results")
```

### Create DataFrames from local data

```elixir
# From a list of maps
df = SparkEx.create_dataframe(session, [
  %{"name" => "Alice", "age" => 30},
  %{"name" => "Bob", "age" => 25}
])

# From an Explorer.DataFrame
explorer_df = Explorer.DataFrame.new(%{x: [1, 2, 3], y: ["a", "b", "c"]})
df = SparkEx.create_dataframe(session, explorer_df)
```

### Explorer integration

```elixir
# Materialize as Explorer.DataFrame (bounded by default: 10k rows / 64 MB)
{:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 1_000)

# Unbounded (use with care)
{:ok, explorer_df} = DataFrame.to_explorer(df, unsafe: true, max_rows: :infinity)
```

### Structured streaming

```elixir
alias SparkEx.{StreamReader, StreamWriter, StreamingQuery}

# Start a streaming query
{:ok, query} =
  StreamReader.rate(session, rows_per_second: 10)
  |> DataFrame.write_stream()
  |> StreamWriter.format("memory")
  |> StreamWriter.output_mode("append")
  |> StreamWriter.query_name("my_stream")
  |> StreamWriter.option("checkpointLocation", "/tmp/checkpoint")
  |> StreamWriter.start()

# Monitor
{:ok, true} = StreamingQuery.is_active?(query)
{:ok, status} = StreamingQuery.status(query)

# Stop
:ok = StreamingQuery.stop(query)
```

### Catalog

```elixir
alias SparkEx.Catalog

{:ok, dbs}    = Catalog.list_databases(session)
{:ok, tables} = Catalog.list_tables(session)
{:ok, cols}   = Catalog.list_columns(session, "my_table")

:ok = Catalog.cache_table(session, "frequently_used")
{:ok, true} = Catalog.is_cached?(session, "frequently_used")
```

### Temporary views

```elixir
df = SparkEx.sql(session, "SELECT 1 AS id, 'hello' AS msg")
:ok = DataFrame.create_or_replace_temp_view(df, "greetings")

SparkEx.sql(session, "SELECT * FROM greetings") |> DataFrame.collect()
# => {:ok, [%{"id" => 1, "msg" => "hello"}]}
```

### NA handling and statistics

```elixir
df = SparkEx.sql(session, """
  SELECT * FROM VALUES (1, 10.0), (2, null), (null, 30.0) AS t(id, score)
""")

# Fill nulls
DataFrame.fillna(df, %{"id" => 0, "score" => 0.0})

# Drop rows with nulls
DataFrame.dropna(df)

# Descriptive statistics
DataFrame.describe(df) |> DataFrame.collect()
```

### MERGE INTO

```elixir
alias SparkEx.{MergeIntoWriter, Column}

source = SparkEx.sql(session, "SELECT 2 AS id, 'Bobby' AS name")

DataFrame.merge_into(source, "target_table")
|> MergeIntoWriter.on(Column.eq(col("source.id"), col("target.id")))
|> MergeIntoWriter.when_matched_update_all()
|> MergeIntoWriter.when_not_matched_insert_all()
|> MergeIntoWriter.merge()
```

### Session management

```elixir
# Tag operations for selective interruption
df = SparkEx.sql(session, "SELECT * FROM big_table") |> DataFrame.tag("etl-job")

# Interrupt from another process
SparkEx.interrupt_tag(session, "etl-job")
SparkEx.interrupt_all(session)

# Clone a session (shares server state, independent client)
{:ok, session2} = SparkEx.clone_session(session)

# Release server resources
SparkEx.Session.release(session)
```

### Livebook

In Livebook, `SparkEx.DataFrame` structs render automatically with tabs for
Schema, Preview, Explain, and Raw via the `Kino.Render` protocol.

```elixir
# Explicit rendering helpers
SparkEx.Livebook.preview(df, num_rows: 50)
SparkEx.Livebook.explain(df, mode: :extended)
SparkEx.Livebook.schema(df)
```

See [`notebooks/spark_ex_demo.livemd`](notebooks/spark_ex_demo.livemd) for a
full interactive walkthrough.

### Telemetry

SparkEx emits [`:telemetry`](https://hex.pm/packages/telemetry) events you can
attach to for logging, metrics, or tracing:

| Event                              | Description                     |
| ---------------------------------- | ------------------------------- |
| `[:spark_ex, :rpc, :start\|stop]`  | Every gRPC call                 |
| `[:spark_ex, :retry, :attempt]`    | Transient-error retry           |
| `[:spark_ex, :reattach, :attempt]` | Mid-stream reattach             |
| `[:spark_ex, :result, :batch]`     | Each Arrow batch received       |
| `[:spark_ex, :result, :progress]`  | Query progress update           |

## Prerequisites

| Requirement         | Notes                                                                 |
| ------------------- | --------------------------------------------------------------------- |
| Elixir >= 1.15      |                                                                       |
| Java 17 or 21       | For running the Spark Connect server                                  |
| Spark >= 3.4 with spark-connect       | For running the Spark Connect server                |
| Explorer (optional) | Arrow IPC decoding and `to_explorer/2`                                |
| Kino (optional)     | Livebook rendering                                                    |

`protoc` + `protoc-gen-elixir` are only needed if you regenerate the protobuf
stubs (see [Proto regeneration](#proto-regeneration) below).

## Running tests

### Unit tests

```bash
mix test
```

### Integration tests

Integration tests require a running Spark Connect server and are excluded by
default.

**One-command runner** (starts server, runs tests, tears down):

```bash
./test/run_integration.sh
```

**Manual setup:**

1. Download Spark:

```bash
curl -L -o /tmp/spark.tgz \
  'https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3-connect.tgz'
tar -xzf /tmp/spark.tgz -C test/
```

2. Start the server:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
export SPARK_HOME="test/spark-4.1.1-bin-hadoop3-connect"
bash "$SPARK_HOME/sbin/start-connect-server.sh"
```

3. Run:

```bash
mix test --include integration
```

4. Or point at a remote server:

```bash
SPARK_REMOTE="sc://my-spark-host:15002" mix test --include integration
```

## Proto regeneration

The vendored `.proto` files in `priv/proto/spark/connect/` and the generated
Elixir modules in `lib/spark_ex/proto/` are checked in. You do **not** need
`protoc` for normal development.

To update protos from a new Spark release:

```bash
# Copy protos from Spark source
git clone --depth 1 --branch v4.1.1 https://github.com/apache/spark.git /tmp/spark
cp /tmp/spark/sql/connect/common/src/main/protobuf/spark/connect/*.proto \
   priv/proto/spark/connect/

# Regenerate Elixir stubs
mix escript.install hex protobuf   # one-time
mix spark_ex.gen_proto
mix format
```

## Project layout

```
lib/
  spark_ex.ex                     # Public API entry point
  spark_ex/
    session.ex                    # Session GenServer
    data_frame.ex                 # Lazy DataFrame API
    data_frame/na.ex              # Null-value handling
    data_frame/stat.ex            # Statistical operations
    column.ex                     # Expression wrapper
    functions.ex                  # 900+ auto-generated Spark SQL functions
    grouped_data.ex               # GroupedData (group_by + agg + pivot)
    window.ex                     # Window convenience constructors
    window_spec.ex                # WindowSpec (partition, order, frame)
    reader.ex                     # Batch readers (parquet, csv, json, etc.)
    writer.ex                     # Batch writer (V1)
    writer_v2.ex                  # V2 DataSource writer
    stream_reader.ex              # Streaming source reader
    stream_writer.ex              # Streaming sink writer
    streaming_query.ex            # StreamingQuery controls
    streaming_query_manager.ex    # Manage active streaming queries
    catalog.ex                    # Catalog API
    merge_into_writer.ex          # MERGE INTO builder
    udf_registration.ex           # UDF/UDTF registration
    livebook.ex                   # Livebook/Kino helpers
    connect/
      channel.ex                  # sc:// URI parser + gRPC channel
      client.ex                   # Low-level gRPC calls (telemetry + retry)
      plan_encoder.ex             # DataFrame ops -> protobuf encoding
      command_encoder.ex          # Commands -> protobuf encoding
      result_decoder.ex           # Arrow IPC decoding
      type_mapper.ex              # Spark DataType <-> Explorer dtype
    proto/spark/connect/*.pb.ex   # Generated protobuf modules

priv/proto/spark/connect/         # Vendored Spark Connect protos (v4.1.1)
notebooks/spark_ex_demo.livemd    # Interactive Livebook demo
test/unit/                        # Unit tests (890 tests)
test/integration/                 # Integration tests (260 tests)
```

## Acknowledgements

SparkEx builds on the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
protocol introduced in Apache Spark 3.4 and stabilized in Spark 4.x.

## License

Copyright (c) 2026 Łukasz Samson

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
