# SparkEx

Elixir client for Apache Spark via the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) protocol (gRPC).

Targets Spark 4.1.1. See `SPEC_V1.md` for the full design.

## Status

Milestone 0 (Foundations) and Milestone 1 (Minimal Query Path) are complete:

- Generated protobuf + gRPC stubs from vendored Spark Connect protos (v4.1.1)
- `sc://` URI parsing (`SparkEx.Connect.Channel`)
- Session GenServer with plan ID counter and server-side session ID tracking (`SparkEx.Session`)
- `SparkEx.sql/3` &mdash; create DataFrames from SQL queries (with positional/named args)
- `SparkEx.range/3` &mdash; create DataFrames from integer ranges
- `DataFrame.collect/2`, `count/1`, `take/3` &mdash; execute plans and return rows
- `DataFrame.schema/1`, `explain/2` &mdash; analyze plans via `AnalyzePlan` RPC
- `SparkEx.config_set/2`, `config_get/2` &mdash; Spark configuration management
- Arrow IPC stream decoding via Explorer
- Structured error handling with `FetchErrorDetails` RPC (`SparkEx.Error.Remote`)

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

## Prerequisites

- Elixir >= 1.19
- `protoc` with `protoc-gen-elixir` (for proto regeneration only)
- Java 17 or 21 (for running the Spark Connect server; Spark 4.1.1 does **not** support Java 25+)
- [Explorer](https://hex.pm/packages/explorer) (optional, for Arrow IPC decoding &mdash; included as optional dep)

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
    data_frame.ex                  # Lazy DataFrame struct (collect, count, take, schema, explain)
    connect/
      channel.ex                   # sc:// URI parser + gRPC channel management
      client.ex                    # Low-level gRPC RPC calls
      plan_encoder.ex              # Internal plan -> Spark Connect protobuf encoding
      result_decoder.ex            # Arrow IPC stream decoding (via Explorer)
      errors.ex                    # Structured error handling (FetchErrorDetails RPC)
    proto/spark/connect/*.pb.ex    # Generated protobuf modules (do not edit)

priv/
  proto/spark/connect/*.proto      # Vendored Spark Connect protos (v4.1.1)
  scripts/gen_proto.sh             # Proto generation script

test/
  unit/                            # Unit tests (no server needed)
  integration/                     # Integration tests (tagged :integration)
  spark-4.1.1-bin-hadoop3-connect/ # Spark distribution (download separately, gitignored)
  run_integration.sh               # One-command integration test runner
```
