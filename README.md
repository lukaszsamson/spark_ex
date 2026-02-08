# SparkEx

Elixir client for Apache Spark via the [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) protocol (gRPC).

Targets Spark 4.1.1. See `SPEC_V1.md` for the full design.

## Status

Milestone 0 (Foundations) is complete:

- Generated protobuf + gRPC stubs from vendored Spark Connect protos (v4.1.1)
- `sc://` URI parsing (`SparkEx.Connect.Channel`)
- Session GenServer with plan ID counter and server-side session ID tracking (`SparkEx.Session`)
- `AnalyzePlan.SparkVersion` RPC validates the full client -> gRPC -> Spark -> response path

## Quick start

```elixir
{:ok, session} = SparkEx.connect(url: "sc://localhost:15002")
{:ok, version} = SparkEx.spark_version(session)
# => {:ok, "4.1.1"}
```

## Prerequisites

- Elixir >= 1.19
- `protoc` with `protoc-gen-elixir` (for proto regeneration only)
- Java 17+ (for running the Spark Connect server)

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

- **Java 17+** &mdash; set `JAVA_HOME` or have Homebrew OpenJDK installed (`brew install openjdk`)
- **uv** &mdash; the `test/spark_server/` directory is a uv project with `pyspark-connect==4.1.1`

On first run, `uv` will create its venv automatically if `test/spark_server/.venv` doesn't exist yet. Set it up with:

```bash
cd test/spark_server
uv sync
```

#### Manual server + tests

Start the server yourself (in a separate terminal):

```bash
cd test/spark_server
uv run python start_connect_server.py
```

Then run the tests:

```bash
mix test --include integration
```

Or point at a remote server:

```bash
SPARK_REMOTE="sc://my-spark-host:15002" mix test --include integration
```

## Project layout

```
lib/
  spark_ex.ex                      # Public entrypoint (connect/1, spark_version/1)
  spark_ex/
    application.ex                 # OTP application (starts GRPC.Client.Supervisor)
    session.ex                     # Session GenServer
    connect/
      channel.ex                   # sc:// URI parser + gRPC channel management
      client.ex                    # Low-level gRPC RPC calls
    proto/spark/connect/*.pb.ex    # Generated protobuf modules (do not edit)

priv/
  proto/spark/connect/*.proto      # Vendored Spark Connect protos (v4.1.1)
  scripts/gen_proto.sh             # Proto generation script

test/
  unit/                            # Unit tests (no server needed)
  integration/                     # Integration tests (tagged :integration)
  spark_server/                    # uv project with pyspark-connect for local server
  run_integration.sh               # One-command integration test runner
```
