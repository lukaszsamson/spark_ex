# Spark 4.2 provider fixtures

These fixtures exercise CDC and schema evolution against Spark's published
4.2.0 test catalogs, supported real-time streaming, and cancellation of an
operation held Pending before its execution thread starts. Prepare the checksum-pinned JARs and the small seeded CDC
catalog outside the repository. Use a Java 17+ JDK (CI uses Java 17;
the example below uses Java 21):

```sh
export SPARK_HOME=/path/to/spark-4.2.0-bin-hadoop3
export JAVA_HOME=/path/to/jdk-21
providers=$(tools/prepare_spark42_test_providers.sh)
```

Launch an isolated Connect server with the returned comma-separated JAR list:

```sh
$SPARK_HOME/sbin/start-connect-server.sh \
  --master 'local[2]' \
  --jars "$providers" \
  --conf spark.connect.grpc.binding.port=15004 \
  --conf spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryTableCatalog \
  --conf spark.sql.catalog.testcat.copyOnLoad=true \
  --conf spark.sql.catalog.cdc_e2e=org.apache.spark.sql.connector.catalog.Spark42SeededChangelogCatalog \
  --conf spark.connect.extensions.command.classes=org.apache.spark.sql.connect.service.Spark42PendingOperationPlugin
```

Run the provider-gated tests:

```sh
SPARK_REMOTE=sc://localhost:15004 SPARK_EX_TEST_PROVIDERS=1 \
  mix test test/integration/spark42_p1_readwrite_test.exs \
    test/integration/spark42_real_time_test.exs \
    test/integration/spark42_pending_operation_test.exs
```

The real-time source is a finite three-row test provider implementing Spark's
`SupportsRealTimeMode` and `SupportsRealTimeRead` contracts. It rejects ordinary
micro-batch planning and uses the supported console sink with Update output
mode and a five-second real-time trigger. It verifies the Connect trigger path;
it does not establish support for arbitrary production sources or sinks.

The Pending-operation command plugin registers a real server execution holder
without starting its runner. Public GetStatus maps both Pending and Started to
RUNNING, so a status-only test cannot prove this case. The fixture asserts the
internal state, then tests public interruption, CANCELLED status, ReleaseExecute,
and registry removal. Load this test-only plugin solely on an isolated test
server; it deliberately exposes execution-manager controls to the tests.
