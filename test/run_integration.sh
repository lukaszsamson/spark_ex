#!/usr/bin/env bash
# Starts a local Spark Connect server and runs SparkEx integration tests.
#
# Usage: ./test/run_integration.sh
#
# Requirements:
#   - uv (for pyspark-connect)
#   - Java 17+ (JAVA_HOME or Homebrew openjdk)
#   - mix deps already fetched
#
# The script:
#   1. Starts Spark Connect server on localhost:15002
#   2. Waits for it to become ready
#   3. Runs mix test --include integration
#   4. Tears down the server

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_SERVER_DIR="$PROJECT_ROOT/test/spark_server"
PORT=15002

# --- Resolve JAVA_HOME ---
if [ -z "${JAVA_HOME:-}" ]; then
  if [ -d "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"
  elif command -v /usr/libexec/java_home &>/dev/null; then
    export JAVA_HOME="$(/usr/libexec/java_home -v 17+ 2>/dev/null || true)"
  fi
fi

if [ -z "${JAVA_HOME:-}" ]; then
  echo "ERROR: JAVA_HOME not set and no Java 17+ found."
  echo "Install via: brew install openjdk"
  exit 1
fi

echo "JAVA_HOME: $JAVA_HOME"
"$JAVA_HOME/bin/java" -version 2>&1 | head -1

# --- Resolve SPARK_HOME from pyspark package ---
SPARK_HOME="$(cd "$SPARK_SERVER_DIR" && uv run python -c 'import pyspark; print(pyspark.__file__.rsplit("/",1)[0])')"
export SPARK_HOME
echo "SPARK_HOME: $SPARK_HOME"

# --- Check if port is already in use ---
if lsof -i ":$PORT" -sTCP:LISTEN &>/dev/null; then
  echo "Port $PORT already in use. Using existing server."
  SPARK_PID=""
else
  # --- Start Spark Connect server ---
  echo "Starting Spark Connect server on port $PORT..."

  bash "$SPARK_HOME/bin/spark-submit" \
    --class org.apache.spark.sql.connect.SimpleSparkConnectService \
    --conf "spark.connect.grpc.binding.port=$PORT" \
    "$SPARK_HOME/jars/spark-connect_2.13-4.1.1.jar" \
    &

  SPARK_PID=$!
  echo "Spark Connect server PID: $SPARK_PID"

  # --- Wait for server to be ready ---
  echo -n "Waiting for server to be ready"
  MAX_WAIT=60
  WAITED=0
  while ! lsof -i ":$PORT" -sTCP:LISTEN &>/dev/null; do
    if [ $WAITED -ge $MAX_WAIT ]; then
      echo ""
      echo "ERROR: Spark Connect server did not start within ${MAX_WAIT}s"
      kill "$SPARK_PID" 2>/dev/null || true
      exit 1
    fi
    if ! kill -0 "$SPARK_PID" 2>/dev/null; then
      echo ""
      echo "ERROR: Spark Connect server process exited unexpectedly"
      exit 1
    fi
    echo -n "."
    sleep 1
    WAITED=$((WAITED + 1))
  done
  echo " ready! (${WAITED}s)"
fi

# --- Cleanup function ---
cleanup() {
  if [ -n "${SPARK_PID:-}" ]; then
    echo "Stopping Spark Connect server (PID $SPARK_PID)..."
    kill "$SPARK_PID" 2>/dev/null || true
    wait "$SPARK_PID" 2>/dev/null || true
    echo "Server stopped."
  fi
}
trap cleanup EXIT

# --- Run integration tests ---
echo ""
echo "=== Running integration tests ==="
cd "$PROJECT_ROOT"
mix test --include integration "$@"
