#!/usr/bin/env bash
# Starts a local Spark Connect server and runs SparkEx integration tests.
#
# Usage: ./test/run_integration.sh
#
# Requirements:
#   - Java 17 or 21 (JAVA_HOME or Homebrew openjdk@21)
#   - Official Spark 4.1.1 distribution in test/spark-4.1.1-bin-hadoop3-connect/
#   - mix deps already fetched
#
# The script:
#   1. Starts Spark Connect server on localhost:15002 (daemon)
#   2. Waits for it to become ready
#   3. Runs mix test --include integration
#   4. Tears down the server

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SPARK_HOME="$PROJECT_ROOT/test/spark-4.1.1-bin-hadoop3-connect"
PORT=15002

# --- Validate SPARK_HOME ---
if [ ! -d "$SPARK_HOME/sbin" ]; then
  echo "ERROR: Spark distribution not found at $SPARK_HOME"
  echo "Download from https://spark.apache.org/downloads.html:"
  echo "  curl -L -o /tmp/spark.tgz 'https://dlcdn.apache.org/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3-connect.tgz'"
  echo "  tar -xzf /tmp/spark.tgz -C test/"
  exit 1
fi

export SPARK_HOME

# --- Resolve JAVA_HOME (prefer 21, then 17) ---
if [ -z "${JAVA_HOME:-}" ]; then
  if [ -d "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
  elif [ -d "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
  elif command -v /usr/libexec/java_home &>/dev/null; then
    export JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || /usr/libexec/java_home -v 17 2>/dev/null || true)"
  fi
fi

if [ -z "${JAVA_HOME:-}" ]; then
  echo "ERROR: JAVA_HOME not set and no Java 17/21 found."
  echo "Spark 4.1.1 requires Java 17 or 21."
  echo "Install via: brew install openjdk@21"
  exit 1
fi

echo "JAVA_HOME: $JAVA_HOME"
"$JAVA_HOME/bin/java" -version 2>&1 | head -1

STARTED_SERVER=false

# --- Check if port is already in use ---
if lsof -i ":$PORT" -sTCP:LISTEN &>/dev/null; then
  echo "Port $PORT already in use. Using existing server."
else
  # --- Start Spark Connect server (daemon) ---
  echo "Starting Spark Connect server on port $PORT..."
  bash "$SPARK_HOME/sbin/start-connect-server.sh" \
    --conf "spark.connect.grpc.binding.port=$PORT"
  STARTED_SERVER=true

  # --- Wait for server to be ready ---
  echo -n "Waiting for server to be ready"
  MAX_WAIT=60
  WAITED=0
  while ! lsof -i ":$PORT" -sTCP:LISTEN &>/dev/null; do
    if [ $WAITED -ge $MAX_WAIT ]; then
      echo ""
      echo "ERROR: Spark Connect server did not start within ${MAX_WAIT}s"
      bash "$SPARK_HOME/sbin/stop-connect-server.sh" 2>/dev/null || true
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
  if [ "$STARTED_SERVER" = true ]; then
    echo "Stopping Spark Connect server..."
    bash "$SPARK_HOME/sbin/stop-connect-server.sh" 2>/dev/null || true
    echo "Server stopped."
  fi
}
trap cleanup EXIT

# --- Run integration tests ---
echo ""
echo "=== Running integration tests ==="
cd "$PROJECT_ROOT"
mix test --include integration "$@"
