#!/usr/bin/env bash
# Generates Elixir protobuf modules from vendored Spark Connect proto files.
# Usage: mix spark_ex.gen_proto  (or run directly: ./priv/scripts/gen_proto.sh)

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROTO_SRC="$PROJECT_ROOT/priv/proto"
OUTPUT_DIR="$PROJECT_ROOT/lib/spark_ex/proto"

# Ensure protoc-gen-elixir is available
if ! command -v protoc-gen-elixir &> /dev/null; then
  echo "protoc-gen-elixir not found. Install with: mix escript.install hex protobuf"
  exit 1
fi

# Clean previous generated files
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

echo "Generating Elixir protobuf modules..."

protoc \
  --elixir_out=plugins=grpc:"$OUTPUT_DIR" \
  --proto_path="$PROTO_SRC" \
  "$PROTO_SRC"/spark/connect/*.proto

echo "Generated Elixir protobuf modules in $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"/spark/connect/
