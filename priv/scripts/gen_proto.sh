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
mkdir -p "$OUTPUT_DIR/spark/connect"

echo "Generating Elixir protobuf modules..."

# protoc-gen-elixir >= 0.17 nests its output under the package module path
# ("spark/connect") on top of the .proto path, so generate into a staging
# directory and keep the historical lib/spark_ex/proto/spark/connect layout.
STAGING_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_DIR"' EXIT

protoc \
  --elixir_out=plugins=grpc:"$STAGING_DIR" \
  --proto_path="$PROTO_SRC" \
  "$PROTO_SRC"/spark/connect/*.proto

if [ -d "$STAGING_DIR/spark/connect/spark/connect" ]; then
  GENERATED_DIR="$STAGING_DIR/spark/connect/spark/connect"
else
  GENERATED_DIR="$STAGING_DIR/spark/connect"
fi

cp "$GENERATED_DIR"/*.pb.ex "$OUTPUT_DIR/spark/connect/"

echo "Generated Elixir protobuf modules in $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"/spark/connect/
