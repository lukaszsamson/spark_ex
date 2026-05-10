#!/usr/bin/env bash
# Local mirror of .github/workflows/ci.yml
#
# Usage:
#   tools/check.sh                 # all gates: format, compile, credo, test, dialyzer (no Spark Connect)
#   tools/check.sh quick           # format + compile --warnings-as-errors + credo + test (skip dialyzer)
#   tools/check.sh format          # mix format --check-formatted
#   tools/check.sh compile         # mix compile --warnings-as-errors
#   tools/check.sh credo           # mix credo
#   tools/check.sh test            # mix test (unit; integration tests auto-skip without SPARK_REMOTE)
#   tools/check.sh dialyzer        # mix dialyzer
#   tools/check.sh integration     # mix test with SPARK_REMOTE set (requires Spark Connect on :15002)
#   tools/check.sh all             # everything including integration
#
# Env:
#   SPARK_REMOTE  defaults to sc://localhost:15002 for the integration gate
set -euo pipefail

cd "$(dirname "$0")/.."
export MIX_ENV=test

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[34m%s\033[0m\n' "$*"; }

run() {
  blue "==> $*"
  "$@"
}

gate_deps()    { run mix deps.get; }
gate_format()  { run mix format --check-formatted; }
gate_compile() { run mix compile --warnings-as-errors; }
gate_credo()   { run mix credo; }
gate_test()    { run mix test; }
gate_dialyzer(){ run mix dialyzer; }

gate_integration() {
  local remote="${SPARK_REMOTE:-sc://localhost:15002}"
  local hostport="${remote#sc://}"
  local host="${hostport%%:*}"
  local port="${hostport##*:}"
  if ! nc -z "$host" "$port" 2>/dev/null; then
    red "Spark Connect not reachable at $remote"
    red "Start it, e.g.:  \"\$SPARK_HOME/sbin/start-connect-server.sh\" --conf spark.connect.grpc.binding.port=$port"
    exit 1
  fi
  SPARK_REMOTE="$remote" run mix test
}

cmd="${1:-default}"
case "$cmd" in
  default)
    gate_deps; gate_format; gate_compile; gate_credo; gate_test; gate_dialyzer
    ;;
  quick)
    gate_deps; gate_format; gate_compile; gate_credo; gate_test
    ;;
  all)
    gate_deps; gate_format; gate_compile; gate_credo; gate_test; gate_dialyzer; gate_integration
    ;;
  format|compile|credo|test|dialyzer|integration|deps)
    "gate_$cmd"
    ;;
  *)
    red "Unknown command: $cmd"
    sed -n '1,20p' "$0" >&2
    exit 2
    ;;
esac

green "OK: $cmd"
