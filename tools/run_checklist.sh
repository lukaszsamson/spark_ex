#!/usr/bin/env bash
#
# Checklist gate — runs every checklist/*.exs script against a live Spark Connect
# server and fails if any script triggers a *native* panic (Explorer/polars NIF)
# or a session crash. These are the V02_BLOCKERS-class failures; transient
# caught `ERROR:` lines from the exploratory negative tests are expected and do
# NOT fail the gate (they differ by Spark version and are noise, not bugs).
#
# Gate FAILS when any non-skipped script log contains:
#   - "panicked at"        (a Rust panic fired in the Explorer/polars NIF)
#   - ":nif_panicked"      (rustler-surfaced NIF panic)
#   - "CHECKLIST-GATE-FAIL" (an assertion script flagged a regression, e.g. H1)
#
# Connection: scripts connect via SPARK_REMOTE (default sc://localhost:15002).
#
# Usage:
#   SPARK_REMOTE=sc://localhost:15002 tools/run_checklist.sh
#   tools/run_checklist.sh checklist/section_01_session.exs   # run a subset
#
set -uo pipefail

cd "$(dirname "$0")/.."

export SPARK_REMOTE="${SPARK_REMOTE:-sc://localhost:15002}"
TIMEOUT="${CHECKLIST_TIMEOUT:-240}"

# Scripts intentionally excluded from the gate:
#   repro_arrow_panic_explorer_only.exs — by design calls Explorer directly on a
#   duplicate-column payload to demonstrate the known UPSTREAM polars panic (see
#   ARROW_PANIC.md). SparkEx's own decode path already guards this case.
SKIP=(
  "repro_arrow_panic_explorer_only.exs"
)

is_skipped() {
  local base="$1"
  for s in "${SKIP[@]}"; do
    [ "$base" = "$s" ] && return 0
  done
  return 1
}

if [ "$#" -gt 0 ]; then
  FILES=("$@")
else
  FILES=(checklist/*.exs)
fi

LOGDIR="$(mktemp -d)"
echo "Checklist gate: SPARK_REMOTE=$SPARK_REMOTE  logs=$LOGDIR"
echo "Compiling once before the sweep..."
mix compile >/dev/null 2>&1 || { echo "compile failed"; exit 2; }

total=0
skipped=0
failed=0
fail_list=()

for f in "${FILES[@]}"; do
  base="$(basename "$f")"
  if is_skipped "$base"; then
    skipped=$((skipped + 1))
    echo "SKIP  $base"
    continue
  fi
  total=$((total + 1))
  log="$LOGDIR/$base.log"
  timeout "$TIMEOUT" mix run "$f" >"$log" 2>&1
  rc=$?

  reasons=""
  if grep -qE "panicked at|:nif_panicked" "$log"; then
    reasons+="native-panic "
  fi
  if grep -q "CHECKLIST-GATE-FAIL" "$log"; then
    reasons+="assertion-failed "
  fi
  if [ "$rc" -eq 124 ]; then
    reasons+="timeout(${TIMEOUT}s) "
  fi

  if [ -n "$reasons" ]; then
    failed=$((failed + 1))
    fail_list+=("$base: ${reasons}")
    echo "FAIL  $base  [${reasons}]"
  else
    echo "ok    $base"
  fi
done

echo ""
echo "================ Checklist gate summary ================"
echo "ran=$total  skipped=$skipped  failed=$failed"
if [ "$failed" -gt 0 ]; then
  echo ""
  echo "Failing scripts (logs in $LOGDIR):"
  for item in "${fail_list[@]}"; do
    echo "  - $item"
  done
  echo ""
  echo "First offending excerpts:"
  for item in "${fail_list[@]}"; do
    b="${item%%:*}"
    echo "----- $b -----"
    grep -nE "panicked at|:nif_panicked|CHECKLIST-GATE-FAIL" "$LOGDIR/$b.log" | head -3
  done
  exit 1
fi

echo "All checklist scripts clean (no native panics, no session crashes)."
