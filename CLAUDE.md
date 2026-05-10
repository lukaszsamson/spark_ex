# CLAUDE.md

## CI gate script

Before declaring any change complete, run the local mirror of CI:

```
tools/check.sh           # format + compile (--warnings-as-errors) + credo + test + dialyzer
tools/check.sh quick     # same minus dialyzer (faster inner loop)
tools/check.sh all       # everything + integration (needs Spark Connect on :15002)
tools/check.sh <gate>    # one of: format compile credo test dialyzer integration deps
```

`tools/check.sh` mirrors `.github/workflows/ci.yml` exactly (unit + format/credo/dialyzer + integration jobs, `MIX_ENV=test`). It uses `set -euo pipefail`, so the first failing gate halts the run with the underlying tool's exit code.

Credo runs with the project's `.credo.exs` defaults (no flags, no disabled checks). The current baseline is dirty — clean it up incrementally; do not silence checks to make the gate pass.

The integration gate expects `SPARK_REMOTE` (defaults to `sc://localhost:15002`); without a reachable Spark Connect server, integration tests auto-skip in `mix test` and the `integration` subcommand fails fast with a hint.

Do not claim a task done if any gate fails. Type-check / test green is necessary but not sufficient — for UI/feature changes, also exercise the behavior end-to-end.
