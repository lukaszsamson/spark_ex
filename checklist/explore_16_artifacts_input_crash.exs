# Gate assertion: invalid add_artifacts input must NOT kill the session.
# Usage: mix run checklist/explore_16_artifacts_input_crash.exs
#
# Regression guard for V02_BLOCKERS [H1]: a malformed artifacts argument used to
# raise inside the SparkEx.Session GenServer, crashing the whole session. The
# fix makes validation return {:error, _} (or raise in the caller) while the
# session stays alive. If the session dies, this script prints the gate marker
# CHECKLIST-GATE-FAIL and exits non-zero so CI fails.

IO.puts("=== Gate: add_artifacts input validation must not crash session ===\n")

Process.flag(:trap_exit, true)

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

fail = fn reason ->
  IO.puts("CHECKLIST-GATE-FAIL: #{reason}")
  System.halt(1)
end

# Baseline: a well-formed artifact upload should succeed (or at worst return a
# clean {:error, _} from the server) without taking the session down.
IO.inspect(SparkEx.add_artifacts(s, [{"ok.txt", "hello"}]), label: "valid tuple input")

# The regression: a non-tuple element is invalid input. It must be reported as
# an error (returned tuple OR a raised/exited error in THIS process), never as a
# session crash.
invalid_result =
  try do
    {:returned, SparkEx.add_artifacts(s, ["/tmp/not-a-tuple.txt"])}
  rescue
    e -> {:raised, Exception.message(e)}
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(invalid_result, label: "invalid input result")

# Give any (buggy) crash propagation a moment to land.
receive do
  {:EXIT, ^s, reason} ->
    IO.inspect(reason, label: "UNEXPECTED session exit reason")
after
  500 -> :ok
end

alive? = Process.alive?(s)
IO.puts("session_alive?: #{inspect(alive?)}")
unless alive?, do: fail.("session GenServer died after invalid add_artifacts input")

# A {:exit, ...} result means the GenServer.call observed the server dying — also
# a crash, even if the supervisor would restart it.
case invalid_result do
  {:exit, reason} ->
    fail.("add_artifacts/2 exited instead of returning an error: #{inspect(reason)}")

  _ ->
    :ok
end

# Session must remain usable end-to-end.
version_result =
  try do
    SparkEx.spark_version(s)
  catch
    :exit, reason -> {:exit, reason}
  end

IO.inspect(version_result, label: "spark_version after invalid input")

case version_result do
  {:ok, _} -> :ok
  other -> fail.("session unusable after invalid add_artifacts input: #{inspect(other)}")
end

IO.puts("\n=== OK: session survived invalid add_artifacts input ===")
