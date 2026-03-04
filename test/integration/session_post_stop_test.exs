defmodule SparkEx.Integration.SessionPostStopTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  test "operations fail after session stop" do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    assert :ok = SparkEx.Session.stop(session)

    stopped? =
      try do
        SparkEx.Session.is_stopped(session)
      catch
        :exit, _ -> true
      end

    assert stopped? == true

    df = SparkEx.range(session, 1)

    result =
      try do
        DataFrame.collect(df)
      catch
        :exit, {:noproc, _} -> {:exit, :noproc}
        :exit, {:normal, _} -> {:exit, :normal}
      end

    assert match?({:error, :session_released}, result) or
             match?({:exit, :noproc}, result) or
             match?({:exit, :normal}, result),
           "expected session_released or noproc/normal exit, got: #{inspect(result)}"
  end
end
