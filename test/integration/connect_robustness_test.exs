defmodule SparkEx.Integration.ConnectRobustnessTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  @tag :skip
  test "connection fails fast on unreachable host" do
    task = Task.async(fn -> SparkEx.connect(url: "sc://localhost:1", connect_timeout: 500) end)

    case Task.yield(task, 2000) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:error, _}} ->
        assert true

      {:exit, _} ->
        assert true

      nil ->
        flunk("connect timeout did not return within 2s")
    end
  end

  test "large payload collects without error", %{session: session} do
    df = SparkEx.sql(session, "SELECT repeat('x', 2000) AS payload FROM range(0, 2000)")
    assert {:ok, rows} = DataFrame.collect(df)
    assert length(rows) == 2000
    assert is_binary(hd(rows)["payload"])
  end
end
