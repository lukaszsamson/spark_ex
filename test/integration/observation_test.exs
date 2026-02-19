defmodule SparkEx.Integration.ObservationTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Column, DataFrame, Observation, Functions}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "observe collects metrics after action", %{session: session} do
    obs = Observation.new("metric_test_#{System.unique_integer([:positive])}")

    df =
      SparkEx.range(session, 5)
      |> DataFrame.observe(obs, [Column.alias_(Functions.sum(Functions.col("id")), "sum_id")])

    assert {:ok, _} = DataFrame.collect(df)

    metrics = Observation.get(obs)
    assert metrics == %{"sum_id" => 10}
  end
end
