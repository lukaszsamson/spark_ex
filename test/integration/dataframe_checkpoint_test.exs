defmodule SparkEx.Integration.DataFrameCheckpointTest do
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

  test "checkpoint materializes and collects", %{session: session} do
    df = SparkEx.range(session, 3)

    case DataFrame.checkpoint(df, eager: true) do
      %DataFrame{} = checkpointed ->
        assert {:ok, rows} = DataFrame.collect(checkpointed)
        assert Enum.map(rows, & &1["id"]) == [0, 1, 2]

      {:error, %SparkEx.Error.Remote{} = error} ->
        assert error.error_class in ["_LEGACY_ERROR_TEMP_3016", "CANNOT_MODIFY_CONFIG"]
        assert is_binary(error.message)
    end
  end

  test "checkpoint uses cached remote relation semantics", %{session: session} do
    df = SparkEx.range(session, 2)

    case DataFrame.checkpoint(df, eager: true) do
      %DataFrame{} = checkpointed ->
        assert {:cached_remote_relation, _relation_id} = checkpointed.plan
        assert {:ok, rows} = DataFrame.collect(checkpointed)
        assert Enum.map(rows, & &1["id"]) == [0, 1]

      {:error, %SparkEx.Error.Remote{} = error} ->
        assert error.error_class in ["_LEGACY_ERROR_TEMP_3016", "CANNOT_MODIFY_CONFIG"]
        assert is_binary(error.message)
    end
  end

  test "to/2 casts to target schema", %{session: session} do
    df = SparkEx.range(session, 3) |> DataFrame.to("id STRING")

    assert {:ok, rows} = DataFrame.collect(df)
    assert Enum.all?(rows, fn row -> is_binary(row["id"]) end)
  end
end
