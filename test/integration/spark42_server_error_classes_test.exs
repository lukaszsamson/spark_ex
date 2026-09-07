defmodule SparkEx.Integration.Spark42ServerErrorClassesTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.2"

  alias SparkEx.{DataFrame, Session}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  # Spark v4.2.0 pins these conditions in
  # common/utils/src/main/resources/error/error-conditions.json. Exercise the
  # server relation that introduced them so SparkEx cannot accidentally rename
  # the class, synthesize a SQLSTATE, or discard the message parameters.
  test "Parse preserves the final release error for multi-column input", %{session: session} do
    error =
      session
      |> SparkEx.sql("SELECT '1' AS first, '2' AS second")
      |> DataFrame.parse(:csv, "value INT")
      |> collect_remote_error()

    assert error.error_class == "DATAFRAME_INPUT_NOT_SINGLE_COLUMN"
    assert error.sql_state == "42K09"
    assert error.message_parameters["numColumns"] == "2"
  end

  test "Parse preserves the final release error for non-string input", %{session: session} do
    error =
      session
      |> SparkEx.sql("SELECT CAST(1 AS INT) AS value")
      |> DataFrame.parse(:json, "value INT")
      |> collect_remote_error()

    assert error.error_class == "DATAFRAME_INPUT_NOT_STRING_TYPE"
    assert error.sql_state == "42K09"
    assert is_binary(error.message_parameters["dataType"])
    assert error.message_parameters["dataType"] =~ "INT"
  end

  defp collect_remote_error(df) do
    assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)
    error
  end
end
