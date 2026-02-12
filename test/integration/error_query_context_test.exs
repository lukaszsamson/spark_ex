defmodule SparkEx.Integration.ErrorQueryContextTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "analysis error includes query contexts", %{session: session} do
    df = SparkEx.sql(session, "SELECT missing_col FROM range(1)")

    assert {:error, %SparkEx.Error.Remote{} = error} = SparkEx.DataFrame.collect(df)

    assert error.error_class == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
    assert is_list(error.query_contexts)
  end
end
