defmodule SparkEx.Integration.ProgressHandlerTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  test "progress handler receives execution progress" do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      SparkEx.clear_progress_handlers(session)
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    parent = self()

    handler = fn payload ->
      send(parent, {:progress, payload})
    end

    :ok = SparkEx.register_progress_handler(session, handler)

    df = SparkEx.range(session, 1_000_000)
    assert {:ok, _} = SparkEx.DataFrame.count(df)

    assert_receive {:progress, payload}, 5_000
    session_id = SparkEx.Session.get_state(session).session_id
    assert payload.metadata.session_id == session_id
  end
end
