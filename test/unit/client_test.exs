defmodule SparkEx.Connect.ClientTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client

  test "analyze_explain/3 returns error for unknown explain mode" do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "test-session",
      user_id: "test",
      client_type: "test"
    }

    assert {:error, {:invalid_explain_mode, :invalid_mode}} =
             Client.analyze_explain(session, %Spark.Connect.Plan{}, :invalid_mode)
  end
end
