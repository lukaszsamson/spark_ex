defmodule SparkEx.ResultChunkingRequestTest do
  use ExUnit.Case, async: true

  alias SparkEx.Connect.Client

  test "build_execute_request encodes result chunking options" do
    session = %SparkEx.Session{
      session_id: "s1",
      client_type: :elixir,
      user_id: "u1",
      server_side_session_id: nil,
      allow_arrow_batch_chunking: false,
      preferred_arrow_chunk_size: nil
    }

    plan = %Spark.Connect.Plan{op_type: {:root, %Spark.Connect.Relation{}}}

    request =
      Client.build_execute_request(
        session,
        plan,
        [],
        nil,
        true,
        allow_arrow_batch_chunking: true,
        preferred_arrow_chunk_size: 1024
      )

    [chunking_option | _] = request.request_options

    assert %Spark.Connect.ExecutePlanRequest.RequestOption{
             request_option: {
               :result_chunking_options,
               %Spark.Connect.ResultChunkingOptions{
                 allow_arrow_batch_chunking: true,
                 preferred_arrow_chunk_size: 1024
               }
             }
           } = chunking_option
  end
end
