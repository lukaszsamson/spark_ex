defmodule SparkEx.Unit.ArtifactTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{
    AddArtifactsRequest,
    AddArtifactsResponse,
    ArtifactStatusesRequest,
    ArtifactStatusesResponse,
    UserContext
  }

  alias SparkEx.Connect.Client
  alias SparkEx.Artifacts

  setup do
    session = %SparkEx.Session{
      channel: nil,
      session_id: "sess-123",
      server_side_session_id: "ss-456",
      user_id: "test_user",
      client_type: "elixir/test"
    }

    %{session: session}
  end

  describe "ArtifactStatusesRequest building" do
    test "includes session fields and artifact names" do
      request = %ArtifactStatusesRequest{
        session_id: "sess-123",
        client_observed_server_side_session_id: "ss-456",
        user_context: %UserContext{user_id: "test_user"},
        client_type: "elixir/test",
        names: ["artifact1.jar", "artifact2.jar"]
      }

      assert request.session_id == "sess-123"
      assert request.names == ["artifact1.jar", "artifact2.jar"]
      assert request.user_context.user_id == "test_user"
    end
  end

  describe "ArtifactStatusesResponse parsing" do
    test "maps artifact names to exists status" do
      resp = %ArtifactStatusesResponse{
        session_id: "sess-123",
        server_side_session_id: "ss-456",
        statuses: %{
          "artifact1.jar" => %ArtifactStatusesResponse.ArtifactStatus{exists: true},
          "artifact2.jar" => %ArtifactStatusesResponse.ArtifactStatus{exists: false}
        }
      }

      assert resp.statuses["artifact1.jar"].exists == true
      assert resp.statuses["artifact2.jar"].exists == false
      assert resp.server_side_session_id == "ss-456"
    end

    test "empty statuses map" do
      resp = %ArtifactStatusesResponse{
        session_id: "s",
        statuses: %{}
      }

      assert resp.statuses == %{}
    end
  end

  describe "AddArtifactsRequest building" do
    test "batch payload with single chunk artifacts" do
      chunk = %AddArtifactsRequest.ArtifactChunk{
        data: "binary data here",
        crc: 12345
      }

      artifact = %AddArtifactsRequest.SingleChunkArtifact{
        name: "my-lib.jar",
        data: chunk
      }

      batch = %AddArtifactsRequest.Batch{artifacts: [artifact]}

      request = %AddArtifactsRequest{
        session_id: "sess-123",
        user_context: %UserContext{user_id: "test_user"},
        client_type: "elixir/test",
        payload: {:batch, batch}
      }

      assert request.session_id == "sess-123"
      assert {:batch, %AddArtifactsRequest.Batch{artifacts: [art]}} = request.payload
      assert art.name == "my-lib.jar"
      assert art.data.crc == 12345
    end

    test "CRC32 computation for artifact data" do
      data = "hello world"
      crc = :erlang.crc32(data)

      chunk = %AddArtifactsRequest.ArtifactChunk{data: data, crc: crc}
      assert chunk.crc == :erlang.crc32("hello world")
      assert is_integer(chunk.crc)
    end

    test "multiple artifacts in a batch" do
      artifacts =
        for i <- 1..3 do
          data = "data-#{i}"
          crc = :erlang.crc32(data)

          %AddArtifactsRequest.SingleChunkArtifact{
            name: "file-#{i}.jar",
            data: %AddArtifactsRequest.ArtifactChunk{data: data, crc: crc}
          }
        end

      batch = %AddArtifactsRequest.Batch{artifacts: artifacts}
      assert length(batch.artifacts) == 3
      assert Enum.at(batch.artifacts, 0).name == "file-1.jar"
      assert Enum.at(batch.artifacts, 2).name == "file-3.jar"
    end

    test "begin_chunk payload for chunked uploads" do
      initial_chunk = %AddArtifactsRequest.ArtifactChunk{data: "first-part", crc: 0}

      begin = %AddArtifactsRequest.BeginChunkedArtifact{
        name: "big-file.jar",
        total_bytes: 1_000_000,
        num_chunks: 10,
        initial_chunk: initial_chunk
      }

      request = %AddArtifactsRequest{
        session_id: "s",
        user_context: %UserContext{user_id: "u"},
        payload: {:begin_chunk, begin}
      }

      assert {:begin_chunk, %AddArtifactsRequest.BeginChunkedArtifact{name: "big-file.jar"}} =
               request.payload
    end
  end

  describe "build_add_artifacts_requests/3" do
    test "batches small artifacts into a single request", %{session: session} do
      requests =
        Client.build_add_artifacts_requests(
          session,
          [{"a.txt", "abc"}, {"b.txt", "def"}],
          16
        )

      assert length(requests) == 1
      [request] = requests
      assert {:batch, %AddArtifactsRequest.Batch{artifacts: artifacts}} = request.payload
      assert Enum.map(artifacts, & &1.name) == ["a.txt", "b.txt"]
    end

    test "streams large artifact as begin_chunk + chunk messages", %{session: session} do
      data = String.duplicate("x", 25)
      requests = Client.build_add_artifacts_requests(session, [{"large.bin", data}], 10)

      assert length(requests) == 3
      [begin_req, chunk_req_1, chunk_req_2] = requests

      assert {:begin_chunk, %AddArtifactsRequest.BeginChunkedArtifact{} = begin_chunk} =
               begin_req.payload

      assert begin_chunk.name == "large.bin"
      assert begin_chunk.total_bytes == 25
      assert begin_chunk.num_chunks == 3
      assert byte_size(begin_chunk.initial_chunk.data) == 10
      assert {:chunk, %AddArtifactsRequest.ArtifactChunk{data: c1}} = chunk_req_1.payload
      assert {:chunk, %AddArtifactsRequest.ArtifactChunk{data: c2}} = chunk_req_2.payload
      assert byte_size(c1) == 10
      assert byte_size(c2) == 5
    end

    test "flushes batch before chunked artifact and resumes batching", %{session: session} do
      requests =
        Client.build_add_artifacts_requests(
          session,
          [
            {"small-1.txt", "aaaa"},
            {"large.bin", String.duplicate("x", 25)},
            {"small-2.txt", "bbbb"}
          ],
          10
        )

      assert length(requests) == 5
      [batch_1, begin_req, _chunk_1, _chunk_2, batch_2] = requests

      assert {:batch, %AddArtifactsRequest.Batch{artifacts: [%{name: "small-1.txt"}]}} =
               batch_1.payload

      assert {:begin_chunk, %AddArtifactsRequest.BeginChunkedArtifact{name: "large.bin"}} =
               begin_req.payload

      assert {:batch, %AddArtifactsRequest.Batch{artifacts: [%{name: "small-2.txt"}]}} =
               batch_2.payload
    end
  end

  describe "Artifacts.prepare/2" do
    test "reads local files and prefixes names" do
      jar_path = tmp_path("artifact_jar.txt")
      file_path = tmp_path("artifact_file.txt")

      File.write!(jar_path, "jar-data")
      File.write!(file_path, "file-data")

      assert {:ok, artifacts} = Artifacts.prepare([jar_path, file_path], "jars")
      assert {"jars/spark_ex_artifact_jar.txt", "jar-data"} in artifacts
      assert {"jars/spark_ex_artifact_file.txt", "file-data"} in artifacts
    end

    test "returns error for missing file" do
      missing = tmp_path("missing.txt")
      assert {:error, {:file_read_error, ^missing, _}} = Artifacts.prepare(missing, "files/")
    end
  end

  defp tmp_path(name) do
    Path.join(System.tmp_dir!(), "spark_ex_" <> name)
  end

  describe "AddArtifactsResponse parsing" do
    test "artifact summaries with CRC status" do
      resp = %AddArtifactsResponse{
        session_id: "sess-123",
        server_side_session_id: "ss-456",
        artifacts: [
          %AddArtifactsResponse.ArtifactSummary{name: "file1.jar", is_crc_successful: true},
          %AddArtifactsResponse.ArtifactSummary{name: "file2.jar", is_crc_successful: false}
        ]
      }

      assert length(resp.artifacts) == 2
      assert hd(resp.artifacts).name == "file1.jar"
      assert hd(resp.artifacts).is_crc_successful == true
      assert Enum.at(resp.artifacts, 1).is_crc_successful == false
    end

    test "empty artifacts list" do
      resp = %AddArtifactsResponse{
        session_id: "s",
        artifacts: []
      }

      assert resp.artifacts == []
    end
  end
end
