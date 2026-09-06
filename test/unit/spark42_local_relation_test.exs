defmodule SparkEx.Spark42LocalRelationTest do
  use ExUnit.Case, async: true

  alias SparkEx.Session
  alias SparkEx.Error.Remote

  @limit "spark.sql.session.localRelationSizeLimit"

  test "size limit is parsed independently and absent on older servers" do
    assert Session.__parse_local_relation_configs__([{@limit, "10"}]).size_limit == 10
    assert Session.__parse_local_relation_configs__([{@limit, nil}]).size_limit == nil
    assert Session.__parse_local_relation_configs__([{@limit, "bad"}]).size_limit == nil
    assert Session.__parse_local_relation_configs__([{@limit, "-1"}]).size_limit == nil
    assert Session.__parse_local_relation_configs__([{@limit, "0"}]).size_limit == 0
  end

  test "serialized schema and repeated chunks count, including exact boundary" do
    assert :ok = Session.__validate_local_relation_size__(["abc", "abc"], "xy", 8)
    assert :ok = Session.__validate_local_relation_size__(["abc", "abc"], "xy", 9)

    assert {:error,
            %Remote{error_class: "LOCAL_RELATION_SIZE_LIMIT_EXCEEDED", message_parameters: params}} =
             Session.__validate_local_relation_size__(["abc", "abc"], "xy", 7)

    assert params == %{"actualSize" => "8", "sizeLimit" => "7"}
    assert :ok = Session.__validate_local_relation_size__(["abc"], nil, nil)
    assert :ok = Session.__validate_local_relation_size__([], nil, 0)
  end

  test "cache strategy overrides retain the server limit" do
    configs = Session.__parse_local_relation_configs__([{@limit, "1"}])

    assert {:ok, %{size_limit: 1}} =
             Session.__local_relation_chunk_params__(
               [cache_threshold: 0, cache_chunk_size: 10],
               configs
             )
  end

  test "oversized cached relation fails before any upload on an unconnected session" do
    source = Explorer.DataFrame.new(%{"id" => [1, 1, 1]})
    state = %Session{session_id: "test"}

    params = %{
      chunk_size_bytes: 1024,
      chunk_size_rows: 1,
      batch_of_chunks_size_bytes: 2048,
      size_limit: 1
    }

    assert {:reply, {:error, %Remote{error_class: "LOCAL_RELATION_SIZE_LIMIT_EXCEEDED"}}, ^state} =
             Session.handle_call(
               {:create_dataframe_chunked_cache, source, "id BIGINT", params},
               nil,
               state
             )
  end

  test "failed configuration lookup cannot fail open to unlimited cached upload" do
    source = Explorer.DataFrame.new(%{"id" => [1]})
    state = %Session{session_id: "test", local_relation_configs: {:unavailable, 123}}

    params = %{
      chunk_size_bytes: 1024,
      chunk_size_rows: 1,
      batch_of_chunks_size_bytes: 2048,
      size_limit: nil
    }

    assert {:reply, {:error, :local_relation_config_unavailable}, ^state} =
             Session.handle_call(
               {:create_dataframe_chunked_cache, source, "id BIGINT", params},
               nil,
               state
             )
  end
end
