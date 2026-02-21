defmodule SparkEx.Unit.StreamingQueryTest do
  use ExUnit.Case, async: true

  alias SparkEx.StreamingQuery
  alias SparkEx.StreamingQueryManager

  test "id/run_id/name accessors" do
    query = %StreamingQuery{session: self(), query_id: "qid", run_id: "rid", name: "qname"}

    assert StreamingQuery.id(query) == "qid"
    assert StreamingQuery.run_id(query) == "rid"
    assert StreamingQuery.name(query) == "qname"
  end

  test "StreamingQueryManager exposes list_active/1 alias" do
    assert {:module, StreamingQueryManager} = Code.ensure_loaded(StreamingQueryManager)
    assert function_exported?(StreamingQueryManager, :list_active, 1)
  end
end
