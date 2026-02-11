defmodule SparkEx.Unit.StreamingQueryTest do
  use ExUnit.Case, async: true

  alias SparkEx.StreamingQuery

  test "id/run_id/name accessors" do
    query = %StreamingQuery{session: self(), query_id: "qid", run_id: "rid", name: "qname"}

    assert StreamingQuery.id(query) == "qid"
    assert StreamingQuery.run_id(query) == "rid"
    assert StreamingQuery.name(query) == "qname"
  end
end
