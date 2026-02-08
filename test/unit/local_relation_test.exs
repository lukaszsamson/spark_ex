defmodule SparkEx.Unit.LocalRelationTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{
    CachedLocalRelation,
    ChunkedCachedLocalRelation,
    LocalRelation,
    Plan,
    Relation
  }

  alias SparkEx.Connect.PlanEncoder

  describe "PlanEncoder.encode_relation/2 for local relation types" do
    test "encodes local_relation with data and schema" do
      data = <<1, 2, 3, 4>>
      schema = "id INT, name STRING"

      {relation, counter} = PlanEncoder.encode_relation({:local_relation, data, schema}, 0)

      assert counter == 1
      assert %Relation{rel_type: {:local_relation, local}} = relation
      assert %LocalRelation{data: ^data, schema: ^schema} = local
      assert relation.common.plan_id == 0
    end

    test "encodes local_relation with nil schema" do
      data = <<10, 20, 30>>

      {relation, _counter} = PlanEncoder.encode_relation({:local_relation, data, nil}, 5)

      assert %Relation{rel_type: {:local_relation, local}} = relation
      assert local.data == data
      assert local.schema == nil
      assert relation.common.plan_id == 5
    end

    test "encodes cached_local_relation with hash" do
      hash = "abc123def456"

      {relation, counter} = PlanEncoder.encode_relation({:cached_local_relation, hash}, 10)

      assert counter == 11
      assert %Relation{rel_type: {:cached_local_relation, cached}} = relation
      assert %CachedLocalRelation{hash: ^hash} = cached
    end

    test "encodes chunked_cached_local_relation with data hashes and schema hash" do
      data_hashes = ["hash1", "hash2", "hash3"]
      schema_hash = "schema_hash_abc"

      {relation, counter} =
        PlanEncoder.encode_relation(
          {:chunked_cached_local_relation, data_hashes, schema_hash},
          0
        )

      assert counter == 1
      assert %Relation{rel_type: {:chunked_cached_local_relation, chunked}} = relation
      assert %ChunkedCachedLocalRelation{} = chunked
      assert chunked.dataHashes == data_hashes
      assert chunked.schemaHash == schema_hash
    end

    test "encodes chunked_cached_local_relation with nil schema hash" do
      {relation, _counter} =
        PlanEncoder.encode_relation(
          {:chunked_cached_local_relation, ["h1"], nil},
          0
        )

      assert %Relation{rel_type: {:chunked_cached_local_relation, chunked}} = relation
      assert chunked.dataHashes == ["h1"]
      assert chunked.schemaHash == nil
    end

    test "encode/2 wraps local_relation in a Plan" do
      data = <<0, 1, 2>>

      {plan, counter} = PlanEncoder.encode({:local_relation, data, "x INT"}, 0)

      assert counter == 1
      assert %Plan{op_type: {:root, relation}} = plan
      assert %Relation{rel_type: {:local_relation, _}} = relation
    end

    test "local_relation composes with limit" do
      data = <<1>>
      inner_plan = {:local_relation, data, "x INT"}
      plan = {:limit, inner_plan, 10}

      {%Plan{op_type: {:root, relation}}, counter} = PlanEncoder.encode(plan, 0)

      assert counter == 2
      assert {:limit, %Spark.Connect.Limit{limit: 10, input: child}} = relation.rel_type
      assert {:local_relation, %LocalRelation{data: ^data}} = child.rel_type
    end

    test "local_relation composes with filter" do
      data = <<1>>
      inner_plan = {:local_relation, data, nil}
      plan = {:filter, inner_plan, {:col, "x"}}

      {%Plan{op_type: {:root, relation}}, _counter} = PlanEncoder.encode(plan, 0)

      assert {:filter, %Spark.Connect.Filter{input: child}} = relation.rel_type
      assert {:local_relation, %LocalRelation{}} = child.rel_type
    end
  end
end
