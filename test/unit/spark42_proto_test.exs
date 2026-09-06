defmodule SparkEx.Unit.Spark42ProtoTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.{
    AnalyzeTable,
    Catalog,
    CreateDatabase,
    DropDatabase,
    DropTable,
    DropView,
    GetCreateTableString,
    GetStatusRequest,
    GetTableProperties,
    ListPartitions,
    ListViews,
    NearestByJoin,
    Parse,
    Relation,
    RelationChanges,
    TruncateTable,
    WriteOperation,
    WriteOperationV2,
    WriteStreamOperationStart
  }

  alias Spark.Connect.ExecutePlanResponse.ObservedMetrics
  alias Spark.Connect.FetchErrorDetailsResponse.Error
  alias Spark.Connect.GetStatusRequest.OperationStatusRequest
  alias Spark.Connect.Read.DataSource

  describe "new relation and catalog oneofs" do
    test "encodes every new catalog case at its pinned v4.2 tag" do
      catalog_cases = [
        {:drop_table, DropTable, <<0xDA, 0x01, 0>>},
        {:drop_view, DropView, <<0xE2, 0x01, 0>>},
        {:create_database, CreateDatabase, <<0xEA, 0x01, 0>>},
        {:drop_database, DropDatabase, <<0xF2, 0x01, 0>>},
        {:list_partitions, ListPartitions, <<0xFA, 0x01, 0>>},
        {:list_views, ListViews, <<0x82, 0x02, 0>>},
        {:get_table_properties, GetTableProperties, <<0x8A, 0x02, 0>>},
        {:get_create_table_string, GetCreateTableString, <<0x92, 0x02, 0>>},
        {:truncate_table, TruncateTable, <<0x9A, 0x02, 0>>},
        {:analyze_table, AnalyzeTable, <<0xA2, 0x02, 0>>}
      ]

      for {field, module, wire} <- catalog_cases do
        encoded = Protobuf.encode(%Catalog{cat_type: {field, struct(module)}})

        assert encoded == wire

        assert %Catalog{cat_type: {^field, %{__struct__: ^module}}} =
                 Protobuf.decode(encoded, Catalog)
      end
    end

    test "encodes v4.2 relation cases at fields 46 and 47" do
      relation_changes = %RelationChanges{unparsed_identifier: "orders", is_streaming: true}

      assert Protobuf.encode(%Relation{rel_type: {:relation_changes, relation_changes}}) ==
               <<0xF2, 0x02, 10, 0x0A, 6, "orders", 0x18, 1>>

      assert %Relation{rel_type: {:relation_changes, ^relation_changes}} =
               Protobuf.decode(
                 <<0xF2, 0x02, 10, 0x0A, 6, "orders", 0x18, 1>>,
                 Relation
               )

      assert Protobuf.encode(%Relation{
               rel_type: {:nearest_by_join, %NearestByJoin{num_results: 1}}
             }) ==
               <<0xFA, 0x02, 2, 0x20, 1>>
    end
  end

  describe "v4.2 write and read fields" do
    test "write schema-evolution flags use their pinned wire fields and retain false defaults" do
      assert Protobuf.encode(%WriteOperation{}) == <<>>
      assert Protobuf.encode(%WriteOperation{with_schema_evolution: true}) == <<0x58, 1>>
      assert Protobuf.decode(<<0x58, 1>>, WriteOperation).with_schema_evolution

      assert Protobuf.encode(%WriteOperationV2{}) == <<>>
      assert Protobuf.encode(%WriteOperationV2{with_schema_evolution: true}) == <<0x50, 1>>
      assert Protobuf.decode(<<0x50, 1>>, WriteOperationV2).with_schema_evolution
    end

    test "source name and XML format preserve optional and enum semantics" do
      assert Protobuf.encode(%DataSource{}) == <<>>
      assert Protobuf.decode(<<>>, DataSource).source_name == nil

      assert Protobuf.encode(%DataSource{source_name: "stable"}) == <<0x32, 6, "stable">>
      assert Protobuf.decode(<<0x32, 6, "stable">>, DataSource).source_name == "stable"

      assert Protobuf.encode(%Parse{}) == <<>>
      assert Protobuf.decode(<<>>, Parse).format == :PARSE_FORMAT_UNSPECIFIED
      assert Protobuf.encode(%Parse{format: :PARSE_FORMAT_XML}) == <<0x10, 3>>
      assert Protobuf.decode(<<0x10, 3>>, Parse).format == :PARSE_FORMAT_XML
    end

    test "real-time batch duration is the field-100 trigger oneof" do
      message = %WriteStreamOperationStart{
        trigger: {:real_time_batch_duration, "500 ms"}
      }

      assert Protobuf.encode(message) == <<0xA2, 0x06, 6, "500 ms">>
      assert Protobuf.decode(<<0xA2, 0x06, 6, "500 ms">>, WriteStreamOperationStart) == message
      assert Protobuf.decode(<<>>, WriteStreamOperationStart).trigger == nil
    end
  end

  describe "v4.2 optional message fields" do
    test "GetStatus preserves an explicitly present empty operation-status request" do
      request = %GetStatusRequest{operation_status: %OperationStatusRequest{}}

      assert Protobuf.encode(%GetStatusRequest{}) == <<>>
      assert Protobuf.encode(request) == <<0x2A, 0>>
      assert Protobuf.decode(<<0x2A, 0>>, GetStatusRequest) == request
    end

    test "observed metrics preserves a root error index of zero" do
      metrics = %ObservedMetrics{
        root_error_idx: 0,
        errors: [%Error{message: "oops"}]
      }

      assert Protobuf.encode(%ObservedMetrics{}) == <<>>
      assert Protobuf.encode(metrics) == <<0x28, 0, 0x32, 6, 0x12, 4, "oops">>
      assert Protobuf.decode(<<0x28, 0, 0x32, 6, 0x12, 4, "oops">>, ObservedMetrics) == metrics
    end
  end
end
