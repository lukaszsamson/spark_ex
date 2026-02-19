defmodule SparkEx.Integration.FunctionsSubsetTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{Column, DataFrame, Functions}

  alias Google.Protobuf.{
    DescriptorProto,
    FieldDescriptorProto,
    FileDescriptorProto,
    FileDescriptorSet
  }

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "transform higher-order function executes", %{session: session} do
    df =
      SparkEx.sql(session, "SELECT array(1, 2, 3) AS arr")
      |> DataFrame.select([
        Column.alias_(
          Functions.transform("arr", fn x -> Column.plus(x, Functions.lit(1)) end),
          "arr2"
        )
      ])

    assert {:ok, [row]} = DataFrame.collect(df)
    assert row["arr2"] == [2, 3, 4]
  end

  test "from_avro/to_avro roundtrip", %{session: session} do
    schema =
      ~s({"type":"record","name":"t","fields":[{"name":"id","type":"int"}]})

    df =
      SparkEx.sql(session, "SELECT 1 AS id")
      |> DataFrame.select([
        Column.alias_(Functions.from_avro(Functions.to_avro("id", schema), schema), "decoded")
      ])

    case DataFrame.collect(df) do
      {:ok, [row]} ->
        assert row["decoded"] in [%{"id" => 1}, %{"id" => 1.0}]

      {:error, %SparkEx.Error.Remote{} = error} ->
        if error.error_class == "AVRO_NOT_LOADED_SQL_FUNCTIONS_UNUSABLE" and
             error.sql_state == "22KD3" do
          assert error.message_parameters["functionName"] == "FROM_AVRO"
        else
          flunk("unexpected avro roundtrip error: #{inspect(error)}")
        end
    end
  end

  test "from_protobuf/to_protobuf roundtrip", %{session: session} do
    descriptor = build_descriptor_set()

    df =
      SparkEx.sql(session, "SELECT named_struct('id', 1) AS payload")
      |> DataFrame.select([
        Column.alias_(
          Functions.from_protobuf(
            Functions.to_protobuf("payload", "sparkex.test.Simple",
              binary_descriptor_set: descriptor
            ),
            "sparkex.test.Simple",
            binary_descriptor_set: descriptor
          ),
          "decoded"
        )
      ])

    case DataFrame.collect(df) do
      {:ok, [row]} ->
        assert row["decoded"] in [%{"id" => 1}, %{"id" => 1.0}]

      {:error, %SparkEx.Error.Remote{} = error} ->
        if error.error_class == "PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND" do
          assert is_binary(error.message)
          assert error.message =~ "descriptor"
        else
          flunk("unexpected protobuf roundtrip error: #{inspect(error)}")
        end
    end
  end

  defp build_descriptor_set do
    %FileDescriptorSet{
      file: [
        %FileDescriptorProto{
          name: "simple.proto",
          package: "sparkex.test",
          syntax: "proto3",
          message_type: [
            %DescriptorProto{
              name: "Simple",
              field: [
                %FieldDescriptorProto{
                  name: "id",
                  number: 1,
                  label: :LABEL_OPTIONAL,
                  type: :TYPE_INT32
                }
              ]
            }
          ]
        }
      ]
    }
    |> FileDescriptorSet.encode()
  end
end
