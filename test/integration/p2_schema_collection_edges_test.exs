defmodule SparkEx.Integration.P2SchemaCollectionEdgesTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Session}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "an explicit nested array schema preserves empty inner and outer arrays", %{
    session: session
  } do
    rows = [%{"nested" => [[], [1, 2]]}, %{"nested" => []}]

    assert {:ok, df} =
             SparkEx.create_dataframe(session, rows, schema: "nested ARRAY<ARRAY<INT>>")

    assert {:ok, [%{"nested" => [[], [1, 2]]}, %{"nested" => []}]} = DataFrame.collect(df)
    assert {:ok, [{"nested", "array<array<int>>"}]} = DataFrame.dtypes(df)
  end

  test "explicit TIMESTAMP and TIMESTAMP_NTZ schemas remain authoritative", %{session: session} do
    instant = DateTime.from_naive!(~N[2024-01-02 03:04:05.123456], "Etc/UTC")
    local = ~N[2024-01-02 03:04:05.123456]

    assert {:ok, df} =
             SparkEx.create_dataframe(session, [%{"ts" => instant, "ntz" => local}],
               schema: "ts TIMESTAMP, ntz TIMESTAMP_NTZ"
             )

    assert {:ok, [%{"ts" => %DateTime{} = timestamp, "ntz" => ^local}]} = DataFrame.collect(df)
    assert DateTime.to_unix(timestamp, :microsecond) == DateTime.to_unix(instant, :microsecond)
    assert {:ok, [{"ts", "timestamp"}, {"ntz", "timestamp_ntz"}]} = DataFrame.dtypes(df)

    assert {:ok, %Spark.Connect.DataType{kind: {:struct, struct}}} = DataFrame.schema(df)

    assert Enum.map(struct.fields, &{&1.name, elem(&1.data_type.kind, 0)}) == [
             {"ts", :timestamp},
             {"ntz", :timestamp_ntz}
           ]
  end

  test "duplicate nested field names retain both values during collection", %{session: session} do
    df = SparkEx.sql(session, "SELECT named_struct('x', 1, 'x', 2) AS s")

    assert {:ok, %Spark.Connect.DataType{kind: {:struct, struct}}} = DataFrame.schema(df)
    [field] = struct.fields
    assert ["x", "x"] = Enum.map(elem(field.data_type.kind, 1).fields, & &1.name)

    assert {:ok, [%{"s" => %{"x_0" => 1, "x_1" => 2}}]} = DataFrame.collect(df)
  end
end
