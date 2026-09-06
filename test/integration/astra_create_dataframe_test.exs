defmodule SparkEx.Integration.AstraCreateDataFrameTest do
  # End-to-end reproductions of the createDataFrame findings from the
  # 2026-09-06 review; every assertion is the PySpark 4.1 result for the
  # same input.
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.DataFrame
  alias SparkEx.Types

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  defp field_summaries(df) do
    {:ok, %Spark.Connect.DataType{kind: {:struct, %{fields: fields}}}} = DataFrame.schema(df)
    Enum.map(fields, &{&1.name, &1.nullable, &1.metadata})
  end

  describe "finding 1: field metadata does not reorder values" do
    test "metadata-bearing schema keeps values on their columns", %{session: session} do
      schema =
        Types.struct_type([
          Types.struct_field("b", :long, metadata: %{"note" => "b"}),
          Types.struct_field("a", :long)
        ])

      {:ok, df} = SparkEx.create_dataframe(session, [%{"a" => 1, "b" => 2}], schema: schema)

      assert {:ok, ["b", "a"]} = DataFrame.columns(df)
      assert {:ok, [%{"a" => 1, "b" => 2}]} = DataFrame.collect(df)
      assert [{"b", true, meta}, {"a", true, _}] = field_summaries(df)
      assert Jason.decode!(meta) == %{"note" => "b"}
    end

    test "a field missing from every row becomes a null column", %{session: session} do
      schema =
        Types.struct_type([
          Types.struct_field("b", :long, metadata: %{"note" => "b"}),
          Types.struct_field("a", :long)
        ])

      {:ok, df} = SparkEx.create_dataframe(session, [%{"b" => 2}], schema: schema)

      assert {:ok, ["b", "a"]} = DataFrame.columns(df)
      assert {:ok, [%{"a" => nil, "b" => 2}]} = DataFrame.collect(df)
    end

    test "a schema with fewer fields than the map projects the extra keys away",
         %{session: session} do
      schema = Types.struct_type([Types.struct_field("b", :long, metadata: %{"n" => "b"})])

      {:ok, df} = SparkEx.create_dataframe(session, [%{"a" => 1, "b" => 2}], schema: schema)

      assert {:ok, ["b"]} = DataFrame.columns(df)
      assert {:ok, [%{"b" => 2}]} = DataFrame.collect(df)
    end
  end

  describe "finding 2: duplicate requested names keep both values" do
    test "positional rows with schema [x, x]", %{session: session} do
      {:ok, df} = SparkEx.create_dataframe(session, [{1, 2}], schema: ["x", "x"])

      assert {:ok, ["x", "x"]} = DataFrame.columns(df)
      # The client disambiguates duplicate output names during collection.
      assert {:ok, [%{"x" => 1, "x_1" => 2}]} = DataFrame.collect(df)
    end
  end

  describe "duplicate names with DDL or struct schemas" do
    test "are rejected instead of silently dropping a value", %{session: session} do
      assert {:error, {:invalid_schema, message}} =
               SparkEx.create_dataframe(session, [{1, 2}], schema: "x INT, x INT")

      assert message =~ "duplicate column names"
    end
  end

  describe "inferred decimals that do not fit DECIMAL(38,18)" do
    test "are rejected rather than nulled or rounded", %{session: session} do
      assert {:error, {:invalid_data, message}} =
               SparkEx.create_dataframe(session, [
                 %{"d" => Decimal.new("123456789012345678901")}
               ])

      assert message =~ "DECIMAL(38,18)"

      assert {:error, {:invalid_data, _}} =
               SparkEx.create_dataframe(session, [%{"d" => Decimal.new("1.1234567890123456789")}])

      {:ok, df} = SparkEx.create_dataframe(session, [%{"d" => Decimal.new("1.20")}])
      assert {:ok, [%{"d" => d}]} = DataFrame.collect(df)
      assert Decimal.equal?(d, Decimal.new("1.20"))
    end
  end

  describe "finding 3: binary values on the JSON path" do
    test "nested BINARY struct field round-trips", %{session: session} do
      {:ok, df} =
        SparkEx.create_dataframe(session, [%{"s" => %{"b" => "abcd"}}],
          schema: "s STRUCT<b:BINARY>"
        )

      assert {:ok, [%{"s" => %{"b" => "abcd"}}]} = DataFrame.collect(df)
    end

    test "Explorer binary series next to a list column round-trips", %{session: session} do
      frame =
        Explorer.DataFrame.new(
          bin: Explorer.Series.from_list(["abcd"], dtype: :binary),
          arr: Explorer.Series.from_list([[1]])
        )

      {:ok, df} = SparkEx.create_dataframe(session, frame)
      assert {:ok, [%{"arr" => [1], "bin" => "abcd"}]} = DataFrame.collect(df)
    end
  end

  describe "finding 4: non-finite floats next to a complex column" do
    test "NaN / Infinity / -Infinity collect as the sentinel atoms", %{session: session} do
      {:ok, df} =
        SparkEx.create_dataframe(session, [
          %{"d" => :nan, "a" => [1]},
          %{"d" => :infinity, "a" => [2]},
          %{"d" => :neg_infinity, "a" => [3]}
        ])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["d"]) == [:nan, :infinity, :neg_infinity]
    end
  end

  describe "finding 5: nested maps with non-string keys" do
    test "MAP<INT, STRING> inside a struct", %{session: session} do
      {:ok, df} =
        SparkEx.create_dataframe(session, [%{"s" => %{"m" => %{1 => "a"}}}],
          schema: "s STRUCT<m:MAP<INT,STRING>>"
        )

      assert {:ok, [%{"s" => %{"m" => [%{"key" => 1, "value" => "a"}]}}]} = DataFrame.collect(df)
      assert {:ok, [{"s", "struct<m:map<int,string>>"}]} = DataFrame.dtypes(df)
    end

    test "ARRAY<MAP<INT, STRING>>", %{session: session} do
      {:ok, df} =
        SparkEx.create_dataframe(session, [%{"a" => [%{1 => "x"}, %{2 => "y"}]}],
          schema: "a ARRAY<MAP<INT, STRING>>"
        )

      assert {:ok, [%{"a" => [[%{"key" => 1, "value" => "x"}], [%{"key" => 2, "value" => "y"}]]}]} =
               DataFrame.collect(df)
    end
  end

  describe "finding 6: quoted identifiers survive the map rewrite" do
    test "a field named `a b`", %{session: session} do
      {:ok, df} =
        SparkEx.create_dataframe(session, [%{"a b" => %{1 => "x"}}],
          schema: "`a b` MAP<INT,STRING>"
        )

      assert {:ok, ["a b"]} = DataFrame.columns(df)
      assert {:ok, [%{"a b" => [%{"key" => 1, "value" => "x"}]}]} = DataFrame.collect(df)
    end
  end

  describe "finding 7: explicit column-name list order" do
    test "map rows keep the supplied name order", %{session: session} do
      {:ok, df} = SparkEx.create_dataframe(session, [%{"a" => 1, "b" => 2}], schema: ["z", "a"])

      assert {:ok, ["z", "a"]} = DataFrame.columns(df)
      assert {:ok, [%{"z" => 1, "a" => 2}]} = DataFrame.collect(df)
    end
  end

  describe "finding 8: non-null schema constraints" do
    test "a nil value for a non-nullable field is rejected locally", %{session: session} do
      schema = Types.struct_type([Types.struct_field("id", :long, nullable: false)])

      assert {:error, {:invalid_data, message}} =
               SparkEx.create_dataframe(session, [%{"id" => nil}], schema: schema)

      assert message =~ "must not be nil"
    end

    test "valid data with a non-nullable, metadata-bearing field collects", %{session: session} do
      schema =
        Types.struct_type([
          Types.struct_field("id", :long, nullable: false, metadata: %{"c" => "x"})
        ])

      {:ok, df} = SparkEx.create_dataframe(session, [%{"id" => 1}], schema: schema)
      assert {:ok, [%{"id" => 1}]} = DataFrame.collect(df)
      assert [{"id", _nullable, meta}] = field_summaries(df)
      assert Jason.decode!(meta) == %{"c" => "x"}
    end
  end

  describe "finding 9: empty data with a metadata-bearing schema" do
    test "returns an empty DataFrame with the column and its metadata", %{session: session} do
      schema =
        Types.struct_type([Types.struct_field("id", :long, metadata: %{"foo" => "bar"})])

      {:ok, df} = SparkEx.create_dataframe(session, [], schema: schema)

      assert {:ok, ["id"]} = DataFrame.columns(df)
      assert {:ok, []} = DataFrame.collect(df)
      assert {:ok, 0} = DataFrame.count(df)
      assert [{"id", true, meta}] = field_summaries(df)
      assert Jason.decode!(meta) == %{"foo" => "bar"}
    end

    test "nullability is preserved on the empty relation", %{session: session} do
      schema = Types.struct_type([Types.struct_field("id", :long, nullable: false)])

      {:ok, df} = SparkEx.create_dataframe(session, [], schema: schema)
      assert [{"id", false, _}] = field_summaries(df)
    end
  end

  describe "finding 11: non-finite doubles under JSON projection" do
    test "a nested map column does not turn NaN / Infinity into strings", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT array(cast('NaN' as double), cast('Infinity' as double), " <>
            "cast('-Infinity' as double)) AS a, array('NaN') AS s, " <>
            "map('outer', map('inner', 1)) AS m"
        )

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["a"] == [:nan, :infinity, :neg_infinity]
      assert row["s"] == ["NaN"]
    end
  end

  describe "finding 13: Decimal inference" do
    test "local rows infer decimal(38,18) like PySpark", %{session: session} do
      {:ok, df} = SparkEx.create_dataframe(session, [%{"d" => Decimal.new("1.20")}])

      assert {:ok, [{"d", "decimal(38,18)"}]} = DataFrame.dtypes(df)
      assert {:ok, [%{"d" => d}]} = DataFrame.collect(df)
      assert Decimal.equal?(d, Decimal.new("1.20"))
    end

    test "an integral Decimal also infers decimal(38,18)", %{session: session} do
      {:ok, df} = SparkEx.create_dataframe(session, [%{"d" => Decimal.new("1")}])
      assert {:ok, [{"d", "decimal(38,18)"}]} = DataFrame.dtypes(df)
    end
  end
end
