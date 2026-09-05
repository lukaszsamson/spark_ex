defmodule SparkEx.Wave5ApiTest do
  use ExUnit.Case, async: true

  alias Spark.Connect.DataType
  alias SparkEx.Column
  alias SparkEx.Connect.TypeMapper
  alias SparkEx.DataFrame
  alias SparkEx.Functions, as: F
  alias SparkEx.GroupedData
  alias SparkEx.Window
  alias SparkEx.WindowSpec

  defp df, do: DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

  # Plans are wrapped in {:plan_id, n, inner}; strip the id for assertions.
  defp plan(%DataFrame{plan: {:plan_id, _, inner}}), do: inner
  defp plan(%DataFrame{plan: inner}), do: inner

  # ── T-38: shared column-name normalization ──

  describe "T-38 column name normalization" do
    test "Functions.col/1 accepts atoms and keeps star routing" do
      assert %Column{expr: {:col, "age"}} = F.col(:age)
      assert %Column{expr: {:col, "age"}} = F.col("age")
      assert %Column{expr: {:star}} = F.col("*")
      assert %Column{expr: {:star}} = F.col(:*)
      assert %Column{expr: {:star, "t.*"}} = F.col("t.*")
    end

    test "Functions.col/1 rejects nil and booleans" do
      for bad <- [nil, true, false] do
        assert_raise ArgumentError, ~r/column name/, fn -> F.col(bad) end
      end
    end

    test "with_column/3 and with_columns/2 accept atom names" do
      assert {:with_columns, _, [{:alias, {:lit, 1}, "a"}]} =
               plan(DataFrame.with_column(df(), :a, F.lit(1)))

      assert {:with_columns, _, aliases} =
               plan(DataFrame.with_columns(df(), [{:a, F.lit(1)}, {"b", 2}]))

      assert aliases == [{:alias, {:lit, 1}, "a"}, {:alias, {:lit, 2}, "b"}]
    end

    test "with_column/3 rejects nil names" do
      assert_raise ArgumentError, ~r/column name/, fn ->
        DataFrame.with_column(df(), nil, F.lit(1))
      end
    end

    test "select/order_by normalize atoms and reject nil" do
      assert {:project, _, [{:col, "a"}]} = plan(DataFrame.select(df(), [:a]))

      assert_raise ArgumentError, ~r/column name/, fn -> DataFrame.select(df(), [nil]) end
      assert_raise ArgumentError, ~r/column name/, fn -> DataFrame.order_by(df(), [nil]) end
    end

    test "WindowSpec partition_by/order_by accept atoms and reject nil" do
      spec = WindowSpec.partition_by(%WindowSpec{}, [:dept])
      assert spec.partition_spec == [{:col, "dept"}]

      assert_raise ArgumentError, ~r/column name/, fn ->
        WindowSpec.partition_by(%WindowSpec{}, [nil])
      end
    end

    test "GroupedData shortcuts accept atoms" do
      gd = DataFrame.group_by(df(), ["k"])
      assert %GroupedData{} = gd

      assert {:aggregate, _, _, _, agg_exprs} = plan(GroupedData.count(gd, :a))
      assert agg_exprs == [{:fn, "count", [{:col, "a"}], false}]
    end

    test "NA subsets accept atoms and reject nil" do
      assert {:na_drop, _, cols, _} = plan(DataFrame.NA.drop(df(), subset: [:a, "b"]))

      assert cols == ["a", "b"]

      assert_raise ArgumentError, ~r/column name/, fn ->
        DataFrame.NA.drop(df(), subset: [nil])
      end
    end

    test "NA.fill map keys accept atoms" do
      assert {:na_fill, _, cols, values} = plan(DataFrame.NA.fill(df(), %{a: 0}))
      assert cols == ["a"]
      assert values == [0]
    end
  end

  # ── T-56: sample/4 keyword guard ──

  describe "T-56 sample options" do
    test "positional seed form still works" do
      assert {:sample, _, +0.0, 0.5, false, 1234, false} = plan(DataFrame.sample(df(), 0.5, 1234))
    end

    test "non-keyword list opts raise a clear error" do
      assert_raise ArgumentError, ~r/keyword list/, fn ->
        DataFrame.sample(df(), 0.5, [1234])
      end

      assert_raise ArgumentError, ~r/keyword list/, fn ->
        DataFrame.sample(df(), true, 0.5, [1234])
      end
    end

    test "keyword opts still work" do
      assert {:sample, _, +0.0, 0.5, true, 7, false} =
               plan(DataFrame.sample(df(), true, 0.5, seed: 7))
    end
  end

  # ── T-57: frame bound clamping respects position ──

  describe "T-57 window frame clamping" do
    @max_long 9_223_372_036_854_775_807
    @min_long -9_223_372_036_854_775_808

    test "clamps only in the direction the bound faces" do
      spec = Window.rows_between(@min_long, @max_long)
      assert spec.frame_spec == {:rows, :unbounded_preceding, :unbounded_following}

      # A huge lower bound is NOT unbounded_following (PySpark leaves it literal).
      spec = Window.rows_between(@max_long, @max_long)
      assert {:rows, lower, :unbounded_following} = spec.frame_spec
      assert lower == @max_long

      # A tiny upper bound stays literal too.
      spec = Window.range_between(@min_long, @min_long)
      assert {:range, :unbounded_preceding, upper} = spec.frame_spec
      assert upper == @min_long
    end
  end

  # ── T-58: log/2, uuid/0, sort/3 ──

  describe "T-58 function parity" do
    test "log/2 accepts a numeric literal Column base and rejects other Columns" do
      assert %Column{expr: {:fn, "log", [{:lit, 2}, {:col, "x"}], false}} =
               F.log(F.lit(2), "x")

      assert_raise ArgumentError, ~r/numeric literal/, fn -> F.log(F.col("b"), "x") end
    end

    test "uuid/0 bakes a random long seed like PySpark" do
      assert %Column{expr: {:fn, "uuid", [{:lit, seed}], false}} = F.uuid()
      assert is_integer(seed)

      # Two calls should (essentially always) differ.
      assert %Column{expr: {:fn, "uuid", [{:lit, seed2}], false}} = F.uuid()
      assert is_integer(seed2)
    end

    test "DataFrame.sort/3 mirrors order_by/3 options" do
      {:sort, _, sort_exprs} = plan(DataFrame.sort(df(), ["a"], ascending: false))
      {:sort, _, order_by_exprs} = plan(DataFrame.order_by(df(), ["a"], ascending: false))
      assert sort_exprs == order_by_exprs

      assert {:sort, _, [{:sort_order, {:col, "a"}, :desc, :nulls_last}]} =
               plan(DataFrame.sort(df(), ["a"], ascending: false))
    end
  end

  # ── T-58/T-38: Window.partition_by/2 ──

  test "Window.partition_by/2 extends an existing spec" do
    spec =
      Window.order_by(["ts"])
      |> Window.partition_by(["dept"])

    assert spec.partition_spec == [{:col, "dept"}]
    assert [{:sort_order, {:col, "ts"}, _, _}] = spec.order_spec
  end

  # ── T-59: TypeMapper.simple_string/1 ──

  describe "T-59 simple_string/1" do
    defp dt(kind), do: %DataType{kind: kind}

    test "primitive types match PySpark simpleString" do
      assert TypeMapper.simple_string(dt({:null, %DataType.NULL{}})) == "void"
      assert TypeMapper.simple_string(dt({:boolean, %DataType.Boolean{}})) == "boolean"
      assert TypeMapper.simple_string(dt({:byte, %DataType.Byte{}})) == "tinyint"
      assert TypeMapper.simple_string(dt({:short, %DataType.Short{}})) == "smallint"
      assert TypeMapper.simple_string(dt({:integer, %DataType.Integer{}})) == "int"
      assert TypeMapper.simple_string(dt({:long, %DataType.Long{}})) == "bigint"
      assert TypeMapper.simple_string(dt({:float, %DataType.Float{}})) == "float"
      assert TypeMapper.simple_string(dt({:double, %DataType.Double{}})) == "double"
      assert TypeMapper.simple_string(dt({:string, %DataType.String{}})) == "string"
      assert TypeMapper.simple_string(dt({:binary, %DataType.Binary{}})) == "binary"
      assert TypeMapper.simple_string(dt({:date, %DataType.Date{}})) == "date"
      assert TypeMapper.simple_string(dt({:timestamp, %DataType.Timestamp{}})) == "timestamp"

      assert TypeMapper.simple_string(dt({:timestamp_ntz, %DataType.TimestampNTZ{}})) ==
               "timestamp_ntz"

      assert TypeMapper.simple_string(nil) == "void"
    end

    test "parametrized types" do
      assert TypeMapper.simple_string(dt({:decimal, %DataType.Decimal{precision: 10, scale: 2}})) ==
               "decimal(10,2)"

      assert TypeMapper.simple_string(dt({:char, %DataType.Char{length: 5}})) == "char(5)"

      assert TypeMapper.simple_string(dt({:var_char, %DataType.VarChar{length: 7}})) ==
               "varchar(7)"

      assert TypeMapper.simple_string(dt({:string, %DataType.String{collation: "UNICODE"}})) ==
               "string collate UNICODE"
    end

    test "nested types" do
      str = dt({:string, %DataType.String{}})
      int = dt({:integer, %DataType.Integer{}})

      assert TypeMapper.simple_string(dt({:array, %DataType.Array{element_type: str}})) ==
               "array<string>"

      assert TypeMapper.simple_string(dt({:map, %DataType.Map{key_type: str, value_type: int}})) ==
               "map<string,int>"

      struct =
        dt(
          {:struct,
           %DataType.Struct{
             fields: [
               %DataType.StructField{name: "a", data_type: int},
               %DataType.StructField{name: "b", data_type: str}
             ]
           }}
        )

      assert TypeMapper.simple_string(struct) == "struct<a:int,b:string>"
    end

    test "interval and variant types" do
      assert TypeMapper.simple_string(
               dt({:day_time_interval, %DataType.DayTimeInterval{start_field: 0, end_field: 3}})
             ) == "interval day to second"

      assert TypeMapper.simple_string(
               dt(
                 {:year_month_interval, %DataType.YearMonthInterval{start_field: 0, end_field: 0}}
               )
             ) == "interval year"

      assert TypeMapper.simple_string(dt({:calendar_interval, %DataType.CalendarInterval{}})) ==
               "interval"

      assert TypeMapper.simple_string(dt({:variant, %DataType.Variant{}})) == "variant"
    end

    test "udt renders as \"udt\" like PySpark's UserDefinedType.simpleString" do
      sql_type = dt({:long, %DataType.Long{}})

      assert TypeMapper.simple_string(dt({:udt, %DataType.UDT{type: "udt", sql_type: sql_type}})) ==
               "udt"

      assert TypeMapper.simple_string(dt({:udt, %DataType.UDT{type: "udt"}})) == "udt"
    end

    test "unparsed types render their own string" do
      assert TypeMapper.simple_string(
               dt({:unparsed, %DataType.Unparsed{data_type_string: "INTERVAL YEAR"}})
             ) == "interval year"
    end
  end
end
