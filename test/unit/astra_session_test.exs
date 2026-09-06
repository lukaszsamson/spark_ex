defmodule SparkEx.Unit.AstraSessionTest do
  # Unit coverage for the createDataFrame local-data findings from the
  # 2026-09-06 review (metadata-bearing schemas, duplicate names, binary /
  # non-finite / non-string-map JSON encoding, nullability, decimals) and the
  # JSON-projection decoder's non-finite float handling.
  use ExUnit.Case, async: true

  alias Spark.Connect.DataType
  alias SparkEx.Session
  alias SparkEx.Types

  defp meta_schema(fields), do: Types.struct_type(fields)

  defp json_fields(json) do
    {:ok, %{"fields" => fields}} = Jason.decode(json)
    fields
  end

  defp dt(kind), do: %DataType{kind: kind}

  defp proto_struct(fields) do
    dt(
      {:struct,
       %DataType.Struct{
         fields:
           Enum.map(fields, fn {name, type} ->
             %DataType.StructField{name: name, data_type: type, nullable: true}
           end)
       }}
    )
  end

  defp proto_array(type),
    do: dt({:array, %DataType.Array{element_type: type, contains_null: true}})

  defp proto_double, do: dt({:double, %DataType.Double{}})
  defp proto_float, do: dt({:float, %DataType.Float{}})
  defp proto_string, do: dt({:string, %DataType.String{}})

  describe "metadata-bearing struct schemas build rows in field order (finding 1)" do
    test "map rows are projected onto the schema fields positionally" do
      schema =
        meta_schema([
          Types.struct_field("b", :long, metadata: %{"note" => "b"}),
          Types.struct_field("a", :long)
        ])

      assert {:ok, {:local_relation, ipc, json, df}} =
               Session.__prepare_local_data__([%{"a" => 1, "b" => 2}], schema: schema)

      assert is_binary(ipc)
      assert Explorer.DataFrame.names(df) == ["b", "a"]
      assert Explorer.Series.to_list(df["b"]) == [2]
      assert Explorer.Series.to_list(df["a"]) == [1]

      assert [%{"name" => "b", "metadata" => %{"note" => "b"}}, %{"name" => "a"}] =
               json_fields(json)
    end

    test "extra map keys are dropped and missing fields become null" do
      schema =
        meta_schema([
          Types.struct_field("b", :long, metadata: %{"note" => "b"}),
          Types.struct_field("missing", :string)
        ])

      assert {:ok, {:local_relation, _ipc, _json, df}} =
               Session.__prepare_local_data__([%{"a" => 1, "b" => 2}], schema: schema)

      assert Explorer.DataFrame.names(df) == ["b", "missing"]
      assert Explorer.Series.to_list(df["missing"]) == [nil]
    end

    test "tuple rows keep the schema order too" do
      schema =
        meta_schema([
          Types.struct_field("b", :long, metadata: %{"note" => "b"}),
          Types.struct_field("a", :long)
        ])

      assert {:ok, {:local_relation, _ipc, _json, df}} =
               Session.__prepare_local_data__([{2, 1}], schema: schema)

      assert Explorer.DataFrame.names(df) == ["b", "a"]
      assert Explorer.Series.to_list(df["b"]) == [2]
    end
  end

  describe "empty data with an explicit struct schema (finding 9)" do
    test "metadata-bearing schema yields an empty typed relation" do
      schema =
        meta_schema([Types.struct_field("id", :long, metadata: %{"foo" => "bar"})])

      assert {:ok, {:local_relation, nil, json, nil}} =
               Session.__prepare_local_data__([], schema: schema)

      assert [%{"name" => "id", "type" => "long", "metadata" => %{"foo" => "bar"}}] =
               json_fields(json)
    end

    test "nullability and container flags survive on the empty relation" do
      schema =
        meta_schema([
          Types.struct_field("id", :long, nullable: false),
          Types.struct_field("tags", Types.array_type(:string, contains_null: false))
        ])

      assert {:ok, {:local_relation, nil, json, nil}} =
               Session.__prepare_local_data__([], schema: schema)

      assert [
               %{"name" => "id", "nullable" => false},
               %{"name" => "tags", "type" => %{"containsNull" => false}}
             ] = json_fields(json)
    end

    test "a DDL string schema keeps the from_json empty relation" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([], schema: "id BIGINT")

      assert query =~ "WHERE 1 = 0"
    end
  end

  describe "duplicate requested column names (finding 2)" do
    test "positional rows are built under unique names and renamed afterwards" do
      assert {:ok, {:rename_columns, {:local_relation, _ipc, _schema, df}, ["x", "x"]}} =
               Session.__prepare_local_data__([{1, 2}], schema: ["x", "x"])

      assert [first, second] = Explorer.DataFrame.names(df)
      assert first != second
      assert Explorer.Series.to_list(df[first]) == [1]
      assert Explorer.Series.to_list(df[second]) == [2]
    end

    test "list rows behave like tuple rows" do
      assert {:ok, {:rename_columns, {:local_relation, _ipc, _schema, _df}, ["x", "x"]}} =
               Session.__prepare_local_data__([[1, 2]], schema: ["x", "x"])
    end

    test "unique names are untouched" do
      assert {:ok, {:local_relation, _ipc, _schema, df}} =
               Session.__prepare_local_data__([{1, 2}], schema: ["x", "y"])

      assert Explorer.DataFrame.names(df) == ["x", "y"]
    end
  end

  describe "explicit column-name list order for map rows (finding 7)" do
    test "the supplied order wins over the sorted key order" do
      assert {:ok, {:local_relation, _ipc, schema, df}} =
               Session.__prepare_local_data__([%{"a" => 1, "b" => 2}], schema: ["z", "a"])

      assert Explorer.DataFrame.names(df) == ["z", "a"]
      # Values are assigned by sorted-key position (PySpark: sorted dict items).
      assert Explorer.Series.to_list(df["z"]) == [1]
      assert Explorer.Series.to_list(df["a"]) == [2]
      assert schema =~ ~r/\Az\s/
    end

    test "keyword rows follow the same rule" do
      assert {:ok, {:local_relation, _ipc, _schema, df}} =
               Session.__prepare_local_data__([[a: 1, b: 2]], schema: ["z", "a"])

      assert Explorer.DataFrame.names(df) == ["z", "a"]
    end

    test "duplicate names in the list are honoured for map rows" do
      assert {:ok, {:rename_columns, {:local_relation, _ipc, _schema, df}, ["x", "x"]}} =
               Session.__prepare_local_data__([%{"a" => 1, "b" => 2}], schema: ["x", "x"])

      assert [first, second] = Explorer.DataFrame.names(df)
      assert Explorer.Series.to_list(df[first]) == [1]
      assert Explorer.Series.to_list(df[second]) == [2]
    end
  end

  describe "schema-directed binary encoding on the JSON path (finding 3)" do
    test "nested BINARY leaves are base64-encoded" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([%{"s" => %{"b" => "abcd"}}],
                 schema: "s STRUCT<b:BINARY>"
               )

      assert query =~ Base.encode64("abcd")
      refute query =~ ~s("b":"abcd")
    end

    test "BINARY inside arrays and map values is encoded" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__(
                 [%{"arr" => ["abcd"], "m" => %{"k" => "abcd"}}],
                 schema: "arr ARRAY<BINARY>, m MAP<STRING, BINARY>"
               )

      assert length(Regex.scan(~r/YWJjZA==/, query)) == 2
    end

    test "STRING leaves are never encoded" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([%{"s" => %{"t" => "abcd"}}],
                 schema: "s STRUCT<t: STRING>"
               )

      assert query =~ ~s("t":"abcd")
      refute query =~ Base.encode64("abcd")
    end

    test "Explorer binary series survive the complex-dtype JSON fallback" do
      frame =
        Explorer.DataFrame.new(
          bin: Explorer.Series.from_list(["abcd"], dtype: :binary),
          arr: Explorer.Series.from_list([[1]])
        )

      assert {:ok, {:sql_relation, query, nil}} = Session.__prepare_local_data__(frame)
      assert query =~ ~s("bin":"#{Base.encode64("abcd")}")
    end
  end

  describe "non-finite floats on the Explorer/complex JSON path (finding 4)" do
    test "the sentinel atoms become Spark's JSON spellings" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([
                 %{"d" => :nan, "a" => [1]},
                 %{"d" => :infinity, "a" => [2]},
                 %{"d" => :neg_infinity, "a" => [3]}
               ])

      assert query =~ ~s("d":"NaN")
      assert query =~ ~s("d":"Infinity")
      assert query =~ ~s("d":"-Infinity")
      refute query =~ ~s("nan")
      refute query =~ ~s("neg_infinity")
    end
  end

  describe "nested non-string-keyed maps (finding 5)" do
    test "the entry-array rewrite recurses into structs" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([%{"s" => %{"m" => %{1 => "a"}}}],
                 schema: "s STRUCT<m:MAP<INT,STRING>>"
               )

      assert query =~ "s STRUCT<m: ARRAY<STRUCT<key: INT, value: STRING>>>"
      assert query =~ ~s("m":[{"key":1,"value":"a"}])
      assert query =~ "map_from_entries(parsed.`s`.`m`)"
      assert query =~ "named_struct('m'"
    end

    test "arrays of integer-keyed maps are rebuilt element-wise" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([%{"a" => [%{1 => "x"}]}],
                 schema: "a ARRAY<MAP<INT, STRING>>"
               )

      assert query =~ "a ARRAY<ARRAY<STRUCT<key: INT, value: STRING>>>"
      assert query =~ "transform(parsed.`a`, _spark_ex_e0 -> map_from_entries(_spark_ex_e0))"
    end

    test "string-keyed maps stay JSON objects" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([%{"m" => %{"k" => 1}}],
                 schema: "m MAP<STRING, INT>"
               )

      assert query =~ ~s("m":{"k":1})
      refute query =~ "map_from_entries"
    end
  end

  describe "identifier quoting in the rewritten schema and projection (finding 6)" do
    test "names with spaces are re-quoted" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([%{"a b" => %{1 => "x"}}],
                 schema: "`a b` MAP<INT,STRING>"
               )

      assert query =~ "`a b` ARRAY<STRUCT<key: INT, value: STRING>>"
      assert query =~ "map_from_entries(parsed.`a b`) AS `a b`"
    end

    test "embedded backticks are escaped by doubling" do
      assert {:ok, {:sql_relation, query, nil}} =
               Session.__prepare_local_data__([%{"a`b" => %{1 => "x"}}],
                 schema: "`a``b` MAP<INT,STRING>"
               )

      assert query =~ "`a``b` ARRAY<STRUCT<key: INT, value: STRING>>"
      assert query =~ "map_from_entries(parsed.`a``b`) AS `a``b`"
    end
  end

  describe "non-null constraints are validated locally (finding 8)" do
    test "a nil top-level value for a non-nullable field is rejected" do
      schema = Types.struct_type([Types.struct_field("id", :long, nullable: false)])

      assert {:error, {:invalid_data, message}} =
               Session.__prepare_local_data__([%{"id" => nil}], schema: schema)

      assert message =~ ~s|input for field "id" (BIGINT) must not be nil|
    end

    test "positional rows are checked by field position" do
      schema =
        Types.struct_type([
          Types.struct_field("id", :long, nullable: false),
          Types.struct_field("name", :string)
        ])

      assert {:error, {:invalid_data, _}} =
               Session.__prepare_local_data__([{nil, "x"}], schema: schema)

      assert {:ok, _} = Session.__prepare_local_data__([{1, nil}], schema: schema)
    end

    test "nested struct fields, array elements and map values are checked" do
      schema =
        Types.struct_type([
          Types.struct_field(
            "s",
            Types.struct_type([Types.struct_field("inner", :long, nullable: false)])
          ),
          Types.struct_field("tags", Types.array_type(:string, contains_null: false)),
          Types.struct_field("m", Types.map_type(:string, :long, value_contains_null: false))
        ])

      assert {:error, {:invalid_data, message}} =
               Session.__prepare_local_data__(
                 [%{"s" => %{"inner" => nil}, "tags" => ["a"], "m" => %{"k" => 1}}],
                 schema: schema
               )

      assert message =~ ~s("s.inner")

      assert {:error, {:invalid_data, message}} =
               Session.__prepare_local_data__(
                 [%{"s" => %{"inner" => 1}, "tags" => [nil], "m" => %{"k" => 1}}],
                 schema: schema
               )

      assert message =~ ~s("tags[]")

      assert {:error, {:invalid_data, message}} =
               Session.__prepare_local_data__(
                 [%{"s" => %{"inner" => 1}, "tags" => ["a"], "m" => %{"k" => nil}}],
                 schema: schema
               )

      assert message =~ ~s("m[]")

      assert {:ok, _} =
               Session.__prepare_local_data__(
                 [%{"s" => %{"inner" => 1}, "tags" => ["a"], "m" => %{"k" => 1}}],
                 schema: schema
               )
    end

    test "Explorer frames are checked against non-nullable fields" do
      schema = Types.struct_type([Types.struct_field("id", :long, nullable: false)])
      frame = Explorer.DataFrame.new(id: [1, nil])

      assert {:error, {:invalid_data, _}} = Session.__prepare_local_data__(frame, schema: schema)
    end

    test "nullable fields still accept nil" do
      schema = Types.struct_type([Types.struct_field("id", :long)])
      assert {:ok, _} = Session.__prepare_local_data__([%{"id" => nil}], schema: schema)
    end

    test "the transmitted Arrow-path schema relaxes nullability but keeps metadata" do
      schema =
        Types.struct_type([
          Types.struct_field("id", :long, nullable: false, metadata: %{"c" => "x"})
        ])

      assert {:ok, {:local_relation, ipc, json, _df}} =
               Session.__prepare_local_data__([%{"id" => 1}], schema: schema)

      assert is_binary(ipc)
      assert [%{"nullable" => true, "metadata" => %{"c" => "x"}}] = json_fields(json)
    end
  end

  describe "JSON-projection decoding of non-finite doubles (finding 11)" do
    test "the three spellings become the Explorer sentinels for DOUBLE / FLOAT" do
      schema =
        proto_struct([
          {"a", proto_array(proto_double())},
          {"f", proto_array(proto_float())}
        ])

      row = %{
        "a" => Jason.encode!(["NaN", "Infinity", "-Infinity", 1.5]),
        "f" => Jason.encode!(["NaN"])
      }

      assert [decoded] = Session.__decode_json_projection_rows__([row], schema)
      assert decoded["a"] == [:nan, :infinity, :neg_infinity, 1.5]
      assert decoded["f"] == [:nan]
    end

    test "STRING columns holding the same text are left alone" do
      schema = proto_struct([{"s", proto_array(proto_string())}])
      row = %{"s" => Jason.encode!(["NaN", "Infinity", "-Infinity"])}

      assert [decoded] = Session.__decode_json_projection_rows__([row], schema)
      assert decoded["s"] == ["NaN", "Infinity", "-Infinity"]
    end
  end

  describe "Decimal inference uses the fixed PySpark shape (finding 13)" do
    test "local rows infer decimal(38, 18) regardless of the value's scale" do
      assert {:ok, {:local_relation, _ipc, schema, df}} =
               Session.__prepare_local_data__([
                 %{"d" => Decimal.new("1.20")},
                 %{"d" => Decimal.new("1")}
               ])

      assert Explorer.DataFrame.dtypes(df)["d"] == {:decimal, 38, 18}
      assert schema =~ "DECIMAL(38, 18)"
    end

    test "user-supplied Explorer frames keep their own decimal dtype" do
      frame = Explorer.DataFrame.new(d: [Decimal.new("1.20")])
      assert Explorer.DataFrame.dtypes(frame)["d"] == {:decimal, 38, 2}

      assert {:ok, {:local_relation, _ipc, schema, df}} = Session.__prepare_local_data__(frame)
      assert Explorer.DataFrame.dtypes(df)["d"] == {:decimal, 38, 2}
      assert schema =~ "DECIMAL(38, 2)"
    end
  end
end
