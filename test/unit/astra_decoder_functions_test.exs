defmodule SparkEx.AstraDecoderFunctionsTest do
  # Not async: the UDT registry is process-global.
  use ExUnit.Case, async: false

  alias Spark.Connect.DataType
  alias SparkEx.Column
  alias SparkEx.Connect.ResultDecoder
  alias SparkEx.Connect.UDTRegistry
  alias SparkEx.Functions

  # ── Schema helpers ──────────────────────────────────────────────────────

  defp t(kind, value), do: %DataType{kind: {kind, value}}
  defp int_type, do: t(:integer, %DataType.Integer{})
  defp string_type, do: t(:string, %DataType.String{})
  defp map_type(kt, vt), do: t(:map, %DataType.Map{key_type: kt, value_type: vt})
  defp array_type(et), do: t(:array, %DataType.Array{element_type: et})

  defp udt_type(class, sql_type),
    do: t(:udt, %DataType.UDT{type: "udt", jvm_class: class, sql_type: sql_type})

  defp struct_type(fields) do
    t(:struct, %DataType.Struct{
      fields:
        Enum.map(fields, fn {name, dt} ->
          %DataType.StructField{name: name, data_type: dt, nullable: true}
        end)
    })
  end

  # ── Finding 10: regexp pattern arguments are columns ─────────────────────

  describe "regexp pattern arguments follow PySpark column semantics" do
    test "regexp_instr/2 takes a column name" do
      assert %Column{expr: {:fn, "regexp_instr", [{:col, "s"}, {:col, "p"}], false}} =
               Functions.regexp_instr("s", "p")
    end

    test "regexp_instr/2 takes a literal pattern via lit/1" do
      assert %Column{expr: {:fn, "regexp_instr", [{:col, "s"}, {:lit, "[0-9]+"}], false}} =
               Functions.regexp_instr("s", Functions.lit("[0-9]+"))
    end

    test "regexp_instr/3 takes a column name and a literal idx" do
      assert %Column{
               expr: {:fn, "regexp_instr", [{:col, "s"}, {:col, "p"}, {:lit, 1}], false}
             } = Functions.regexp_instr("s", "p", 1)
    end

    test "regexp_substr/2 takes a column name" do
      assert %Column{expr: {:fn, "regexp_substr", [{:col, "s"}, {:col, "p"}], false}} =
               Functions.regexp_substr("s", "p")
    end

    test "regexp_substr/2 takes a literal pattern via lit/1" do
      assert %Column{expr: {:fn, "regexp_substr", [{:col, "s"}, {:lit, "[0-9]+"}], false}} =
               Functions.regexp_substr("s", Functions.lit("[0-9]+"))
    end

    test "regexp_extract_all/2 takes a column name" do
      assert %Column{expr: {:fn, "regexp_extract_all", [{:col, "s"}, {:col, "p"}], false}} =
               Functions.regexp_extract_all("s", "p")
    end

    test "regexp_extract_all/3 takes a column name and a literal idx" do
      assert %Column{
               expr: {:fn, "regexp_extract_all", [{:col, "s"}, {:col, "p"}, {:lit, 0}], false}
             } = Functions.regexp_extract_all("s", "p", 0)
    end

    test "regexp_extract_all/3 takes a literal pattern via lit/1" do
      assert %Column{
               expr:
                 {:fn, "regexp_extract_all", [{:col, "s"}, {:lit, "([0-9]+)"}, {:lit, 1}], false}
             } = Functions.regexp_extract_all("s", Functions.lit("([0-9]+)"), 1)
    end

    test "regexp_extract/3 keeps PySpark's literal pattern" do
      assert %Column{
               expr: {:fn, "regexp_extract", [{:col, "s"}, {:lit, "([0-9]+)"}, {:lit, 1}], false}
             } = Functions.regexp_extract("s", "([0-9]+)", 1)
    end

    test "regexp_replace/3 keeps PySpark's literal pattern" do
      assert %Column{
               expr: {:fn, "regexp_replace", [{:col, "s"}, {:lit, "[0-9]"}, {:lit, "x"}], false}
             } = Functions.regexp_replace("s", "[0-9]", "x")
    end

    test "regexp_count/2 and regexp_like/2 keep column semantics" do
      assert %Column{expr: {:fn, "regexp_count", [{:col, "s"}, {:col, "p"}], false}} =
               Functions.regexp_count("s", "p")

      assert %Column{expr: {:fn, "regexp_like", [{:col, "s"}, {:col, "p"}], false}} =
               Functions.regexp_like("s", "p")
    end
  end

  # ── Finding 12: UDT deserializers are not called for nulls ───────────────

  describe "UDT deserializers skip null values" do
    setup do
      class = "com.example.AstraPoint#{System.unique_integer([:positive])}"
      :ok = UDTRegistry.register(class, fn [x, y] -> {x, y} end)
      on_exit(fn -> UDTRegistry.unregister(class) end)
      %{class: class, udt: udt_type(class, struct_type([{"x", int_type()}, {"y", int_type()}]))}
    end

    test "top-level nil passes through untouched", %{udt: udt} do
      fun = ResultDecoder.column_value_transform(udt)
      assert fun.(nil) == nil
      assert fun.([1, 2]) == {1, 2}
    end

    test "nil array elements pass through", %{udt: udt} do
      fun = ResultDecoder.column_value_transform(array_type(udt))
      assert fun.([[1, 2], nil]) == [{1, 2}, nil]
      assert fun.(nil) == nil
    end

    test "nil map values and nil struct fields pass through", %{udt: udt} do
      map_fun = ResultDecoder.column_value_transform(map_type(string_type(), udt))
      assert map_fun.([%{"key" => "a", "value" => nil}]) == [%{"key" => "a", "value" => nil}]
      assert map_fun.(%{"a" => nil, "b" => [1, 2]}) == %{"a" => nil, "b" => {1, 2}}

      struct_fun = ResultDecoder.column_value_transform(struct_type([{"p", udt}]))
      assert struct_fun.(%{"p" => nil}) == %{"p" => nil}
      assert struct_fun.(%{"p" => [3, 4]}) == %{"p" => {3, 4}}
    end

    test "apply_row_transforms leaves null UDT cells alone", %{udt: udt} do
      schema = struct_type([{"p", udt}, {"arr", array_type(udt)}])

      assert [%{"p" => nil, "arr" => [nil, {1, 2}]}] =
               ResultDecoder.apply_row_transforms([%{"p" => nil, "arr" => [nil, [1, 2]]}], schema)
    end
  end
end
