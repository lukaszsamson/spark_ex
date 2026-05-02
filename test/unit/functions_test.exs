defmodule SparkEx.FunctionsTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions

  describe "constructors" do
    test "col/1 creates column reference" do
      assert %Column{expr: {:col, "name"}} = Functions.col("name")
    end

    test "lit/1 creates literal for various types" do
      assert %Column{expr: {:lit, 42}} = Functions.lit(42)
      assert %Column{expr: {:lit, "hello"}} = Functions.lit("hello")
      assert %Column{expr: {:lit, true}} = Functions.lit(true)
      assert %Column{expr: {:lit, nil}} = Functions.lit(nil)
      assert %Column{expr: {:lit, 3.14}} = Functions.lit(3.14)
    end

    test "expr/1 creates expression string" do
      assert %Column{expr: {:expr, "age + 1"}} = Functions.expr("age + 1")
    end

    test "star/0 creates unresolved star" do
      assert %Column{expr: {:star}} = Functions.star()
    end
  end

  describe "aggregate functions" do
    test "count/1" do
      result = Functions.count(Functions.col("x"))
      assert %Column{expr: {:fn, "count", [{:col, "x"}], false}} = result
    end

    test "sum/1" do
      result = Functions.sum(Functions.col("x"))
      assert %Column{expr: {:fn, "sum", [{:col, "x"}], false}} = result
    end

    test "avg/1" do
      result = Functions.avg(Functions.col("x"))
      assert %Column{expr: {:fn, "avg", [{:col, "x"}], false}} = result
    end

    test "min/1" do
      result = Functions.min(Functions.col("x"))
      assert %Column{expr: {:fn, "min", [{:col, "x"}], false}} = result
    end

    test "max/1" do
      result = Functions.max(Functions.col("x"))
      assert %Column{expr: {:fn, "max", [{:col, "x"}], false}} = result
    end

    test "count_distinct/1 sets is_distinct flag" do
      result = Functions.count_distinct(Functions.col("x"))
      assert %Column{expr: {:fn, "count", [{:col, "x"}], true}} = result
    end
  end

  describe "sort helpers" do
    test "asc/1 delegates to Column" do
      result = Functions.asc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :asc, :nulls_first}} = result
    end

    test "desc/1 delegates to Column" do
      result = Functions.desc(Functions.col("x"))
      assert %Column{expr: {:sort_order, {:col, "x"}, :desc, :nulls_last}} = result
    end
  end

  describe "udf helpers" do
    test "call_udf/2 creates function expression" do
      result = Functions.call_udf("my_udf", [Functions.col("x"), Functions.col("y")])
      assert %Column{expr: {:fn, "my_udf", [{:col, "x"}, {:col, "y"}], false}} = result
    end

    test "unwrap_udt/1 creates unwrap_udt expression" do
      result = Functions.unwrap_udt(Functions.col("x"))
      assert %Column{expr: {:fn, "unwrap_udt", [{:col, "x"}], false}} = result
    end
  end

  describe "call_function/3" do
    test "builds call_function expression with named args" do
      expr = Functions.call_function("my_fn", [Functions.lit(1)], foo: 2, bar: Functions.lit(3))

      assert %Column{
               expr:
                 {:call_function, "my_fn",
                  [
                    {:lit, 1},
                    {:named_arg, "foo", {:lit, 2}},
                    {:named_arg, "bar", {:lit, 3}}
                  ]}
             } = expr
    end
  end

  describe "partitioning helpers" do
    test "bucket/2 builds bucket expression" do
      result = Functions.bucket(10, Functions.col("id"))
      assert %Column{expr: {:fn, "bucket", [{:lit, 10}, {:col, "id"}], false}} = result
    end

    test "years/months/days/hours build expressions" do
      assert %Column{expr: {:fn, "years", [{:col, "i"}], false}} = Functions.years("i")
      assert %Column{expr: {:fn, "months", [{:col, "i"}], false}} = Functions.months("i")
      assert %Column{expr: {:fn, "days", [{:col, "i"}], false}} = Functions.days("i")
      assert %Column{expr: {:fn, "hours", [{:col, "i"}], false}} = Functions.hours("i")
    end
  end

  describe "regex helpers" do
    test "regexp_extract_all/3 builds expression" do
      result = Functions.regexp_extract_all(Functions.col("name"), "(foo)", 1)

      assert %Column{
               expr:
                 {:fn, "regexp_extract_all", [{:col, "name"}, {:lit, "(foo)"}, {:lit, 1}], false}
             } =
               result
    end

    test "regexp_substr/2 builds expression" do
      result = Functions.regexp_substr(Functions.col("name"), "foo")

      assert %Column{expr: {:fn, "regexp_substr", [{:col, "name"}, {:lit, "foo"}], false}} =
               result
    end

    test "regexp_like/2 builds expression" do
      result = Functions.regexp_like(Functions.col("name"), "foo")
      assert %Column{expr: {:fn, "regexp_like", [{:col, "name"}, {:lit, "foo"}], false}} = result
    end
  end

  describe "avro/protobuf helpers" do
    test "from_avro/3 builds options map literal" do
      result = Functions.from_avro(Functions.col("payload"), "schema", %{"mode" => "PERMISSIVE"})

      assert %Column{
               expr:
                 {:fn, "from_avro",
                  [
                    {:col, "payload"},
                    {:lit, "schema"},
                    {:fn, "map", [{:lit, "mode"}, {:lit, "PERMISSIVE"}], false}
                  ], false}
             } = result
    end

    test "to_protobuf/3 reads descriptor file" do
      tmp = Path.join(System.tmp_dir!(), "spark_ex_test.desc")
      File.write!(tmp, <<1, 2, 3>>)
      on_exit(fn -> File.rm(tmp) end)

      result = Functions.to_protobuf(Functions.col("payload"), "Message", desc_file_path: tmp)

      assert %Column{
               expr:
                 {:fn, "to_protobuf", [{:col, "payload"}, {:lit, "Message"}, {:lit, <<1, 2, 3>>}],
                  false}
             } = result
    end
  end

  describe "R3-H4 array/map collection lit-wrapping (C1-1)" do
    test "array_contains/2 wraps the second argument as a literal" do
      assert %Column{
               expr: {:fn, "array_contains", [{:col, "xs"}, {:lit, "x"}], false}
             } = Functions.array_contains(Functions.col("xs"), "x")
    end

    test "array_position/2 wraps the second argument as a literal" do
      assert %Column{
               expr: {:fn, "array_position", [{:col, "xs"}, {:lit, "x"}], false}
             } = Functions.array_position(Functions.col("xs"), "x")
    end

    test "array_remove/2 wraps the second argument as a literal" do
      assert %Column{
               expr: {:fn, "array_remove", [{:col, "xs"}, {:lit, 1}], false}
             } = Functions.array_remove(Functions.col("xs"), 1)
    end

    test "array_append/2 wraps the second argument as a literal" do
      assert %Column{
               expr: {:fn, "array_append", [{:col, "xs"}, {:lit, "x"}], false}
             } = Functions.array_append(Functions.col("xs"), "x")
    end

    test "array_prepend/2 wraps the second argument as a literal" do
      assert %Column{
               expr: {:fn, "array_prepend", [{:col, "xs"}, {:lit, "x"}], false}
             } = Functions.array_prepend(Functions.col("xs"), "x")
    end

    test "element_at/2 wraps the second argument as a literal" do
      assert %Column{
               expr: {:fn, "element_at", [{:col, "xs"}, {:lit, 1}], false}
             } = Functions.element_at(Functions.col("xs"), 1)
    end

    test "map_contains_key/2 wraps the second argument as a literal" do
      assert %Column{
               expr: {:fn, "map_contains_key", [{:col, "m"}, {:lit, "k"}], false}
             } = Functions.map_contains_key(Functions.col("m"), "k")
    end

    test "try_element_at/2 still treats string arg as column (per GPT triage)" do
      assert %Column{
               expr: {:fn, "try_element_at", [{:col, "xs"}, {:col, "k"}], false}
             } = Functions.try_element_at(Functions.col("xs"), "k")
    end
  end

  describe "R3-H5 call_function/3 string args are column refs (C1-2)" do
    test "binary positional args become column refs" do
      assert %Column{
               expr: {:call_function, "upper", [{:col, "name"}]}
             } = Functions.call_function("upper", ["name"])
    end

    test "lit-wrapped strings stay as literals" do
      assert %Column{
               expr: {:call_function, "upper", [{:lit, "hello"}]}
             } = Functions.call_function("upper", [Functions.lit("hello")])
    end
  end

  describe "R3-L1 slice/3 (C1-3)" do
    test "encodes integer start/length as literals" do
      assert %Column{
               expr: {:fn, "slice", [{:col, "xs"}, {:lit, 1}, {:lit, 2}], false}
             } = Functions.slice(Functions.col("xs"), 1, 2)
    end

    test "encodes Column start/length as expressions" do
      assert %Column{
               expr: {:fn, "slice", [{:col, "xs"}, {:col, "s"}, {:col, "l"}], false}
             } =
               Functions.slice(
                 Functions.col("xs"),
                 Functions.col("s"),
                 Functions.col("l")
               )
    end

    test "encodes string start/length as column refs" do
      assert %Column{
               expr: {:fn, "slice", [{:col, "xs"}, {:col, "s"}, {:col, "l"}], false}
             } = Functions.slice(Functions.col("xs"), "s", "l")
    end
  end
end
