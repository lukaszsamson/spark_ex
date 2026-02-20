defmodule SparkEx.MissingFeaturesTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions
  alias SparkEx.Types

  # ── Types ──

  describe "SparkEx.Types new types" do
    test ":null type DDL" do
      schema = Types.struct_type([Types.struct_field("v", :null)])
      assert Types.to_ddl(schema) == "v VOID"
    end

    test ":null type JSON" do
      schema = Types.struct_type([Types.struct_field("v", :null)])
      json = Types.to_json(schema)
      assert json =~ "\"type\":\"void\""
    end

    test "{:char, n} type DDL" do
      schema = Types.struct_type([Types.struct_field("c", {:char, 10})])
      assert Types.to_ddl(schema) == "c CHAR(10)"
    end

    test "{:varchar, n} type DDL" do
      schema = Types.struct_type([Types.struct_field("v", {:varchar, 255})])
      assert Types.to_ddl(schema) == "v VARCHAR(255)"
    end

    test "{:char, n} type JSON" do
      schema = Types.struct_type([Types.struct_field("c", {:char, 10})])
      json = Types.to_json(schema)
      assert json =~ "char(10)"
    end

    test "{:varchar, n} type JSON" do
      schema = Types.struct_type([Types.struct_field("v", {:varchar, 255})])
      json = Types.to_json(schema)
      assert json =~ "varchar(255)"
    end

    test ":time type DDL" do
      schema = Types.struct_type([Types.struct_field("t", :time)])
      assert Types.to_ddl(schema) == "t TIME"
    end

    test ":time type JSON" do
      schema = Types.struct_type([Types.struct_field("t", :time)])
      json = Types.to_json(schema)
      assert json =~ "\"type\":\"time\""
    end

    test ":day_time_interval type DDL" do
      schema = Types.struct_type([Types.struct_field("d", :day_time_interval)])
      assert Types.to_ddl(schema) == "d INTERVAL DAY TO SECOND"
    end

    test ":year_month_interval type DDL" do
      schema = Types.struct_type([Types.struct_field("y", :year_month_interval)])
      assert Types.to_ddl(schema) == "y INTERVAL YEAR TO MONTH"
    end

    test ":calendar_interval type DDL" do
      schema = Types.struct_type([Types.struct_field("c", :calendar_interval)])
      assert Types.to_ddl(schema) == "c INTERVAL"
    end

    test ":variant type DDL" do
      schema = Types.struct_type([Types.struct_field("v", :variant)])
      assert Types.to_ddl(schema) == "v VARIANT"
    end

    test ":variant type JSON" do
      schema = Types.struct_type([Types.struct_field("v", :variant)])
      json = Types.to_json(schema)
      assert json =~ "\"type\":\"variant\""
    end

    test "interval types JSON" do
      schema =
        Types.struct_type([
          Types.struct_field("d", :day_time_interval),
          Types.struct_field("y", :year_month_interval),
          Types.struct_field("c", :calendar_interval)
        ])

      json = Types.to_json(schema)
      assert json =~ "day-time interval"
      assert json =~ "year-month interval"
      assert json =~ "\"type\":\"interval\""
    end
  end

  # ── DataFrame.agg ──

  describe "DataFrame.agg/2" do
    test "creates aggregate plan via empty group_by" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      result = DataFrame.agg(df, [Functions.count(Functions.col("id"))])
      assert %DataFrame{plan: {:aggregate, _, :groupby, [], _}} = result
    end
  end

  # ── Column aliases ──

  describe "Column.name/2" do
    test "is alias for alias_/2" do
      col = Functions.col("a")
      result = Column.name(col, "renamed")
      assert %Column{expr: {:alias, {:col, "a"}, "renamed"}} = result
    end
  end

  describe "Column.astype/2" do
    test "is alias for cast/2" do
      col = Functions.col("a")
      result = Column.astype(col, "int")
      assert %Column{expr: {:cast, {:col, "a"}, "int"}} = result
    end
  end

  # ── Sorting function delegates ──

  describe "Functions sorting null variants" do
    test "asc_nulls_first/1" do
      result = Functions.asc_nulls_first(Functions.col("a"))
      assert %Column{expr: {:sort_order, {:col, "a"}, :asc, :nulls_first}} = result
    end

    test "asc_nulls_last/1" do
      result = Functions.asc_nulls_last(Functions.col("a"))
      assert %Column{expr: {:sort_order, {:col, "a"}, :asc, :nulls_last}} = result
    end

    test "desc_nulls_first/1" do
      result = Functions.desc_nulls_first(Functions.col("a"))
      assert %Column{expr: {:sort_order, {:col, "a"}, :desc, :nulls_first}} = result
    end

    test "desc_nulls_last/1" do
      result = Functions.desc_nulls_last(Functions.col("a"))
      assert %Column{expr: {:sort_order, {:col, "a"}, :desc, :nulls_last}} = result
    end
  end

  # ── JSON/CSV/XML functions ──

  describe "from_json/2" do
    test "creates from_json expression without options" do
      result = Functions.from_json(Functions.col("data"), "a INT, b STRING")
      assert %Column{expr: {:fn, "from_json", [{:col, "data"}, {:lit, "a INT, b STRING"}], false}} = result
    end

    test "creates from_json expression with options" do
      result = Functions.from_json(Functions.col("data"), "a INT", %{"mode" => "FAILFAST"})

      assert %Column{
               expr:
                 {:fn, "from_json",
                  [{:col, "data"}, {:lit, "a INT"}, {:fn, "map", _, false}], false}
             } = result
    end
  end

  describe "to_json/1" do
    test "creates to_json expression without options" do
      result = Functions.to_json(Functions.col("struct_col"))
      assert %Column{expr: {:fn, "to_json", [{:col, "struct_col"}], false}} = result
    end

    test "creates to_json expression with options" do
      result = Functions.to_json(Functions.col("struct_col"), %{"pretty" => "true"})

      assert %Column{
               expr: {:fn, "to_json", [{:col, "struct_col"}, {:fn, "map", _, false}], false}
             } = result
    end
  end

  describe "from_csv/2" do
    test "creates from_csv expression" do
      result = Functions.from_csv(Functions.col("csv_str"), "a INT, b STRING")
      assert %Column{expr: {:fn, "from_csv", [{:col, "csv_str"}, {:lit, "a INT, b STRING"}], false}} = result
    end
  end

  describe "to_csv/1" do
    test "creates to_csv expression" do
      result = Functions.to_csv(Functions.col("struct_col"))
      assert %Column{expr: {:fn, "to_csv", [{:col, "struct_col"}], false}} = result
    end
  end

  describe "from_xml/2" do
    test "creates from_xml expression" do
      result = Functions.from_xml(Functions.col("xml_str"), "a INT, b STRING")
      assert %Column{expr: {:fn, "from_xml", [{:col, "xml_str"}, {:lit, "a INT, b STRING"}], false}} = result
    end
  end

  describe "to_xml/1" do
    test "creates to_xml expression" do
      result = Functions.to_xml(Functions.col("struct_col"))
      assert %Column{expr: {:fn, "to_xml", [{:col, "struct_col"}], false}} = result
    end
  end

  # ── Window function ──

  describe "Functions.window/2" do
    test "creates tumbling window" do
      result = Functions.window(Functions.col("ts"), "10 minutes")
      assert %Column{expr: {:fn, "window", [{:col, "ts"}, {:lit, "10 minutes"}], false}} = result
    end

    test "creates sliding window" do
      result = Functions.window(Functions.col("ts"), "10 minutes", "5 minutes")

      assert %Column{
               expr:
                 {:fn, "window",
                  [{:col, "ts"}, {:lit, "10 minutes"}, {:lit, "5 minutes"}], false}
             } = result
    end

    test "creates sliding window with start time" do
      result = Functions.window(Functions.col("ts"), "10 minutes", "5 minutes", "2 minutes")

      assert %Column{
               expr:
                 {:fn, "window",
                  [{:col, "ts"}, {:lit, "10 minutes"}, {:lit, "5 minutes"}, {:lit, "2 minutes"}],
                  false}
             } = result
    end
  end

  # ── make_timestamp family ──

  describe "make_timestamp/1" do
    test "creates make_timestamp with 6 args" do
      cols = Enum.map(~w(y m d h min sec), &Functions.col/1)
      result = Functions.make_timestamp(cols)

      assert %Column{
               expr:
                 {:fn, "make_timestamp",
                  [{:col, "y"}, {:col, "m"}, {:col, "d"}, {:col, "h"}, {:col, "min"}, {:col, "sec"}],
                  false}
             } = result
    end
  end

  describe "try_make_timestamp/1" do
    test "creates try_make_timestamp" do
      result = Functions.try_make_timestamp([Functions.col("y"), Functions.col("m")])
      assert %Column{expr: {:fn, "try_make_timestamp", [{:col, "y"}, {:col, "m"}], false}} = result
    end
  end

  describe "make_timestamp_ltz/1" do
    test "creates make_timestamp_ltz" do
      cols = Enum.map(~w(y m d h min sec), &Functions.col/1)
      result = Functions.make_timestamp_ltz(cols)
      assert %Column{expr: {:fn, "make_timestamp_ltz", [_ | _], false}} = result
    end
  end

  describe "make_timestamp_ntz/1" do
    test "creates make_timestamp_ntz" do
      cols = Enum.map(~w(y m d h min sec), &Functions.col/1)
      result = Functions.make_timestamp_ntz(cols)
      assert %Column{expr: {:fn, "make_timestamp_ntz", [_ | _], false}} = result
    end
  end

  # ── Interval construction ──

  describe "make_dt_interval/1" do
    test "creates make_dt_interval with defaults" do
      result = Functions.make_dt_interval()
      assert %Column{expr: {:fn, "make_dt_interval", [_, _, _, _], false}} = result
    end

    test "creates make_dt_interval with custom days" do
      result = Functions.make_dt_interval(days: Functions.col("d"))
      assert %Column{expr: {:fn, "make_dt_interval", [{:col, "d"}, _, _, _], false}} = result
    end
  end

  describe "make_interval/1" do
    test "creates make_interval with 7 args" do
      result = Functions.make_interval()
      assert %Column{expr: {:fn, "make_interval", args, false}} = result
      assert length(args) == 7
    end
  end

  describe "try_make_interval/1" do
    test "creates try_make_interval with 7 args" do
      result = Functions.try_make_interval()
      assert %Column{expr: {:fn, "try_make_interval", args, false}} = result
      assert length(args) == 7
    end
  end

  describe "make_ym_interval/1" do
    test "creates make_ym_interval with defaults" do
      result = Functions.make_ym_interval()
      assert %Column{expr: {:fn, "make_ym_interval", [_, _], false}} = result
    end

    test "creates make_ym_interval with custom years" do
      result = Functions.make_ym_interval(years: Functions.col("y"))
      assert %Column{expr: {:fn, "make_ym_interval", [{:col, "y"}, _], false}} = result
    end
  end

  # ── Registry-generated functions ──

  describe "make_time/3 (registry)" do
    test "creates make_time expression" do
      result = Functions.make_time(Functions.col("h"), Functions.col("m"), Functions.col("s"))
      assert %Column{expr: {:fn, "make_time", [{:col, "h"}, {:col, "m"}, {:col, "s"}], false}} = result
    end
  end

  describe "window_time/1 (registry)" do
    test "creates window_time expression" do
      result = Functions.window_time(Functions.col("w"))
      assert %Column{expr: {:fn, "window_time", [{:col, "w"}], false}} = result
    end
  end

  describe "session_window/2 (registry)" do
    test "creates session_window expression" do
      result = Functions.session_window(Functions.col("ts"), Functions.col("gap"))
      assert %Column{expr: {:fn, "session_window", [{:col, "ts"}, {:col, "gap"}], false}} = result
    end
  end

  # ── Sketch functions ──

  describe "HLL sketch functions" do
    test "hll_sketch_agg/1" do
      result = Functions.hll_sketch_agg(Functions.col("x"))
      assert %Column{expr: {:fn, "hll_sketch_agg", [{:col, "x"}], false}} = result
    end

    test "hll_sketch_estimate/1" do
      result = Functions.hll_sketch_estimate(Functions.col("sketch"))
      assert %Column{expr: {:fn, "hll_sketch_estimate", [{:col, "sketch"}], false}} = result
    end

    test "hll_union/1" do
      result = Functions.hll_union(Functions.col("sketch"))
      assert %Column{expr: {:fn, "hll_union", [{:col, "sketch"}], false}} = result
    end

    test "hll_union_agg/1" do
      result = Functions.hll_union_agg(Functions.col("sketch"))
      assert %Column{expr: {:fn, "hll_union_agg", [{:col, "sketch"}], false}} = result
    end
  end

  # ── Bitmap functions ──

  describe "bitmap functions" do
    test "bitmap_bit_position/1" do
      result = Functions.bitmap_bit_position(Functions.col("x"))
      assert %Column{expr: {:fn, "bitmap_bit_position", [{:col, "x"}], false}} = result
    end

    test "bitmap_bucket_number/1" do
      result = Functions.bitmap_bucket_number(Functions.col("x"))
      assert %Column{expr: {:fn, "bitmap_bucket_number", [{:col, "x"}], false}} = result
    end

    test "bitmap_construct_agg/1" do
      result = Functions.bitmap_construct_agg(Functions.col("x"))
      assert %Column{expr: {:fn, "bitmap_construct_agg", [{:col, "x"}], false}} = result
    end

    test "bitmap_count/1" do
      result = Functions.bitmap_count(Functions.col("x"))
      assert %Column{expr: {:fn, "bitmap_count", [{:col, "x"}], false}} = result
    end

    test "bitmap_or_agg/1" do
      result = Functions.bitmap_or_agg(Functions.col("x"))
      assert %Column{expr: {:fn, "bitmap_or_agg", [{:col, "x"}], false}} = result
    end

    test "bitmap_and_agg/1" do
      result = Functions.bitmap_and_agg(Functions.col("x"))
      assert %Column{expr: {:fn, "bitmap_and_agg", [{:col, "x"}], false}} = result
    end
  end

  # ── Misc new string functions ──

  describe "new string functions" do
    test "parse_url/3" do
      result =
        Functions.parse_url(
          Functions.col("url"),
          Functions.col("part"),
          Functions.col("key")
        )

      assert %Column{expr: {:fn, "parse_url", [{:col, "url"}, {:col, "part"}, {:col, "key"}], false}} =
               result
    end

    test "quote_/1" do
      result = Functions.quote_(Functions.col("str"))
      assert %Column{expr: {:fn, "quote", [{:col, "str"}], false}} = result
    end

    test "bitwise_not_/1" do
      result = Functions.bitwise_not_(Functions.col("x"))
      assert %Column{expr: {:fn, "~", [{:col, "x"}], false}} = result
    end

    test "contains_/2 standalone function" do
      result = Functions.contains_(Functions.col("str"), Functions.col("substr"))
      assert %Column{expr: {:fn, "contains", [{:col, "str"}, {:col, "substr"}], false}} = result
    end

    test "like_/2 standalone function" do
      result = Functions.like_(Functions.col("str"), Functions.col("pattern"))
      assert %Column{expr: {:fn, "like", [{:col, "str"}, {:col, "pattern"}], false}} = result
    end

    test "substr_/3 standalone function" do
      result = Functions.substr_(Functions.col("str"), Functions.col("pos"), Functions.col("len"))

      assert %Column{
               expr: {:fn, "substr", [{:col, "str"}, {:col, "pos"}, {:col, "len"}], false}
             } = result
    end

    test "count_min_sketch/4" do
      result =
        Functions.count_min_sketch(Functions.col("x"), 0.01, 0.95, 42)

      assert %Column{
               expr:
                 {:fn, "count_min_sketch", [{:col, "x"}, {:lit, 0.01}, {:lit, 0.95}, {:lit, 42}],
                  false}
             } = result
    end
  end

  # ── CODEX fixes ──

  describe "filter/2 with string predicate" do
    test "accepts a string SQL expression" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      result = DataFrame.filter(df, "age > 18")
      assert %DataFrame{plan: {:filter, _, {:expr, "age > 18"}}} = result
    end
  end

  describe "repartition/2 with columns only (no partition count)" do
    test "repartitions by columns without explicit count" do
      df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      result = DataFrame.repartition(df, [Functions.col("key")])
      assert %DataFrame{plan: {:repartition_by_expression, _, [{:col, "key"}], nil}} = result
    end
  end
end
