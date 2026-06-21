# Round 61: Bitmap functions, sketch functions, explode variants, string/number
# conversions, make_date, null-ordering sorts, Reader format options,
# Column.like/rlike/ilike, inline, stack SQL, to_number, soundex
alias SparkEx.{DataFrame, Column, GroupedData, WindowSpec, Catalog}
import SparkEx.Functions, except: [length: 1, abs: 1, struct: 1, round: 1, ceil: 1, floor: 1]

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

run = fn label, func ->
  IO.puts(label)
  try do
    result = func.()
    IO.inspect(result, label: "  OK", limit: 80, printable_limit: 5000)
  rescue
    e -> IO.puts("  ERROR: #{Exception.message(e)}")
  catch
    kind, reason -> IO.puts("  CATCH (#{kind}): #{inspect(reason, limit: 5)}")
  end
  IO.puts("")
end

run_id = :erlang.system_time(:millisecond) |> to_string()

# =============================================
# 1. Bitmap functions
# =============================================

run.("1a. bitmap_bit_position + bitmap_bucket_number", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1}, %{"id" => 100}, %{"id" => 1000}, %{"id" => 32767}
  ], schema: "id INT")
  df |> DataFrame.select([
    col("id"),
    bitmap_bit_position(col("id")) |> Column.alias_("bit_pos"),
    bitmap_bucket_number(col("id")) |> Column.alias_("bucket")
  ]) |> DataFrame.collect()
end)

run.("1b. bitmap_construct_agg + bitmap_count", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..20, fn i -> %{"val" => i} end),
    schema: "val INT")
  df |> DataFrame.select([
    bitmap_count(bitmap_construct_agg(col("val"))) |> Column.alias_("distinct_count")
  ]) |> DataFrame.collect()
end)

run.("1c. bitmap_or_agg", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "val" => 1}, %{"grp" => "A", "val" => 2},
    %{"grp" => "B", "val" => 3}, %{"grp" => "B", "val" => 4}
  ], schema: "grp STRING, val INT")
  df |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    bitmap_count(bitmap_or_agg(col("val"))) |> Column.alias_("count")
  ]) |> DataFrame.collect()
end)

# =============================================
# 2. HLL sketch functions
# =============================================

run.("2a. hll_sketch_agg + hll_sketch_estimate", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..1000, fn i -> %{"val" => rem(i, 100)} end),
    schema: "val INT")
  df |> DataFrame.select([
    hll_sketch_estimate(hll_sketch_agg(col("val"))) |> Column.alias_("approx_distinct")
  ]) |> DataFrame.collect()
end)

run.("2b. hll_union of two sketches", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "val" => 1}, %{"grp" => "A", "val" => 2}, %{"grp" => "A", "val" => 3},
    %{"grp" => "B", "val" => 3}, %{"grp" => "B", "val" => 4}, %{"grp" => "B", "val" => 5}
  ], schema: "grp STRING, val INT")

  # Get per-group sketches then union
  sketches = df
  |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([hll_sketch_agg(col("val")) |> Column.alias_("sketch")])

  sketches |> DataFrame.select([
    hll_sketch_estimate(hll_union_agg(col("sketch"))) |> Column.alias_("union_estimate")
  ]) |> DataFrame.collect()
end)

# =============================================
# 3. Explode variants: explode_outer, posexplode_outer, inline
# =============================================

run.("3a. explode_outer (preserves null rows)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => [10, 20, 30]},
    %{"id" => 2, "arr" => nil},
    %{"id" => 3, "arr" => [40]}
  ], schema: "id INT, arr ARRAY<INT>")
  df |> DataFrame.select([
    col("id"),
    explode_outer(col("arr")) |> Column.alias_("val")
  ]) |> DataFrame.collect()
end)

run.("3b. posexplode_outer", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "arr" => ["a", "b"]},
    %{"id" => 2, "arr" => nil}
  ], schema: "id INT, arr ARRAY<STRING>")
  # posexplode returns (pos, col) — need to use select_expr or SQL
  SparkEx.sql(s, """
    SELECT id, pos, val
    FROM __spark_ex_tmp_posexplode_#{run_id}
    LATERAL VIEW posexplode_outer(arr) t AS pos, val
  """)
  |> DataFrame.collect()
end)

run.("3c. inline (array of structs → rows)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "items" => [%{"name" => "a", "qty" => 10}, %{"name" => "b", "qty" => 20}]},
    %{"id" => 2, "items" => [%{"name" => "c", "qty" => 30}]}
  ], schema: "id INT, items ARRAY<STRUCT<name: STRING, qty: INT>>")
  df |> DataFrame.select([
    col("id"),
    inline(col("items"))
  ]) |> DataFrame.collect()
end)

run.("3d. inline_outer (preserves null arrays)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"id" => 1, "items" => [%{"name" => "a", "qty" => 10}]},
    %{"id" => 2, "items" => nil}
  ], schema: "id INT, items ARRAY<STRUCT<name: STRING, qty: INT>>")
  df |> DataFrame.select([
    col("id"),
    inline_outer(col("items"))
  ]) |> DataFrame.collect()
end)

# =============================================
# 4. String/number conversion functions
# =============================================

run.("4a. soundex", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Robert"}, %{"name" => "Rupert"}, %{"name" => "Smith"}, %{"name" => "Smythe"}
  ], schema: "name STRING")
  df |> DataFrame.select([
    col("name"),
    soundex(col("name")) |> Column.alias_("soundex_code")
  ]) |> DataFrame.collect()
end)

run.("4b. to_number + try_to_number", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s" => "$1,234.56"},
    %{"s" => "$999.00"},
    %{"s" => "invalid"}
  ], schema: "s STRING")
  df |> DataFrame.select([
    col("s"),
    to_number(col("s"), lit("$9,999.99")) |> Column.alias_("parsed"),
    try_to_number(col("s"), lit("$9,999.99")) |> Column.alias_("try_parsed")
  ]) |> DataFrame.collect()
end)

run.("4c. to_char_ (number formatting)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1234.567}, %{"val" => 0.5}, %{"val" => -99.9}
  ], schema: "val DOUBLE")
  df |> DataFrame.select([
    col("val"),
    to_char_(col("val"), lit("$9,999.99")) |> Column.alias_("formatted")
  ]) |> DataFrame.collect()
end)

run.("4d. char_ (chr) + ascii", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"code" => 65}, %{"code" => 90}, %{"code" => 97}, %{"code" => 122}
  ], schema: "code INT")
  df |> DataFrame.select([
    col("code"),
    char_(col("code")) |> Column.alias_("character"),
    ascii(char_(col("code"))) |> Column.alias_("back_to_code")
  ]) |> DataFrame.collect()
end)

run.("4e. base64 + unbase64 roundtrip", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello, World!"},
    %{"text" => "SparkEx testing 123"},
    %{"text" => ""}
  ], schema: "text STRING")
  df |> DataFrame.select([
    col("text"),
    base64(col("text")) |> Column.alias_("encoded"),
    unbase64(base64(col("text"))) |> Column.cast("STRING") |> Column.alias_("decoded")
  ]) |> DataFrame.collect()
end)

run.("4f. unhex", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"hex_str" => "48656C6C6F"},  # "Hello"
    %{"hex_str" => "DEADBEEF"}
  ], schema: "hex_str STRING")
  df |> DataFrame.select([
    col("hex_str"),
    unhex(col("hex_str")) |> Column.cast("STRING") |> Column.alias_("decoded"),
    hex(unhex(col("hex_str"))) |> Column.alias_("roundtrip")
  ]) |> DataFrame.collect()
end)

run.("4g. conv (base conversion)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"num" => "255"}, %{"num" => "1010"}, %{"num" => "FF"}
  ], schema: "num STRING")
  df |> DataFrame.select([
    col("num"),
    conv(col("num"), lit(10), lit(16)) |> Column.alias_("dec_to_hex"),
    conv(col("num"), lit(2), lit(10)) |> Column.alias_("bin_to_dec"),
    conv(col("num"), lit(16), lit(2)) |> Column.alias_("hex_to_bin")
  ]) |> DataFrame.collect()
end)

# =============================================
# 5. Date construction functions
# =============================================

run.("5a. make_date", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"y" => 2024, "m" => 1, "d" => 15},
    %{"y" => 2024, "m" => 12, "d" => 31},
    %{"y" => 2024, "m" => 2, "d" => 29}  # leap year
  ], schema: "y INT, m INT, d INT")
  df |> DataFrame.select([
    make_date(col("y"), col("m"), col("d")) |> Column.alias_("date"),
    dayofweek(make_date(col("y"), col("m"), col("d"))) |> Column.alias_("dow"),
    dayofyear(make_date(col("y"), col("m"), col("d"))) |> Column.alias_("doy")
  ]) |> DataFrame.collect()
end)

run.("5b. make_date with invalid date", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"y" => 2024, "m" => 13, "d" => 1},  # invalid month
    %{"y" => 2023, "m" => 2, "d" => 29}   # not leap year
  ], schema: "y INT, m INT, d INT")
  df |> DataFrame.select([
    make_date(col("y"), col("m"), col("d")) |> Column.alias_("date")
  ]) |> DataFrame.collect()
end)

run.("5c. schema_of_json", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"json" => ~s({"name":"Alice","age":30,"scores":[90,85,92]})}
  ], schema: "json STRING")
  df |> DataFrame.select([
    schema_of_json(col("json")) |> Column.alias_("inferred_schema")
  ]) |> DataFrame.collect()
end)

# =============================================
# 6. Column null-ordering sorts
# =============================================

run.("6a. asc_nulls_first vs asc_nulls_last", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 3}, %{"val" => nil}, %{"val" => 1}, %{"val" => nil}, %{"val" => 2}
  ], schema: "val INT")

  {:ok, first} = df |> DataFrame.sort([Column.asc_nulls_first(col("val"))]) |> DataFrame.collect()
  {:ok, last} = df |> DataFrame.sort([Column.asc_nulls_last(col("val"))]) |> DataFrame.collect()
  %{nulls_first: first, nulls_last: last}
end)

run.("6b. desc_nulls_first vs desc_nulls_last", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 3}, %{"val" => nil}, %{"val" => 1}, %{"val" => nil}, %{"val" => 2}
  ], schema: "val INT")

  {:ok, first} = df |> DataFrame.sort([Column.desc_nulls_first(col("val"))]) |> DataFrame.collect()
  {:ok, last} = df |> DataFrame.sort([Column.desc_nulls_last(col("val"))]) |> DataFrame.collect()
  %{desc_nulls_first: first, desc_nulls_last: last}
end)

# =============================================
# 7. Column.like / rlike / ilike
# =============================================

run.("7a. Column.like with pattern", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "Alicia"},
    %{"name" => "ALICE"}, %{"name" => "alice"}
  ], schema: "name STRING")
  df |> DataFrame.select([
    col("name"),
    Column.like(col("name"), "Al%") |> Column.alias_("like_Al"),
    Column.rlike(col("name"), "^[Aa]l") |> Column.alias_("rlike_Al"),
    Column.ilike(col("name"), "al%") |> Column.alias_("ilike_al")
  ]) |> DataFrame.collect()
end)

run.("7b. Column.like with escape character", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => "10%"}, %{"val" => "100"}, %{"val" => "10_x"}, %{"val" => "10ax"}
  ], schema: "val STRING")
  df |> DataFrame.select([
    col("val"),
    Column.like(col("val"), "10\\%%") |> Column.alias_("contains_percent"),
    Column.like(col("val"), "10\\_%") |> Column.alias_("contains_underscore")
  ]) |> DataFrame.collect()
end)

# =============================================
# 8. Column.is_null, is_not_null, is_nan
# =============================================

run.("8. Null and NaN checks", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"val" => 1.0}, %{"val" => nil}, %{"val" => 0.0}
  ], schema: "val DOUBLE")

  # Add a NaN via computation
  with_nan = df |> DataFrame.with_column("nan_test",
    Column.divide(lit(0.0), lit(0.0)))

  with_nan |> DataFrame.select([
    col("val"),
    col("nan_test"),
    Column.is_null(col("val")) |> Column.alias_("val_is_null"),
    Column.is_not_null(col("val")) |> Column.alias_("val_is_not_null"),
    Column.is_nan(col("nan_test")) |> Column.alias_("nan_test_is_nan")
  ]) |> DataFrame.collect()
end)

# =============================================
# 9. Column.get_item (array/map indexing)
# =============================================

run.("9a. Column.get_item on array", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"arr" => [10, 20, 30, 40, 50]}
  ], schema: "arr ARRAY<INT>")
  df |> DataFrame.select([
    col("arr"),
    Column.get_item(col("arr"), 0) |> Column.alias_("first"),
    Column.get_item(col("arr"), 2) |> Column.alias_("third"),
    Column.get_item(col("arr"), 4) |> Column.alias_("fifth")
  ]) |> DataFrame.collect()
end)

run.("9b. Column.get_item on map", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"m" => %{"x" => 1, "y" => 2, "z" => 3}}
  ], schema: "m MAP<STRING, INT>")
  df |> DataFrame.select([
    col("m"),
    Column.get_item(col("m"), "x") |> Column.alias_("x_val"),
    Column.get_item(col("m"), "z") |> Column.alias_("z_val"),
    Column.get_item(col("m"), "missing") |> Column.alias_("missing_val")
  ]) |> DataFrame.collect()
end)

# =============================================
# 10. Column.with_field / drop_fields on structs
# =============================================

run.("10a. Column.with_field (add field to struct)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"person" => %{"name" => "Alice", "age" => 30}},
    %{"person" => %{"name" => "Bob", "age" => 25}}
  ], schema: "person STRUCT<name: STRING, age: INT>")
  df |> DataFrame.select([
    Column.with_field(col("person"), "active", lit(true)) |> Column.alias_("enriched")
  ]) |> DataFrame.collect()
end)

run.("10b. Column.drop_fields (remove field from struct)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"id" => 1, "name" => "Alice", "secret" => "password123", "email" => "a@b.com"}}
  ], schema: "rec STRUCT<id: INT, name: STRING, secret: STRING, email: STRING>")
  df |> DataFrame.select([
    Column.drop_fields(col("rec"), ["secret", "email"]) |> Column.alias_("sanitized")
  ]) |> DataFrame.collect()
end)

run.("10c. with_field + drop_fields chain", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"rec" => %{"id" => 1, "old_name" => "Alice", "score" => 95}}
  ], schema: "rec STRUCT<id: INT, old_name: STRING, score: INT>")
  df |> DataFrame.select([
    col("rec")
    |> Column.with_field("new_name", Column.get_field(col("rec"), "old_name"))
    |> Column.with_field("grade", lit("A"))
    |> Column.drop_fields(["old_name"])
    |> Column.alias_("transformed")
  ]) |> DataFrame.collect()
end)

# =============================================
# 11. Percentile function (non-approximate)
# =============================================

run.("11. percentile (exact)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..100, fn i -> %{"val" => i * 1.0} end),
    schema: "val DOUBLE")
  df |> DataFrame.select([
    percentile(col("val"), lit(0.5)) |> Column.alias_("p50"),
    percentile(col("val"), lit(0.95)) |> Column.alias_("p95"),
    percentile(col("val"), lit(0.99)) |> Column.alias_("p99")
  ]) |> DataFrame.collect()
end)

# =============================================
# 12. sentences function (NLP tokenization)
# =============================================

run.("12. sentences (text tokenization)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"text" => "Hello world. How are you? I'm fine, thanks!"},
    %{"text" => "SparkEx is great. Testing is fun."}
  ], schema: "text STRING")
  df |> DataFrame.select([
    col("text"),
    sentences(col("text")) |> Column.alias_("tokens")
  ]) |> DataFrame.collect()
end)

# =============================================
# 13. overlay function (string replacement at position)
# =============================================

run.("13. overlay (replace substring at position)", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"s" => "Hello, World!"}, %{"s" => "ABCDEFGH"}
  ], schema: "s STRING")
  df |> DataFrame.select([
    col("s"),
    overlay(col("s"), lit("***"), lit(1), lit(5)) |> Column.alias_("replaced")
  ]) |> DataFrame.collect()
end)

# =============================================
# 14. Creative wild combination: bitmap + sketch + window
# =============================================

run.("14. Creative: HLL sketch per group + window ranking", fn ->
  {:ok, df} = SparkEx.create_dataframe(s,
    Enum.map(1..200, fn i ->
      %{"dept" => Enum.at(["eng", "sales", "ops", "hr"], rem(i, 4)),
        "user_id" => rem(i * 7, 50)}
    end), schema: "dept STRING, user_id INT")

  df
  |> DataFrame.group_by([col("dept")])
  |> GroupedData.agg([
    hll_sketch_estimate(hll_sketch_agg(col("user_id"))) |> Column.alias_("approx_users"),
    count(col("user_id")) |> Column.alias_("total_events")
  ])
  |> DataFrame.with_column("events_per_user",
    Column.divide(
      Column.cast(col("total_events"), "DOUBLE"),
      Column.cast(col("approx_users"), "DOUBLE")))
  |> DataFrame.sort([desc(col("approx_users"))])
  |> DataFrame.collect()
end)

# =============================================
# 15. any_value + count_if + bool_and + bool_or
# =============================================

run.("15. Boolean aggregates + any_value + count_if", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"grp" => "A", "val" => 10, "flag" => true},
    %{"grp" => "A", "val" => 20, "flag" => true},
    %{"grp" => "A", "val" => 30, "flag" => false},
    %{"grp" => "B", "val" => 40, "flag" => true},
    %{"grp" => "B", "val" => 50, "flag" => true}
  ], schema: "grp STRING, val INT, flag BOOLEAN")
  df |> DataFrame.group_by([col("grp")])
  |> GroupedData.agg([
    any_value(col("val")) |> Column.alias_("any_val"),
    count_if(col("flag")) |> Column.alias_("true_count"),
    bool_and(col("flag")) |> Column.alias_("all_true"),
    bool_or(col("flag")) |> Column.alias_("any_true")
  ]) |> DataFrame.collect()
end)

# =============================================
# 16. Complex: explode_outer + window + struct access pipeline
# =============================================

run.("16. Pipeline: explode_outer → window → struct access", fn ->
  {:ok, df} = SparkEx.create_dataframe(s, [
    %{"order_id" => 1, "items" => [
      %{"product" => "Widget", "price" => 10.0},
      %{"product" => "Gadget", "price" => 25.0}
    ]},
    %{"order_id" => 2, "items" => [
      %{"product" => "Widget", "price" => 10.0},
      %{"product" => "Doohickey", "price" => 50.0},
      %{"product" => "Thingamajig", "price" => 15.0}
    ]},
    %{"order_id" => 3, "items" => nil}
  ], schema: "order_id INT, items ARRAY<STRUCT<product: STRING, price: DOUBLE>>")

  w = SparkEx.Window.partition_by([col("order_id")]) |> WindowSpec.order_by([desc(col("price"))])

  df
  |> DataFrame.select([
    col("order_id"),
    explode_outer(col("items")) |> Column.alias_("item")
  ])
  |> DataFrame.select([
    col("order_id"),
    Column.get_field(col("item"), "product") |> Column.alias_("product"),
    Column.get_field(col("item"), "price") |> Column.alias_("price")
  ])
  |> DataFrame.with_column("rank_in_order",
    row_number() |> Column.over(w))
  |> DataFrame.sort([asc(col("order_id")), asc(col("rank_in_order"))])
  |> DataFrame.collect()
end)

IO.puts("=== Round 61 complete ===")
