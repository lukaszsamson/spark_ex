defmodule SparkEx.Integration.Spark42FunctionsTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.2"

  alias SparkEx.{Column, DataFrame, Session}
  alias SparkEx.Functions, as: F

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "TIME conversion precision, variant validity and current path", %{session: session} do
    :ok = Session.config_set(session, [{"spark.sql.timeType.enabled", "true"}])

    df = SparkEx.sql(session, "SELECT 3723000004L AS micros, parse_json('{\"x\":1}') AS v")

    result =
      DataFrame.select(df, [
        Column.alias_(F.time_to_micros(F.time_from_micros("micros")), "micros"),
        Column.alias_(F.time_to_millis(F.time_from_millis(F.lit(3_723_000))), "millis"),
        Column.alias_(F.time_to_seconds(F.time_from_seconds(F.lit(3723))), "seconds"),
        Column.alias_(F.is_valid_variant("v"), "valid"),
        Column.alias_(F.current_path(), "path")
      ])

    assert {:ok, [row]} = DataFrame.collect(result)
    assert row["micros"] == 3_723_000_004
    assert row["millis"] == 3_723_000
    assert Decimal.equal?(row["seconds"], Decimal.new("3723.000000"))
    assert row["valid"] == true
    assert row["path"] != nil
  end

  test "top K aggregates preserve the scalar overload", %{session: session} do
    df = SparkEx.sql(session, "SELECT * FROM VALUES ('a',1),('b',2),('c',3) AS t(v,n)")

    result =
      DataFrame.select(df, [
        Column.alias_(F.max_by("v", "n"), "max"),
        Column.alias_(F.max_by("v", "n", 2), "top"),
        Column.alias_(F.min_by("v", "n", 2), "bottom")
      ])

    assert {:ok, [row]} = DataFrame.collect(result)
    assert row["max"] == "c"
    assert Enum.sort(row["top"]) == ["b", "c"]
    assert Enum.sort(row["bottom"]) == ["a", "b"]
  end

  test "KLL merge aggregates round trip all supported types", %{session: session} do
    source = SparkEx.range(session, 10)

    sketches =
      DataFrame.select(source, [
        Column.alias_(F.kll_sketch_agg_bigint("id"), "bigint"),
        Column.alias_(F.kll_sketch_agg_float(F.expr("CAST(id AS FLOAT)")), "float"),
        Column.alias_(F.kll_sketch_agg_double(F.expr("CAST(id AS DOUBLE)")), "double")
      ])

    merged =
      DataFrame.select(sketches, [
        Column.alias_(F.kll_merge_agg_bigint("bigint"), "bigint"),
        Column.alias_(F.kll_merge_agg_float("float", 200), "float"),
        Column.alias_(F.kll_merge_agg_double("double"), "double")
      ])

    assert {:ok, [%{"bigint" => 10, "float" => 10, "double" => 10}]} =
             merged
             |> DataFrame.select([
               Column.alias_(F.kll_sketch_get_n_bigint("bigint"), "bigint"),
               Column.alias_(F.kll_sketch_get_n_float("float"), "float"),
               Column.alias_(F.kll_sketch_get_n_double("double"), "double")
             ])
             |> DataFrame.collect()
  end

  test "tuple sketches support summaries, set operations, nulls, and empty input", %{
    session: session
  } do
    source =
      SparkEx.sql(
        session,
        """
        SELECT * FROM VALUES
          ('a', CAST(1.0 AS DOUBLE), 'a', CAST(5.0 AS DOUBLE)),
          ('b', CAST(2.0 AS DOUBLE), 'c', CAST(6.0 AS DOUBLE)),
          ('c', CAST(3.0 AS DOUBLE), 'd', CAST(7.0 AS DOUBLE))
        AS source(left_key, left_value, right_key, right_value)
        """
      )

    sketches =
      DataFrame.select(source, [
        Column.alias_(F.tuple_sketch_agg_double("left_key", "left_value"), "left"),
        Column.alias_(F.tuple_sketch_agg_double("right_key", "right_value"), "right")
      ])

    operations =
      DataFrame.select(sketches, [
        Column.alias_(F.tuple_sketch_estimate_double("left"), "left_estimate"),
        Column.alias_(F.tuple_sketch_summary_double("left"), "left_summary"),
        Column.alias_(F.tuple_sketch_theta_double("left"), "left_theta"),
        Column.alias_(
          F.tuple_sketch_estimate_double(F.tuple_union_double("left", "right")),
          "union"
        ),
        Column.alias_(
          F.tuple_sketch_estimate_double(F.tuple_intersection_double("left", "right")),
          "intersection"
        ),
        Column.alias_(
          F.tuple_sketch_estimate_double(F.tuple_difference_double("left", "right")),
          "difference"
        )
      ])

    assert {:ok, [row]} = DataFrame.collect(operations)
    assert_in_delta row["left_estimate"], 3.0, 0.01
    assert_in_delta row["left_summary"], 6.0, 0.01
    assert row["left_theta"] > 0.0
    assert_in_delta row["union"], 4.0, 0.01
    assert_in_delta row["intersection"], 2.0, 0.01
    assert_in_delta row["difference"], 1.0, 0.01

    mixed_sketches =
      DataFrame.select(source, [
        Column.alias_(F.tuple_sketch_agg_double("left_key", "left_value"), "tuple"),
        Column.alias_(F.theta_sketch_agg("right_key"), "theta")
      ])

    assert {:ok, [theta]} =
             mixed_sketches
             |> DataFrame.select([
               Column.alias_(F.tuple_union_theta_double("tuple", "theta"), "union"),
               Column.alias_(F.tuple_intersection_theta_double("tuple", "theta"), "intersection"),
               Column.alias_(F.tuple_difference_theta_double("tuple", "theta"), "difference")
             ])
             |> DataFrame.collect()

    assert theta["union"] > 0.0
    assert theta["intersection"] > 0.0
    assert theta["difference"] > 0.0

    aggregate_inputs =
      DataFrame.select(sketches, [Column.alias_(F.col("left"), "sketch")])
      |> DataFrame.union(DataFrame.select(sketches, [Column.alias_(F.col("right"), "sketch")]))

    assert {:ok, [%{"union" => union, "intersection" => intersection}]} =
             aggregate_inputs
             |> DataFrame.select([
               Column.alias_(
                 F.tuple_sketch_estimate_double(F.tuple_union_agg_double("sketch", 12, "sum")),
                 "union"
               ),
               Column.alias_(
                 F.tuple_sketch_estimate_double(F.tuple_intersection_agg_double("sketch", "sum")),
                 "intersection"
               )
             ])
             |> DataFrame.collect()

    assert_in_delta union, 4.0, 0.01
    assert_in_delta intersection, 2.0, 0.01

    nulls =
      SparkEx.sql(
        session,
        "SELECT * FROM VALUES ('present', CAST(1.0 AS DOUBLE)), ('missing', CAST(NULL AS DOUBLE)) AS source(key, summary)"
      )

    assert {:ok, [%{"estimate" => estimate}]} =
             nulls
             |> DataFrame.select([
               Column.alias_(
                 F.tuple_sketch_estimate_double(F.tuple_sketch_agg_double("key", "summary")),
                 "estimate"
               )
             ])
             |> DataFrame.collect()

    assert_in_delta estimate, 1.0, 0.01

    empty =
      SparkEx.sql(
        session,
        "SELECT CAST(NULL AS STRING) AS key, CAST(NULL AS DOUBLE) AS summary WHERE false"
      )

    assert {:ok, [%{"estimate" => empty_estimate}]} =
             empty
             |> DataFrame.select([
               Column.alias_(
                 F.tuple_sketch_estimate_double(F.tuple_sketch_agg_double("key", "summary")),
                 "estimate"
               )
             ])
             |> DataFrame.collect()

    assert_in_delta empty_estimate, 0.0, 0.01
  end

  test "geospatial WKB overloads preserve endianness and a literal SRID", %{session: session} do
    assert :ok = Session.config_set(session, [{"spark.sql.geospatial.enabled", "true"}])

    geometry =
      SparkEx.sql(
        session,
        "SELECT unhex('0101000000000000000000F03F0000000000000040') AS wkb"
      )

    encoded =
      DataFrame.select(geometry, [
        Column.alias_(F.hex(F.st_asbinary(F.st_geomfromwkb("wkb"))), "ndr"),
        Column.alias_(F.hex(F.st_asbinary(F.st_geomfromwkb("wkb"), "XDR")), "xdr"),
        Column.alias_(F.st_srid(F.st_geomfromwkb("wkb", 4326)), "srid")
      ])

    assert {:ok, [row]} = DataFrame.collect(encoded)
    assert row["ndr"] == "0101000000000000000000F03F0000000000000040"
    assert row["xdr"] == "00000000013FF00000000000004000000000000000"
    assert row["srid"] == 4326
  end

  test "SQL-only vector functions use FLOAT arrays and typed literal degrees", %{session: session} do
    df =
      SparkEx.sql(
        session,
        """
        SELECT CAST(array(3.0, 4.0) AS ARRAY<FLOAT>) AS vector,
               CAST(array(0.0, 5.0) AS ARRAY<FLOAT>) AS other
        """
      )

    result =
      DataFrame.select(df, [
        Column.alias_(F.vector_cosine_similarity("vector", "other"), "cosine"),
        Column.alias_(F.vector_inner_product("vector", "other"), "inner"),
        Column.alias_(F.vector_l2_distance("vector", "other"), "distance"),
        Column.alias_(F.vector_norm("vector"), "default_norm"),
        Column.alias_(F.vector_norm("vector", 1.0), "l1_norm"),
        Column.alias_(F.vector_normalize("vector"), "normalized")
      ])

    assert {:ok, [row]} = DataFrame.collect(result)
    assert_in_delta row["cosine"], 0.8, 1.0e-6
    assert_in_delta row["inner"], 20.0, 1.0e-6
    assert_in_delta row["distance"], :math.sqrt(10), 1.0e-6
    assert_in_delta row["default_norm"], 5.0, 1.0e-6
    assert_in_delta row["l1_norm"], 7.0, 1.0e-6
    assert_vector_in_delta(row["normalized"], [0.6, 0.8])
  end

  test "vector aggregates merge partitions and preserve empty and null semantics", %{
    session: session
  } do
    vectors =
      SparkEx.sql(
        session,
        """
        SELECT CAST(array(CAST(id AS FLOAT), CAST(1.0 AS FLOAT)) AS ARRAY<FLOAT>) AS vector
        FROM range(0, 100, 1, 4)
        """
      )

    aggregate =
      DataFrame.select(vectors, [
        Column.alias_(F.vector_sum("vector"), "sum"),
        Column.alias_(F.vector_avg("vector"), "avg")
      ])

    assert {:ok, [row]} = DataFrame.collect(aggregate)
    assert_vector_in_delta(row["sum"], [4950.0, 100.0])
    assert_vector_in_delta(row["avg"], [49.5, 1.0])

    nulls_and_empty =
      SparkEx.sql(
        session,
        """
        SELECT vector_sum(vector) AS sum, vector_avg(vector) AS avg
        FROM VALUES
          (CAST(NULL AS ARRAY<FLOAT>)),
          (CAST(NULL AS ARRAY<FLOAT>))
        AS source(vector)
        """
      )

    assert {:ok, [%{"sum" => nil, "avg" => nil}]} = DataFrame.collect(nulls_and_empty)

    empty = SparkEx.sql(session, "SELECT CAST(array() AS ARRAY<FLOAT>) AS vector WHERE false")

    assert {:ok, [%{"sum" => nil, "avg" => nil}]} =
             empty
             |> DataFrame.select([
               Column.alias_(F.vector_sum("vector"), "sum"),
               Column.alias_(F.vector_avg("vector"), "avg")
             ])
             |> DataFrame.collect()
  end

  test "vector functions propagate null, zero, dimension, and degree behavior", %{
    session: session
  } do
    edge =
      SparkEx.sql(
        session,
        """
        SELECT CAST(array(0.0, 0.0) AS ARRAY<FLOAT>) AS zero,
               CAST(NULL AS ARRAY<FLOAT>) AS null_vector,
               CAST(array(1.0, 2.0) AS ARRAY<FLOAT>) AS short,
               CAST(array(1.0, 2.0, 3.0) AS ARRAY<FLOAT>) AS long
        """
      )

    result =
      DataFrame.select(edge, [
        Column.alias_(F.vector_cosine_similarity("zero", "short"), "zero_cosine"),
        Column.alias_(F.vector_norm("zero"), "zero_norm"),
        Column.alias_(F.vector_normalize("zero"), "zero_normalized"),
        Column.alias_(F.vector_l2_distance("null_vector", "short"), "null_distance")
      ])

    assert {:ok, [row]} = DataFrame.collect(result)
    assert row["zero_cosine"] == nil
    assert_in_delta row["zero_norm"], 0.0, 1.0e-6
    assert row["zero_normalized"] == nil
    assert row["null_distance"] == nil

    mismatched = DataFrame.select(edge, [F.vector_inner_product("short", "long")])
    assert {:error, %SparkEx.Error.Remote{}} = DataFrame.collect(mismatched)

    invalid_degree = DataFrame.select(edge, [F.vector_norm("short", 3.0)])
    assert {:error, %SparkEx.Error.Remote{}} = DataFrame.collect(invalid_degree)
  end

  defp assert_vector_in_delta(actual, expected, delta \\ 1.0e-6) do
    assert length(actual) == length(expected)

    Enum.zip(actual, expected)
    |> Enum.each(fn {value, target} -> assert_in_delta value, target, delta end)
  end
end
