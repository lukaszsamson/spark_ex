defmodule SparkEx.Unit.Spark42FunctionsTest do
  use ExUnit.Case, async: true

  alias SparkEx.Column
  alias SparkEx.Functions, as: F

  test "new scalar functions preserve column argument order" do
    assert %Column{expr: {:fn, "is_valid_variant", [{:col, "v"}], false}} =
             F.is_valid_variant("v")

    assert %Column{expr: {:fn, "current_path", [], false}} = F.current_path()

    for name <-
          ~w(time_from_seconds time_from_millis time_from_micros time_to_seconds time_to_millis time_to_micros)a do
      assert %Column{expr: {:fn, spark_name, [{:col, "t"}], false}} = apply(F, name, ["t"])
      assert spark_name == Atom.to_string(name)
    end
  end

  test "time_bucket keeps interval and origin as expressions" do
    bucket = F.expr("INTERVAL '15' MINUTE")
    origin = F.expr("TIMESTAMP '1970-01-01 00:00:00'")

    assert %Column{expr: {:fn, "time_bucket", [bucket_expr, {:col, "ts"}], false}} =
             F.time_bucket(bucket, "ts")

    assert bucket_expr == bucket.expr

    assert %Column{expr: {:fn, "time_bucket", [^bucket_expr, {:col, "ts"}, origin_expr], false}} =
             F.time_bucket(bucket, "ts", origin)

    assert origin_expr == origin.expr

    assert %Column{
             expr: {:fn, "time_bucket", [^bucket_expr, {:col, "ts"}, {:col, "origin"}], false}
           } = F.time_bucket(bucket, "ts", "origin")

    assert F.time_bucket(bucket, "ts", origin: origin) == F.time_bucket(bucket, "ts", origin)
  end

  test "max_by and min_by retain two arguments and encode k as a literal" do
    assert %Column{expr: {:fn, "max_by", [{:col, "v"}, {:col, "ord"}], false}} =
             F.max_by("v", "ord")

    assert %Column{expr: {:fn, "min_by", [{:col, "v"}, {:col, "ord"}, {:lit, 3}], false}} =
             F.min_by("v", "ord", 3)
  end

  test "tuple sketch defaults are always emitted and explicit columns pass through" do
    assert %Column{
             expr:
               {:fn, "tuple_sketch_agg_double",
                [{:col, "key"}, {:col, "summary"}, {:lit, 12}, {:lit, "sum"}], false}
           } = F.tuple_sketch_agg_double("key", "summary")

    mode = F.col("mode")

    assert %Column{
             expr:
               {:fn, "tuple_union_integer",
                [{:col, "left"}, {:col, "right"}, {:lit, 32}, {:col, "mode"}], false}
           } = F.tuple_union_integer("left", "right", 32, mode)

    assert %Column{expr: {:fn, "tuple_intersection_agg_double", [{:col, "s"}], false}} =
             F.tuple_intersection_agg_double("s")
  end

  test "KLL merge aggregates omit k by default" do
    assert %Column{expr: {:fn, "kll_merge_agg_bigint", [{:col, "s"}], false}} =
             F.kll_merge_agg_bigint("s")

    assert %Column{expr: {:fn, "kll_merge_agg_float", [{:col, "s"}, {:lit, 200}], false}} =
             F.kll_merge_agg_float("s", 200)
  end

  test "geospatial overloads follow PySpark literal and expression coercion" do
    assert %Column{expr: {:fn, "ST_AsBinary", [{:col, "geo"}, {:lit, "little-endian"}], false}} =
             F.st_asbinary("geo", "little-endian")

    assert %Column{expr: {:fn, "ST_GeomFromWKB", [{:col, "wkb"}, {:lit, 4326}], false}} =
             F.st_geomfromwkb("wkb", 4326)

    assert %Column{expr: {:fn, "ST_GeomFromWKB", [{:col, "wkb"}, {:col, "srid"}], false}} =
             F.st_geomfromwkb("wkb", "srid")

    assert F.st_geomfromwkb("wkb", nil) == F.st_geomfromwkb("wkb")
    assert F.st_geomfromwkb("wkb", srid: nil) == F.st_geomfromwkb("wkb")
  end
end
