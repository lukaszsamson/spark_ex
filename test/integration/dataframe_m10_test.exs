defmodule SparkEx.Integration.DataFrameM10Test do
  use ExUnit.Case

  import ExUnit.CaptureIO

  @moduletag :integration

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.Functions

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "union_by_name supports allow_missing_columns", %{session: session} do
    left = SparkEx.sql(session, "SELECT 1 AS id, 'x' AS a")
    right = SparkEx.sql(session, "SELECT 2 AS id, 'y' AS b")

    df =
      DataFrame.union_by_name(left, right, allow_missing: true)
      |> DataFrame.order_by(["id"])

    assert {:ok, rows} = DataFrame.collect(df)
    assert rows == [%{"a" => "x", "b" => nil, "id" => 1}, %{"a" => nil, "b" => "y", "id" => 2}]
  end

  test "except_all and intersect_all preserve duplicates", %{session: session} do
    left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (1), (2), (3) AS t(id)")
    right = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (2) AS t(id)")

    assert {:ok, except_rows} =
             left
             |> DataFrame.except_all(right)
             |> DataFrame.order_by(["id"])
             |> DataFrame.collect()

    assert Enum.map(except_rows, & &1["id"]) == [1, 3]

    assert {:ok, intersect_rows} =
             left
             |> DataFrame.intersect_all(right)
             |> DataFrame.order_by(["id"])
             |> DataFrame.collect()

    assert Enum.map(intersect_rows, & &1["id"]) == [1, 2]
  end

  test "select_expr and rename APIs execute", %{session: session} do
    df =
      SparkEx.sql(session, "SELECT 1 AS id, 'Alice' AS name")
      |> DataFrame.select_expr(["id", "upper(name) AS n1"])
      |> DataFrame.with_column_renamed("n1", "name1")
      |> DataFrame.to_df(["id2", "name2"])

    assert {:ok, [%{"id2" => 1, "name2" => "ALICE"}]} = DataFrame.collect(df)
  end

  test "sample and random_split execute", %{session: session} do
    df = SparkEx.range(session, 100)

    assert {:ok, sample1} = df |> DataFrame.sample(0.2, seed: 123) |> DataFrame.collect()
    assert {:ok, sample2} = df |> DataFrame.sample(0.2, seed: 123) |> DataFrame.collect()
    assert sample1 == sample2

    [a, b, c] = DataFrame.random_split(df, [1.0, 2.0, 3.0], 11)
    assert {:ok, ac} = DataFrame.count(a)
    assert {:ok, bc} = DataFrame.count(b)
    assert {:ok, cc} = DataFrame.count(c)
    assert ac + bc + cc == 100
  end

  test "hint accepts primitive parameters", %{session: session} do
    df =
      SparkEx.range(session, 20)
      |> DataFrame.hint("COALESCE", 2)

    assert {:ok, 20} = DataFrame.count(df)
  end

  test "unpivot and transpose execute", %{session: session} do
    unpivot_df =
      SparkEx.sql(session, "SELECT * FROM VALUES (1, 10, 20) AS t(id, c1, c2)")
      |> DataFrame.unpivot(["id"], ["c1", "c2"], "k", "v")
      |> DataFrame.order_by(["k"])

    assert {:ok, rows} = DataFrame.collect(unpivot_df)
    assert Enum.map(rows, &{&1["k"], &1["v"]}) == [{"c1", 10}, {"c2", 20}]

    transpose_df =
      SparkEx.sql(session, "SELECT * FROM VALUES ('a', 1), ('b', 2) AS t(k, v)")
      |> DataFrame.transpose(index_column: "k")

    assert {:ok, trows} = DataFrame.collect(transpose_df)
    assert length(trows) == 1
  end

  test "print_schema prints and html_string returns html", %{session: session} do
    df = SparkEx.sql(session, "SELECT 1 AS id, 'x' AS name")

    output = capture_io(fn -> assert :ok = DataFrame.print_schema(df, level: 2) end)
    assert String.contains?(output, "id")
    assert String.contains?(output, "name")

    assert {:ok, html} = DataFrame.html_string(df, num_rows: 5, truncate: 20)
    assert String.contains?(html, "<table")
  end

  test "with_metadata updates schema metadata", %{session: session} do
    df =
      SparkEx.sql(session, "SELECT 'x' AS name")
      |> DataFrame.with_metadata("name", %{"source" => "m10"})

    assert {:ok, schema} = DataFrame.schema(df)
    {:struct, struct} = schema.kind
    field = Enum.find(struct.fields, &(&1.name == "name"))
    assert is_binary(field.metadata)
    assert String.contains?(field.metadata, "source")
  end

  test "drop_duplicates_within_watermark returns error on batch dataframe", %{session: session} do
    df =
      SparkEx.range(session, 5)
      |> DataFrame.with_column("event_time", Functions.expr("current_timestamp()"))
      |> DataFrame.with_watermark("event_time", "10 seconds")
      |> DataFrame.drop_duplicates_within_watermark(["id"])

    assert {:error, _} = DataFrame.collect(df)
  end

  test "sort_within_partitions/coalesce/repartition pipeline executes", %{session: session} do
    df =
      SparkEx.range(session, 20)
      |> DataFrame.repartition(4, [Column.multiply(Functions.col("id"), Functions.lit(3))])
      |> DataFrame.sort_within_partitions([Column.desc(Functions.col("id"))])
      |> DataFrame.coalesce(1)

    assert {:ok, 20} = DataFrame.count(df)
  end
end
