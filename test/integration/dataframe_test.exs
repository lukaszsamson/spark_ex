defmodule SparkEx.Integration.DataFrameTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame
  alias SparkEx.Column
  alias SparkEx.Functions
  alias SparkEx.Reader

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")
  @spark_test_support_sql Path.expand(
                            "../spark-4.1.1-bin-hadoop3-connect/python/test_support/sql",
                            __DIR__
                          )
  @spark_examples_resources Path.expand(
                              "../spark-4.1.1-bin-hadoop3-connect/examples/src/main/resources",
                              __DIR__
                            )

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "select/2" do
    test "projects specific columns from range", %{session: session} do
      df =
        SparkEx.range(session, 5)
        |> DataFrame.select(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 5
      assert Map.keys(hd(rows)) == ["id"]
    end

    test "projects with Column structs", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT 1 AS a, 2 AS b, 3 AS c")
        |> DataFrame.select([Functions.col("a"), Functions.col("c")])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row == %{"a" => 1, "c" => 3}
    end

    test "projects with expression (computed column)", %{session: session} do
      df =
        SparkEx.range(session, 3)
        |> DataFrame.select([
          Functions.col("id"),
          Column.alias_(
            Column.multiply(Functions.col("id"), Functions.lit(10)),
            "id_x10"
          )
        ])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      assert hd(rows)["id_x10"] == 0
      assert List.last(rows)["id_x10"] == 20
    end
  end

  describe "filter/2" do
    test "filters rows with condition", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.filter(Column.gt(Functions.col("id"), Functions.lit(6)))

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [7, 8, 9]
    end

    test "filter with equality", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.filter(Column.eq(Functions.col("id"), Functions.lit(5)))

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["id"] == 5
    end

    test "filter with compound condition", %{session: session} do
      df =
        SparkEx.range(session, 20)
        |> DataFrame.filter(
          Column.and_(
            Column.gte(Functions.col("id"), Functions.lit(5)),
            Column.lt(Functions.col("id"), Functions.lit(10))
          )
        )

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [5, 6, 7, 8, 9]
    end
  end

  describe "with_column/3" do
    test "adds a computed column", %{session: session} do
      df =
        SparkEx.range(session, 3)
        |> DataFrame.with_column(
          "doubled",
          Column.multiply(Functions.col("id"), Functions.lit(2))
        )

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      assert hd(rows)["doubled"] == 0
      assert List.last(rows)["doubled"] == 4
    end
  end

  describe "drop/2" do
    test "drops columns", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT 1 AS a, 2 AS b, 3 AS c")
        |> DataFrame.drop(["b"])

      assert {:ok, [row]} = DataFrame.collect(df)
      assert Map.keys(row) |> Enum.sort() == ["a", "c"]
    end
  end

  describe "order_by/2" do
    test "sorts ascending by default", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (3, 'c'), (1, 'a'), (2, 'b') AS t(id, letter)"
        )
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [1, 2, 3]
    end

    test "sorts descending", %{session: session} do
      df =
        SparkEx.range(session, 5)
        |> DataFrame.order_by([Column.desc(Functions.col("id"))])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [4, 3, 2, 1, 0]
    end
  end

  describe "limit/2" do
    test "limits rows", %{session: session} do
      df =
        SparkEx.range(session, 100)
        |> DataFrame.limit(5)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 5
    end
  end

  describe "chaining transforms" do
    test "filter -> select -> order_by -> limit pipeline", %{session: session} do
      df =
        SparkEx.range(session, 100)
        |> DataFrame.filter(Column.gte(Functions.col("id"), Functions.lit(50)))
        |> DataFrame.select(["id"])
        |> DataFrame.order_by([Column.desc(Functions.col("id"))])
        |> DataFrame.limit(3)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      ids = Enum.map(rows, & &1["id"])
      assert ids == [99, 98, 97]
    end
  end

  describe "show/2" do
    test "returns formatted string", %{session: session} do
      df = SparkEx.range(session, 3)
      assert {:ok, str} = DataFrame.show(df)
      assert is_binary(str)
      assert String.contains?(str, "id")
      assert String.contains?(str, "0")
    end

    test "respects num_rows option", %{session: session} do
      df = SparkEx.range(session, 100)
      assert {:ok, str} = DataFrame.show(df, num_rows: 2)
      assert is_binary(str)
      # Show should contain header + 2 data rows
      assert String.contains?(str, "id")
    end
  end

  describe "schema/1 with transforms" do
    test "returns schema after select", %{session: session} do
      df =
        SparkEx.sql(session, "SELECT 1 AS a, 'hello' AS b, true AS c")
        |> DataFrame.select(["a", "b"])

      assert {:ok, schema} = DataFrame.schema(df)
      assert schema != nil
    end

    test "returns schema after with_column", %{session: session} do
      df =
        SparkEx.range(session, 1)
        |> DataFrame.with_column(
          "doubled",
          Column.multiply(Functions.col("id"), Functions.lit(2))
        )

      assert {:ok, schema} = DataFrame.schema(df)
      assert schema != nil
    end
  end

  describe "explain/2 with transforms" do
    test "all explain modes work with transformed DataFrame", %{session: session} do
      df =
        SparkEx.range(session, 10)
        |> DataFrame.filter(Column.gt(Functions.col("id"), Functions.lit(5)))
        |> DataFrame.select(["id"])

      for mode <- [:simple, :extended, :codegen, :cost, :formatted] do
        assert {:ok, explain_str} = DataFrame.explain(df, mode)
        assert is_binary(explain_str)
        assert String.length(explain_str) > 0
      end
    end
  end

  describe "expression string" do
    test "expr/1 works in select", %{session: session} do
      df =
        SparkEx.range(session, 3)
        |> DataFrame.select([Functions.col("id"), Functions.expr("id * 2 AS doubled")])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      assert hd(rows)["doubled"] == 0
    end
  end

  describe "cast" do
    test "cast column type", %{session: session} do
      df =
        SparkEx.range(session, 3)
        |> DataFrame.select([
          Functions.col("id"),
          Column.alias_(Column.cast(Functions.col("id"), "double"), "id_double")
        ])

      assert {:ok, rows} = DataFrame.collect(df)
      assert is_float(hd(rows)["id_double"])
    end
  end

  describe "data source reading" do
    test "read from SQL-created temp view works like named table", %{session: session} do
      # Create a temp view first via SQL
      SparkEx.sql(session, "SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)")
      |> DataFrame.collect()

      # Create the temp view
      SparkEx.sql(
        session,
        "CREATE OR REPLACE TEMPORARY VIEW test_view AS SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, name)"
      )
      |> DataFrame.collect()

      # Read from the named table
      df = Reader.table(session, "test_view")
      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
    end

    test "Reader.json reads Spark test-suite dataset", %{session: session} do
      json_path = Path.join(@spark_test_support_sql, "people.json")

      df =
        Reader.json(session, json_path)
        |> DataFrame.filter(Column.is_not_null(Functions.col("age")))
        |> DataFrame.order_by(["age"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert Enum.map(rows, & &1["name"]) == ["Justin", "Andy"]
    end

    test "Reader.csv reads Spark test-suite dataset", %{session: session} do
      csv_path = Path.join(@spark_test_support_sql, "ages_newlines.csv")

      df =
        Reader.csv(
          session,
          csv_path,
          schema: "name STRING, age INT, note STRING",
          options: %{"multiLine" => "true"}
        )
        |> DataFrame.select(["name", "age"])
        |> DataFrame.order_by(["age"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      assert Enum.map(rows, & &1["age"]) == [20, 25, 30]
    end

    test "Reader.parquet reads Spark examples dataset", %{session: session} do
      parquet_path = Path.join(@spark_examples_resources, "users.parquet")

      df =
        Reader.parquet(session, parquet_path)
        |> DataFrame.select(["name", "favorite_color"])
        |> DataFrame.filter(Column.is_not_null(Functions.col("favorite_color")))
        |> DataFrame.order_by(["name"])
        |> DataFrame.limit(2)

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) >= 1
      assert Enum.all?(rows, &is_binary(&1["name"]))
    end
  end
end
