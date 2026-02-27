defmodule SparkEx.Integration.DataFrameTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame
  alias SparkEx.Column
  alias SparkEx.Functions
  alias SparkEx.Reader

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")
  @spark_home System.get_env(
                "SPARK_HOME",
                Path.expand("../spark-4.1.1-bin-hadoop3-connect", __DIR__)
              )
  @spark_test_support_sql Path.join(@spark_home, "python/test_support/sql")
  @spark_examples_resources Path.join(@spark_home, "examples/src/main/resources")

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

  # --- Milestone 3: join, group_by + agg, distinct, union ---

  describe "group_by/2 + agg/2" do
    test "counts by group", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('eng', 100), ('eng', 200), ('hr', 150) AS t(dept, salary)"
        )
        |> DataFrame.group_by(["dept"])
        |> SparkEx.GroupedData.agg([
          Column.alias_(Functions.count(Functions.col("salary")), "cnt")
        ])
        |> DataFrame.order_by(["dept"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.find(rows, &(&1["dept"] == "eng"))["cnt"] == 2
      assert Enum.find(rows, &(&1["dept"] == "hr"))["cnt"] == 1
    end

    test "sum and avg aggregates", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('eng', 100), ('eng', 200), ('hr', 150) AS t(dept, salary)"
        )
        |> DataFrame.group_by(["dept"])
        |> SparkEx.GroupedData.agg([
          Column.alias_(Functions.sum(Functions.col("salary")), "total"),
          Column.alias_(Functions.avg(Functions.col("salary")), "average")
        ])
        |> DataFrame.filter(Column.eq(Functions.col("dept"), Functions.lit("eng")))

      assert {:ok, [row]} = DataFrame.collect(df)
      assert row["total"] == 300
      assert row["average"] == 150.0
    end
  end

  describe "join/4" do
    test "inner join using columns", %{session: session} do
      left =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol') AS t(id, name)"
        )

      right =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'eng'), (2, 'hr'), (4, 'sales') AS t(id, dept)"
        )

      df =
        DataFrame.join(left, right, ["id"], :inner)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      ids = Enum.map(rows, & &1["id"])
      assert ids == [1, 2]
    end

    test "inner join on condition (list of Column predicates)", %{session: session} do
      left =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 10), (1, 20), (2, 30) AS t(id_l, value_l)"
        )

      right =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 10, 'x'), (1, 99, 'y'), (2, 30, 'z') AS t(id_r, value_r, tag)"
        )

      df =
        DataFrame.join(
          left,
          right,
          [
            Column.eq(Functions.col("id_l"), Functions.col("id_r")),
            Column.eq(Functions.col("value_l"), Functions.col("value_r"))
          ],
          :inner
        )
        |> DataFrame.select(["id_l", "value_l", "tag"])
        |> DataFrame.order_by(["id_l", "value_l"])

      assert {:ok, rows} = DataFrame.collect(df)

      assert Enum.map(rows, &{&1["id_l"], &1["value_l"], &1["tag"]}) == [
               {1, 10, "x"},
               {2, 30, "z"}
             ]
    end

    test "left outer join", %{session: session} do
      left =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol') AS t(id, name)"
        )

      right =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'eng'), (2, 'hr') AS t(id, dept)"
        )

      df =
        DataFrame.join(left, right, ["id"], :left)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      # Carol (id=3) should have null dept
      carol = Enum.find(rows, &(&1["id"] == 3))
      assert carol["dept"] == nil
    end

    test "cross join", %{session: session} do
      left = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2) AS t(a)")
      right = SparkEx.sql(session, "SELECT * FROM VALUES ('x'), ('y') AS t(b)")

      df =
        DataFrame.join(left, right, [], :cross)
        |> DataFrame.order_by(["a", "b"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 4
    end
  end

  describe "session guards" do
    test "join and set operations reject different sessions", %{session: session} do
      {:ok, other_session} = SparkEx.connect(url: @spark_remote)
      Process.unlink(other_session)

      on_exit(fn ->
        if Process.alive?(other_session), do: SparkEx.Session.stop(other_session)
      end)

      left = SparkEx.range(session, 3)
      right = SparkEx.range(other_session, 3)

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.join(left, right, ["id"], :inner)
      end

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.union(left, right)
      end

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.union_distinct(left, right)
      end

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.intersect(left, right)
      end

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.except(left, right)
      end
    end
  end

  describe "distinct/1" do
    test "removes duplicate rows", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'a'), (2, 'b'), (1, 'a'), (3, 'c'), (2, 'b') AS t(id, name)"
        )
        |> DataFrame.distinct()
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 3
      ids = Enum.map(rows, & &1["id"])
      assert ids == [1, 2, 3]
    end
  end

  describe "union/2" do
    test "unions two DataFrames (preserves duplicates)", %{session: session} do
      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (2), (3), (4) AS t(id)")

      df =
        DataFrame.union(df1, df2)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [1, 2, 2, 3, 3, 4]
    end
  end

  describe "union_distinct/2" do
    test "unions two DataFrames removing duplicates", %{session: session} do
      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (2), (3), (4) AS t(id)")

      df =
        DataFrame.union_distinct(df1, df2)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [1, 2, 3, 4]
    end
  end

  describe "intersect/2" do
    test "returns common rows", %{session: session} do
      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (2), (3), (4) AS t(id)")

      df =
        DataFrame.intersect(df1, df2)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [2, 3]
    end
  end

  describe "except/2" do
    test "returns rows only in first DataFrame", %{session: session} do
      df1 = SparkEx.sql(session, "SELECT * FROM VALUES (1), (2), (3) AS t(id)")
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (2), (3), (4) AS t(id)")

      df =
        DataFrame.except(df1, df2)
        |> DataFrame.order_by(["id"])

      assert {:ok, rows} = DataFrame.collect(df)
      ids = Enum.map(rows, & &1["id"])
      assert ids == [1]
    end
  end

  describe "join + aggregate pipeline" do
    test "join then group_by + agg", %{session: session} do
      employees =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Carol', 2) AS t(id, name, dept_id)"
        )

      departments =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'Engineering'), (2, 'HR') AS t(dept_id, dept_name)"
        )

      df =
        DataFrame.join(employees, departments, ["dept_id"], :inner)
        |> DataFrame.group_by(["dept_name"])
        |> SparkEx.GroupedData.agg([
          Column.alias_(Functions.count(Functions.col("id")), "headcount")
        ])
        |> DataFrame.order_by(["dept_name"])

      assert {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
      assert Enum.find(rows, &(&1["dept_name"] == "Engineering"))["headcount"] == 2
      assert Enum.find(rows, &(&1["dept_name"] == "HR"))["headcount"] == 1
    end
  end

  describe "like/2" do
    test "filters with LIKE pattern", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES ('Alice'), ('Bob'), ('Andy'), ('Carol') AS t(name)"
        )
        |> DataFrame.filter(Column.like(Functions.col("name"), Functions.lit("A%")))
        |> DataFrame.order_by(["name"])

      assert {:ok, rows} = DataFrame.collect(df)
      names = Enum.map(rows, & &1["name"])
      assert names == ["Alice", "Andy"]
    end
  end

  describe "to_explorer/2" do
    test "returns Explorer.DataFrame from range", %{session: session} do
      df = SparkEx.range(session, 5)
      assert {:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 100)
      assert %Explorer.DataFrame{} = explorer_df
      assert Explorer.DataFrame.n_rows(explorer_df) == 5
      assert "id" in Explorer.DataFrame.names(explorer_df)
    end

    test "returns Explorer.DataFrame from SQL", %{session: session} do
      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")
      assert {:ok, explorer_df} = DataFrame.to_explorer(df)
      assert Explorer.DataFrame.n_rows(explorer_df) == 2
      assert "id" in Explorer.DataFrame.names(explorer_df)
      assert "name" in Explorer.DataFrame.names(explorer_df)
    end

    test "respects max_rows limit by injecting LIMIT", %{session: session} do
      df = SparkEx.range(session, 100)
      assert {:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 5)
      assert Explorer.DataFrame.n_rows(explorer_df) <= 5
    end

    test "unsafe: true skips LIMIT injection", %{session: session} do
      df = SparkEx.range(session, 10)

      assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :rows}} =
               DataFrame.to_explorer(df, max_rows: 5, unsafe: true)
    end

    test "unsafe: true can be fully unbounded with explicit decoder opts", %{session: session} do
      df = SparkEx.range(session, 10)

      assert {:ok, explorer_df} =
               DataFrame.to_explorer(df, unsafe: true, max_rows: :infinity, max_bytes: :infinity)

      assert Explorer.DataFrame.n_rows(explorer_df) == 10
    end

    test "preserves schema for empty result sets", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT CAST(1 AS INT) AS id, CAST('x' AS STRING) AS name WHERE 1 = 0"
        )

      assert {:ok, explorer_df} = DataFrame.to_explorer(df)
      assert Explorer.DataFrame.n_rows(explorer_df) == 0
      assert Explorer.DataFrame.names(explorer_df) == ["id", "name"]
    end

    test "materializes null-heavy public json dataset", %{session: session} do
      json_path = Path.join(@spark_test_support_sql, "people.json")

      df =
        Reader.json(session, json_path)
        |> DataFrame.select(["name", "age"])
        |> DataFrame.order_by(["name"])

      assert {:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 100)
      assert Explorer.DataFrame.names(explorer_df) == ["name", "age"]
      assert Explorer.DataFrame.n_rows(explorer_df) == 3

      rows = Explorer.DataFrame.to_rows(explorer_df)
      assert Enum.any?(rows, &(&1["age"] == nil))
    end

    test "materializes public parquet dataset", %{session: session} do
      parquet_path = Path.join(@spark_examples_resources, "users.parquet")

      df =
        Reader.parquet(session, parquet_path)
        |> DataFrame.select(["name", "favorite_color"])
        |> DataFrame.order_by(["name"])
        |> DataFrame.limit(3)

      assert {:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 10)
      assert Explorer.DataFrame.names(explorer_df) == ["name", "favorite_color"]
      assert Explorer.DataFrame.n_rows(explorer_df) >= 1
    end

    test "materializes public csv dataset", %{session: session} do
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

      assert {:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 10)
      assert Explorer.DataFrame.names(explorer_df) == ["name", "age"]
      assert Explorer.DataFrame.n_rows(explorer_df) == 3
    end

    test "falls back complex types to strings", %{session: session} do
      df =
        SparkEx.sql(
          session,
          "SELECT array(1, 2) AS arr, named_struct('k', 1, 'v', 'x') AS st, map('a', 1) AS mp"
        )

      assert {:ok, explorer_df} = DataFrame.to_explorer(df, max_rows: 10)
      [row] = Explorer.DataFrame.to_rows(explorer_df)
      assert is_binary(row["arr"]) or is_list(row["arr"])
      assert is_binary(row["st"]) or is_map(row["st"])
      assert is_binary(row["mp"]) or is_list(row["mp"])
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
