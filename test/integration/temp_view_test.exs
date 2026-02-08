defmodule SparkEx.Integration.TempViewTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.DataFrame

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "create_or_replace_temp_view/2" do
    test "creates a temp view queryable via SQL", %{session: session} do
      view_name = "test_view_#{System.unique_integer([:positive])}"

      # Create source data
      df = SparkEx.sql(session, "SELECT * FROM VALUES (1, 'Alice'), (2, 'Bob') AS t(id, name)")

      # Create temp view
      assert :ok = DataFrame.create_or_replace_temp_view(df, view_name)

      # Query the view via SQL
      result_df = SparkEx.sql(session, "SELECT * FROM #{view_name}")
      {:ok, rows} = DataFrame.collect(result_df)
      assert length(rows) == 2
      assert Enum.any?(rows, fn row -> row["name"] == "Alice" end)
    end

    test "replaces an existing temp view", %{session: session} do
      view_name = "test_replace_view_#{System.unique_integer([:positive])}"

      # Create first view
      df1 = SparkEx.sql(session, "SELECT 1 AS x")
      assert :ok = DataFrame.create_or_replace_temp_view(df1, view_name)

      # Replace with different data
      df2 = SparkEx.sql(session, "SELECT * FROM VALUES (10), (20), (30) AS t(x)")
      assert :ok = DataFrame.create_or_replace_temp_view(df2, view_name)

      # Query — should have 3 rows
      {:ok, rows} = SparkEx.sql(session, "SELECT * FROM #{view_name}") |> DataFrame.collect()
      assert length(rows) == 3
    end
  end

  describe "create_temp_view/2" do
    test "creates a temp view", %{session: session} do
      view_name = "test_create_view_#{System.unique_integer([:positive])}"
      df = SparkEx.sql(session, "SELECT 42 AS answer")

      assert :ok = DataFrame.create_temp_view(df, view_name)

      {:ok, rows} = SparkEx.sql(session, "SELECT * FROM #{view_name}") |> DataFrame.collect()
      assert [%{"answer" => 42}] = rows
    end

    test "fails if view already exists", %{session: session} do
      view_name = "test_dup_view_#{System.unique_integer([:positive])}"
      df = SparkEx.sql(session, "SELECT 1 AS x")

      assert :ok = DataFrame.create_temp_view(df, view_name)
      assert {:error, _} = DataFrame.create_temp_view(df, view_name)
    end
  end

  describe "temp views with transforms" do
    test "creates view from filtered DataFrame", %{session: session} do
      import SparkEx.Functions, only: [col: 1, lit: 1]
      view_name = "test_filter_view_#{System.unique_integer([:positive])}"

      df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 10), (2, 20), (3, 30) AS t(id, value)"
        )

      filtered = DataFrame.filter(df, col("value") |> SparkEx.Column.gt(lit(15)))
      assert :ok = DataFrame.create_or_replace_temp_view(filtered, view_name)

      {:ok, rows} = SparkEx.sql(session, "SELECT * FROM #{view_name}") |> DataFrame.collect()
      assert length(rows) == 2
      assert Enum.all?(rows, fn row -> row["value"] > 15 end)
    end

    test "creates view and joins with another query", %{session: session} do
      view_name = "emp_view_#{System.unique_integer([:positive])}"

      emp_df =
        SparkEx.sql(
          session,
          "SELECT * FROM VALUES (1, 'Alice', 10), (2, 'Bob', 20) AS t(id, name, dept_id)"
        )

      assert :ok = DataFrame.create_or_replace_temp_view(emp_df, view_name)

      # Join via SQL using the view
      result_df =
        SparkEx.sql(
          session,
          """
          SELECT e.name, d.dept_name
          FROM #{view_name} e
          JOIN (SELECT * FROM VALUES (10, 'Engineering'), (20, 'HR') AS t(dept_id, dept_name)) d
          ON e.dept_id = d.dept_id
          """
        )

      {:ok, rows} = DataFrame.collect(result_df)
      assert length(rows) == 2

      assert Enum.any?(rows, fn row ->
               row["name"] == "Alice" and row["dept_name"] == "Engineering"
             end)
    end
  end

  describe "create_global_temp_view/2" do
    test "creates a global temp view accessible via global_temp database", %{session: session} do
      view_name = "global_test_view_#{System.unique_integer([:positive])}"
      df = SparkEx.sql(session, "SELECT 99 AS value")

      assert :ok = DataFrame.create_or_replace_global_temp_view(df, view_name)

      # Global temp views are in the global_temp database
      {:ok, rows} =
        SparkEx.sql(session, "SELECT * FROM global_temp.#{view_name}") |> DataFrame.collect()

      assert [%{"value" => 99}] = rows
    end
  end
end
