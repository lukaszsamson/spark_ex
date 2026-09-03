defmodule SparkEx.Unit.Wave1BuildersTest do
  use ExUnit.Case, async: true

  alias SparkEx.Writer
  alias SparkEx.DataFrame
  alias SparkEx.DataFrame.NA
  alias SparkEx.Catalog

  defmodule FakeSession do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {:ok, %{parent: Keyword.fetch!(opts, :parent)}}
    end

    @impl true
    def handle_call({:execute_command, command, exec_opts}, _from, state) do
      send(state.parent, {:execute_command, command, exec_opts})
      {:reply, :ok, state}
    end
  end

  describe "T-20: Writer.jdbc/2 and /4 use seed_writer" do
    test "jdbc/4 rejects a partitioned writer like Spark's DataFrameWriter.jdbc" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert_raise ArgumentError, ~r/jdbc writes do not support/, fn ->
        df
        |> DataFrame.write()
        |> Writer.partition_by("p")
        |> Writer.jdbc("jdbc:postgresql://host/db", "my_table")
      end
    end

    test "jdbc/4 accepts an existing Writer builder without crashing (previously KeyError)" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.mode(:overwrite)
               |> Writer.option("fetchsize", "100")
               |> Writer.jdbc("jdbc:postgresql://host/db", "my_table")

      assert_receive {:execute_command, {:write_operation, _, write_opts}, _exec_opts}
      assert Keyword.get(write_opts, :mode) == :overwrite
      assert Keyword.get(write_opts, :format) == "jdbc"

      options = Keyword.get(write_opts, :options)
      assert options["url"] == "jdbc:postgresql://host/db"
      assert options["dbtable"] == "my_table"
      assert options["fetchsize"] == "100"
    end

    test "jdbc/4 with a bare DataFrame still works" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert :ok = Writer.jdbc(df, "jdbc:postgresql://host/db", "t", mode: :append)

      assert_receive {:execute_command, {:write_operation, _, write_opts}, _exec_opts}
      assert Keyword.get(write_opts, :mode) == :append
      options = Keyword.get(write_opts, :options)
      assert options["url"] == "jdbc:postgresql://host/db"
      assert options["dbtable"] == "t"
    end

    test "jdbc/2 accepts an existing Writer builder and preserves options" do
      {:ok, session} = FakeSession.start_link(parent: self())
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert :ok =
               df
               |> DataFrame.write()
               |> Writer.mode(:ignore)
               |> Writer.option("url", "jdbc:postgresql://host/db")
               |> Writer.option("dbtable", "t2")
               |> Writer.jdbc([])

      assert_receive {:execute_command, {:write_operation, _, write_opts}, _exec_opts}
      assert Keyword.get(write_opts, :mode) == :ignore
      options = Keyword.get(write_opts, :options)
      assert options["url"] == "jdbc:postgresql://host/db"
      assert options["dbtable"] == "t2"
    end
  end

  describe "T-21: Catalog.build_create_function_sql" do
    test "IF NOT EXISTS comes after FUNCTION, not before" do
      sql = Catalog.build_create_function_sql("my_func", "com.example.MyFunc", [])
      assert sql == "CREATE FUNCTION `my_func` AS 'com.example.MyFunc'"
    end

    test "temporary + if_not_exists placement" do
      sql =
        Catalog.build_create_function_sql("my_func", "com.example.MyFunc",
          temporary: true,
          if_not_exists: true
        )

      assert sql == "CREATE TEMPORARY FUNCTION IF NOT EXISTS `my_func` AS 'com.example.MyFunc'"
    end

    test "multiple USING resources are comma-separated" do
      sql =
        Catalog.build_create_function_sql("my_func", "com.example.MyFunc",
          using_jars: ["a.jar", "b.jar"]
        )

      assert sql ==
               "CREATE FUNCTION `my_func` AS 'com.example.MyFunc' USING JAR 'a.jar', JAR 'b.jar'"
    end

    test "empty resource list emits no USING clause" do
      sql =
        Catalog.build_create_function_sql("my_func", "com.example.MyFunc", using_jars: [])

      assert sql == "CREATE FUNCTION `my_func` AS 'com.example.MyFunc'"
    end

    test "single using_jar still works" do
      sql =
        Catalog.build_create_function_sql("my_func", "com.example.MyFunc", using_jar: "a.jar")

      assert sql == "CREATE FUNCTION `my_func` AS 'com.example.MyFunc' USING JAR 'a.jar'"
    end
  end

  describe "T-24: NA.replace/4 requires an explicit value for non-map to_replace" do
    test "raises ArgumentError when to_replace is a scalar and no value given" do
      df = DataFrame.new(self(), {:sql, "SELECT 1", nil})

      assert_raise ArgumentError, ~r/requires a `value` argument/, fn ->
        NA.replace(df, "N/A")
      end
    end

    test "raises ArgumentError when to_replace is a list and no value given" do
      df = DataFrame.new(self(), {:sql, "SELECT 1", nil})

      assert_raise ArgumentError, ~r/requires a `value` argument/, fn ->
        NA.replace(df, ["N/A", "NULL"])
      end
    end

    test "does not raise when to_replace is a map (value not required)" do
      df = DataFrame.new(self(), {:sql, "SELECT 1", nil})
      result = NA.replace(df, %{"N/A" => nil})
      assert %DataFrame{} = result
    end

    test "explicit nil value is honored (replace with null) without raising" do
      df = DataFrame.new(self(), {:sql, "SELECT 1", nil})
      result = NA.replace(df, "N/A", nil)
      assert %DataFrame{} = result
    end

    test "scalar to_replace with explicit value still works" do
      df = DataFrame.new(self(), {:sql, "SELECT 1", nil})
      result = NA.replace(df, "N/A", "unknown")
      assert %DataFrame{} = result
    end
  end
end
