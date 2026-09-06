defmodule SparkEx.BugsFableDataFrameTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.Column
  alias SparkEx.DataFrame
  alias SparkEx.DataFrame.NA
  alias SparkEx.Functions
  alias SparkEx.GroupedData
  alias SparkEx.TableValuedFunction

  defp make_df(plan \\ {:sql, "SELECT * FROM t", nil}) do
    DataFrame.new(self(), plan)
  end

  # A GenServer that answers :analyze_schema with a fixed schema, for the
  # GroupedData numeric-validation path (FABLE-20).
  defmodule SchemaSession do
    use GenServer

    def start_link(schema), do: GenServer.start_link(__MODULE__, schema)

    @impl true
    def init(schema), do: {:ok, schema}

    @impl true
    def handle_call({:analyze_schema, _plan}, _from, schema) do
      {:reply, {:ok, schema}, schema}
    end
  end

  defp schema_with(fields) do
    %Spark.Connect.DataType{
      kind:
        {:struct,
         %Spark.Connect.DataType.Struct{
           fields:
             Enum.map(fields, fn {name, kind} ->
               %Spark.Connect.DataType.StructField{
                 name: name,
                 data_type: %Spark.Connect.DataType{kind: kind},
                 nullable: true
               }
             end)
         }}
    }
  end

  # A GenServer that returns a fixed error for execute_command_with_result,
  # for the checkpoint error-surfacing path (FABLE-39).
  defmodule ErrorCheckpointSession do
    use GenServer

    def start_link(error), do: GenServer.start_link(__MODULE__, error)

    @impl true
    def init(error), do: {:ok, error}

    @impl true
    def handle_call(
          {:execute_command_with_result, {:checkpoint, _, _, _, _}, _opts},
          _from,
          error
        ) do
      {:reply, {:error, error}, error}
    end
  end

  # ── FABLE-09: raw-string star handling matches col/1 ──

  describe "FABLE-09 raw-string star handling" do
    test "select(\"*\") routes through the star expr, not UnresolvedAttribute" do
      result = DataFrame.select(make_df(), ["*"])
      assert {:project, _, [{:star}]} = unwrap_plan(result)
    end

    test "select(\"x.*\") becomes a qualified star expr" do
      result = DataFrame.select(make_df(), ["x.*"])
      assert {:project, _, [{:star, "x.*"}]} = unwrap_plan(result)
    end

    test "group_by(\"*\") routes through the star expr" do
      result = DataFrame.group_by(make_df(), ["*"])
      assert %GroupedData{grouping_exprs: [{:star}]} = result
    end

    test "dict-agg %{\"*\" => \"count\"} routes through star handling" do
      result =
        make_df()
        |> DataFrame.group_by(["dept"])
        |> GroupedData.agg(%{"*" => "count"})

      # T-40: no explicit alias for the star key — Spark names the resolved
      # expression `count(1)` (PySpark parity).
      assert {:aggregate, _, :groupby, _, [{:fn, "count", [{:star}], false}]} =
               unwrap_plan(result)
    end
  end

  # ── FABLE-18: integer column args raise ──

  describe "FABLE-18 integer column args raise" do
    test "select with integer raises ArgumentError" do
      assert_raise ArgumentError, ~r/integer column ordinals are not supported/, fn ->
        DataFrame.select(make_df(), [1])
      end
    end

    test "group_by with zero ordinal raises ArgumentError" do
      assert_raise ArgumentError, ~r/grouping column ordinals must be positive/, fn ->
        DataFrame.group_by(make_df(), [0])
      end
    end
  end

  # ── FABLE-19: ascending: true leaves explicit sort orders untouched ──

  describe "FABLE-19 ascending: true preserves explicit desc" do
    test "ascending: true does not override an explicit .desc()" do
      df = make_df()
      desc_col = Column.desc(Functions.col("age"))
      result = DataFrame.order_by(df, [desc_col], ascending: true)

      assert {:sort, _, [{:sort_order, {:col, "age"}, :desc, :nulls_last}]} = unwrap_plan(result)
    end

    test "ascending: true defaults a plain string to ascending" do
      result = DataFrame.order_by(make_df(), ["name"], ascending: true)
      assert {:sort, _, [{:sort_order, {:col, "name"}, :asc, :nulls_first}]} = unwrap_plan(result)
    end

    test "ascending list leaves truthy entries untouched but forces desc on falsy" do
      df = make_df()
      asc_col = Column.asc(Functions.col("a"))
      desc_col = Column.desc(Functions.col("b"))
      result = DataFrame.order_by(df, [asc_col, desc_col], ascending: [true, false])

      assert {:sort, _,
              [
                {:sort_order, {:col, "a"}, :asc, :nulls_first},
                {:sort_order, {:col, "b"}, :desc, :nulls_last}
              ]} = unwrap_plan(result)
    end
  end

  # ── FABLE-20: GroupedData numeric shortcuts validate explicit columns ──

  describe "FABLE-20 numeric column validation" do
    test "min on a non-numeric explicit column raises" do
      schema = schema_with([{"name", {:string, %Spark.Connect.DataType.String{}}}])
      {:ok, session} = SchemaSession.start_link(schema)
      gd = DataFrame.group_by(DataFrame.new(session, {:sql, "SELECT * FROM t", nil}), ["g"])

      assert_raise ArgumentError, ~r/numeric/, fn ->
        GroupedData.min(gd, "name")
      end
    end

    test "sum on a numeric explicit column succeeds" do
      schema = schema_with([{"salary", {:long, %Spark.Connect.DataType.Long{}}}])
      {:ok, session} = SchemaSession.start_link(schema)
      gd = DataFrame.group_by(DataFrame.new(session, {:sql, "SELECT * FROM t", nil}), ["g"])

      result = GroupedData.sum(gd, "salary")

      assert {:aggregate, _, :groupby, _, [{:fn, "sum", [{:col, "salary"}], false}]} =
               unwrap_plan(result)
    end
  end

  # ── FABLE-21: drop keeps Column args as expressions ──

  describe "FABLE-21 drop keeps Column args as expressions" do
    test "drop([col(\"id\")]) keeps the column as an expression" do
      result = DataFrame.drop(make_df(), [Functions.col("id")])
      assert {:drop, _, [], [{:col, "id"}]} = unwrap_plan(result)
    end

    test "drop with mixed Column and string splits exprs and names" do
      result = DataFrame.drop(make_df(), [Functions.col("a"), "b"])
      assert {:drop, _, ["b"], [{:col, "a"}]} = unwrap_plan(result)
    end

    test "drop with a bare string still becomes a name" do
      result = DataFrame.drop(make_df(), "c")
      assert {:drop, _, ["c"], []} = unwrap_plan(result)
    end
  end

  # ── FABLE-30: TVF string args become column refs ──

  describe "FABLE-30 TVF string args become column refs" do
    test "table_function with a bare string arg becomes a column reference" do
      df = DataFrame.table_function(self(), "explode", ["arr"])
      assert {:table_valued_function, "explode", [{:col, "arr"}]} = unwrap_plan(df)
    end

    test "TableValuedFunction.explode/2 with a bare string becomes a column reference" do
      tvf = TableValuedFunction.new(self())
      df = TableValuedFunction.explode(tvf, "arr")
      assert {:table_valued_function, "explode", [{:col, "arr"}]} = unwrap_plan(df)
    end

    test "non-string scalar args remain literals" do
      df = DataFrame.table_function(self(), "range", [0, 10])
      assert {:table_valued_function, "range", [{:lit, 0}, {:lit, 10}]} = unwrap_plan(df)
    end

    test "string \"*\" arg routes through star handling" do
      df = DataFrame.table_function(self(), "explode", ["*"])
      assert {:table_valued_function, "explode", [{:star}]} = unwrap_plan(df)
    end
  end

  # ── FABLE-39: checkpoint surfaces server errors ──

  describe "FABLE-39 checkpoint surfaces server errors" do
    test "checkpoint surfaces a 'not supported.' Remote error" do
      error = %SparkEx.Error.Remote{message: "checkpoint command is not supported."}
      {:ok, session} = ErrorCheckpointSession.start_link(error)
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:error, ^error} = DataFrame.checkpoint(df)
    end

    test "local_checkpoint surfaces a 'not supported.' Remote error" do
      error = %SparkEx.Error.Remote{message: "checkpoint command is not supported."}
      {:ok, session} = ErrorCheckpointSession.start_link(error)
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:error, ^error} = DataFrame.local_checkpoint(df)
    end
  end

  # ── FABLE-40: NA.drop validates :how even with :thresh ──

  describe "FABLE-40 NA.drop validates :how with :thresh" do
    test "bogus :how raises even when :thresh is provided" do
      assert_raise ArgumentError, ~r/:how to be :any or :all/, fn ->
        NA.drop(make_df(), how: :bogus, thresh: 2)
      end
    end

    test "valid :how with :thresh applies thresh as min_non_nulls" do
      result = NA.drop(make_df(:p), how: :any, thresh: 2)
      assert {:na_drop, :p, [], 2} = unwrap_plan(result)
    end
  end

  # ── FABLE-41: sample/4 accepts fraction > 1.0 without replacement ──

  describe "FABLE-41 sample accepts fraction > 1.0 without replacement" do
    test "fraction > 1.0 without replacement is accepted" do
      result = DataFrame.sample(make_df(), 1.5, seed: 1)
      assert {:sample, _, +0.0, 1.5, false, 1, false} = unwrap_plan(result)
    end

    test "negative fraction is still rejected" do
      assert_raise ArgumentError, ~r/sample fraction must be >= 0/, fn ->
        DataFrame.sample(make_df(), -0.1)
      end
    end
  end

  # ── FABLE-42: {name, alias} tuple form aliases the column ──

  describe "FABLE-42 {name, alias} tuple form aliases" do
    test "unpivot value tuples produce aliased expressions" do
      result = DataFrame.unpivot(make_df(:p), ["id"], [{"c1", "a1"}], "var", "val")

      assert {:unpivot, :p, [{:col, "id"}], [{:alias, {:col, "c1"}, "a1"}], "var", "val"} =
               unwrap_plan(result)
    end
  end
end
