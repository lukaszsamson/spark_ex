defmodule SparkEx.M11.GroupedDataTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.GroupedData
  alias SparkEx.Functions

  defmodule FakeSession do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      {:ok, %{schema: Keyword.fetch!(opts, :schema)}}
    end

    @impl true
    def handle_call({:analyze_schema, _plan}, _from, state) do
      {:reply, {:ok, state.schema}, state}
    end
  end

  defp make_grouped(cols \\ ["dept"], session \\ self()) do
    df = %DataFrame{session: session, plan: {:sql, "SELECT * FROM t", nil}}
    DataFrame.group_by(df, cols)
  end

  defp numeric_schema do
    %Spark.Connect.DataType{
      kind:
        {:struct,
         %Spark.Connect.DataType.Struct{
           fields: [
             %Spark.Connect.DataType.StructField{
               name: "dept",
               data_type: %Spark.Connect.DataType{
                 kind: {:string, %Spark.Connect.DataType.String{}}
               },
               nullable: true
             },
             %Spark.Connect.DataType.StructField{
               name: "salary",
               data_type: %Spark.Connect.DataType{kind: {:long, %Spark.Connect.DataType.Long{}}},
               nullable: true
             },
             %Spark.Connect.DataType.StructField{
               name: "bonus",
               data_type: %Spark.Connect.DataType{
                 kind: {:double, %Spark.Connect.DataType.Double{}}
               },
               nullable: true
             }
           ]
         }}
    }
  end

  # ── Convenience aggregation methods ──

  describe "count/2" do
    test "count with no columns uses count(1) and alias" do
      result = make_grouped() |> GroupedData.count()

      assert %DataFrame{
               plan:
                 {:aggregate, _, :groupby, [{:col, "dept"}],
                  [{:alias, {:fn, "count", [{:lit, 1}], false}, "count"}]}
             } = result
    end

    test "count with specific columns" do
      result = make_grouped() |> GroupedData.count(["salary"])

      assert %DataFrame{
               plan:
                 {:aggregate, _, :groupby, [{:col, "dept"}],
                  [{:fn, "count", [{:col, "salary"}], false}]}
             } = result
    end

    test "count with Column structs" do
      result = make_grouped() |> GroupedData.count([Functions.col("salary")])

      assert %DataFrame{
               plan: {:aggregate, _, :groupby, _, [{:fn, "count", [{:col, "salary"}], false}]}
             } = result
    end
  end

  describe "min/2" do
    test "min with no columns expands numeric columns" do
      {:ok, session} = FakeSession.start_link(schema: numeric_schema())
      result = make_grouped(["dept"], session) |> GroupedData.min()

      assert %DataFrame{plan: {:aggregate, _, :groupby, _, agg_exprs}} = result
      assert length(agg_exprs) == 2
      assert {:fn, "min", [{:col, "salary"}], false} = hd(agg_exprs)
    end
  end

  describe "max/2" do
    test "max with no columns expands numeric columns" do
      {:ok, session} = FakeSession.start_link(schema: numeric_schema())
      result = make_grouped(["dept"], session) |> GroupedData.max()

      assert %DataFrame{plan: {:aggregate, _, :groupby, _, agg_exprs}} = result
      assert length(agg_exprs) == 2
    end
  end

  describe "sum/2" do
    test "sum with no columns expands numeric columns" do
      {:ok, session} = FakeSession.start_link(schema: numeric_schema())
      result = make_grouped(["dept"], session) |> GroupedData.sum()

      assert %DataFrame{plan: {:aggregate, _, :groupby, _, agg_exprs}} = result
      assert length(agg_exprs) == 2
    end
  end

  describe "avg/2" do
    test "avg with no columns expands numeric columns" do
      {:ok, session} = FakeSession.start_link(schema: numeric_schema())
      result = make_grouped(["dept"], session) |> GroupedData.avg()

      assert %DataFrame{plan: {:aggregate, _, :groupby, _, agg_exprs}} = result
      assert length(agg_exprs) == 2
    end
  end

  describe "mean/2" do
    test "mean is alias for avg (uses avg spark function name)" do
      {:ok, session} = FakeSession.start_link(schema: numeric_schema())
      result = make_grouped(["dept"], session) |> GroupedData.mean(["salary"])

      assert %DataFrame{
               plan: {:aggregate, _, :groupby, _, [{:fn, "avg", [{:col, "salary"}], false}]}
             } = result
    end
  end

  # ── Pivot ──

  describe "pivot/3" do
    test "sets pivot column and values" do
      gd = make_grouped() |> GroupedData.pivot("course", ["dotNET", "Java"])

      assert gd.pivot_col == {:col, "course"}
      assert gd.pivot_values == ["dotNET", "Java"]
    end

    test "pivot with Column struct" do
      gd = make_grouped() |> GroupedData.pivot(Functions.col("course"), ["dotNET"])

      assert gd.pivot_col == {:col, "course"}
    end

    test "pivot with nil values" do
      gd = make_grouped() |> GroupedData.pivot("course")

      assert gd.pivot_col == {:col, "course"}
      assert gd.pivot_values == nil
    end

    test "pivot + agg creates pivot aggregate plan" do
      result =
        make_grouped()
        |> GroupedData.pivot("course", ["dotNET", "Java"])
        |> GroupedData.agg([Functions.sum(Functions.col("earnings"))])

      assert %DataFrame{
               plan:
                 {:aggregate, _, :pivot, [{:col, "dept"}],
                  [{:fn, "sum", [{:col, "earnings"}], false}], {:col, "course"},
                  ["dotNET", "Java"]}
             } = result
    end

    test "pivot + count convenience" do
      result =
        make_grouped()
        |> GroupedData.pivot("course", ["dotNET"])
        |> GroupedData.count()

      assert %DataFrame{
               plan:
                 {:aggregate, _, :pivot, [{:col, "dept"}],
                  [{:alias, {:fn, "count", [{:lit, 1}], false}, "count"}], {:col, "course"},
                  ["dotNET"]}
             } = result
    end
  end

  # ── Error cases ──

  describe "error handling" do
    test "agg raises when given non-list" do
      assert_raise ArgumentError, ~r/expected aggregate expressions as a non-empty list/, fn ->
        make_grouped() |> GroupedData.agg(:bad)
      end
    end
  end

  # ── Session preservation ──

  describe "session preservation" do
    test "convenience methods preserve session" do
      session = self()
      gd = make_grouped()
      assert gd.session == session

      result = GroupedData.count(gd)
      assert result.session == session
    end
  end
end
