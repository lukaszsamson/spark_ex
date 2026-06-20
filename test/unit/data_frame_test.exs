defmodule SparkEx.DataFrameTest do
  use ExUnit.Case, async: true

  import SparkEx.Test.PlanHelpers

  alias SparkEx.DataFrame
  alias SparkEx.Column
  alias SparkEx.Functions
  alias SparkEx.TableValuedFunction
  alias Spark.Connect.ExecutePlanResponse

  defmodule ArrowSession do
    use GenServer

    def start_link(test_pid \\ nil) do
      GenServer.start_link(__MODULE__, test_pid, [])
    end

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:execute_arrow, _plan, _opts}, _from, test_pid) do
      {:reply, {:ok, :arrow_data}, test_pid}
    end

    @impl true
    def handle_call(:get_state, _from, test_pid) do
      {:reply, :mock_session, test_pid}
    end

    @impl true
    def handle_call({:execute_collect, _plan, _opts}, _from, test_pid) do
      if test_pid, do: send(test_pid, :execute_collect_called)
      {:reply, {:ok, [%{"id" => 1}]}, test_pid}
    end

    @impl true
    def handle_call({:execute_plan_stream, _plan, _opts}, _from, test_pid) do
      if test_pid, do: send(test_pid, :execute_plan_stream_called)

      stream =
        case SparkEx.DataFrameTest.build_test_ipc_data() do
          <<>> ->
            [
              {:ok,
               %ExecutePlanResponse{
                 response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
               }}
            ]

          ipc_data ->
            batch = %ExecutePlanResponse.ArrowBatch{data: ipc_data, row_count: 1, start_offset: 0}

            [
              {:ok, %ExecutePlanResponse{response_type: {:arrow_batch, batch}}},
              {:ok,
               %ExecutePlanResponse{
                 response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
               }}
            ]
        end

      {:reply, {:ok, stream}, test_pid}
    end

    @impl true
    def handle_call({:execute_plan_reattachable_stream, plan, opts}, from, test_pid) do
      handle_call({:execute_plan_stream, plan, opts}, from, test_pid)
    end
  end

  def build_test_ipc_data do
    if Code.ensure_loaded?(Explorer.DataFrame) do
      df = Explorer.DataFrame.new(%{"id" => [1]})

      case Explorer.DataFrame.dump_ipc_stream(df) do
        {:ok, data} -> data
        _ -> <<>>
      end
    else
      <<>>
    end
  end

  describe "struct" do
    test "holds session and plan" do
      df = DataFrame.new(self(), {:sql, "SELECT 1", nil})
      assert df.session == self()
      assert SparkEx.Test.PlanHelpers.unwrap(df.plan) == {:sql, "SELECT 1", nil}
    end
  end

  describe "SparkEx.sql/3" do
    test "creates a DataFrame with SQL plan" do
      df = SparkEx.sql(self(), "SELECT 1")
      assert {:sql, "SELECT 1", nil} = SparkEx.Test.PlanHelpers.unwrap_plan(df)
    end

    test "creates a DataFrame with SQL and named args" do
      df = SparkEx.sql(self(), "SELECT :id", args: %{id: 1})
      assert {:sql, "SELECT :id", %{id: 1}} = SparkEx.Test.PlanHelpers.unwrap_plan(df)
    end

    test "creates a DataFrame with SQL and positional args" do
      df = SparkEx.sql(self(), "SELECT ?", args: [42])
      assert {:sql, "SELECT ?", [42]} = SparkEx.Test.PlanHelpers.unwrap_plan(df)
    end

    test "raises for invalid SQL args type" do
      assert_raise ArgumentError, ~r/expected :args to be a list, map, or nil/, fn ->
        SparkEx.sql(self(), "SELECT ?", args: MapSet.new([1, 2, 3]))
      end
    end

    test "raises for non-string query" do
      assert_raise ArgumentError, ~r/expected query to be a string/, fn ->
        SparkEx.sql(self(), 42)
      end
    end

    test "raises for nil query" do
      assert_raise ArgumentError, ~r/expected query to be a string/, fn ->
        SparkEx.sql(self(), nil)
      end
    end
  end

  defmodule SchemaSession do
    use GenServer

    def start_link(schema) do
      GenServer.start_link(__MODULE__, schema, [])
    end

    @impl true
    def init(schema), do: {:ok, schema}

    @impl true
    def handle_call({:analyze_schema, _plan}, _from, schema) do
      {:reply, {:ok, schema}, schema}
    end
  end

  defmodule CollectAsMapSession do
    use GenServer

    def start_link(schema, rows) do
      GenServer.start_link(__MODULE__, %{schema: schema, rows: rows}, [])
    end

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call({:analyze_schema, _plan}, _from, state) do
      {:reply, {:ok, state.schema}, state}
    end

    @impl true
    def handle_call({:execute_collect, _plan, _opts}, _from, state) do
      {:reply, {:ok, state.rows}, state}
    end
  end

  defmodule ObserveSession do
    use GenServer

    def start_link(is_streaming) do
      GenServer.start_link(__MODULE__, is_streaming, [])
    end

    @impl true
    def init(is_streaming), do: {:ok, is_streaming}

    @impl true
    def handle_call({:analyze_is_streaming, _plan}, _from, is_streaming) do
      {:reply, {:ok, is_streaming}, is_streaming}
    end

    @impl true
    def handle_call(:get_state, _from, is_streaming) do
      {:reply, :mock_session, is_streaming}
    end
  end

  describe "columns/1" do
    test "returns column names from schema" do
      schema = %Spark.Connect.DataType.Struct{
        fields: [
          %Spark.Connect.DataType.StructField{
            name: "id",
            data_type: %Spark.Connect.DataType{kind: {:long, %Spark.Connect.DataType.Long{}}},
            nullable: true
          }
        ]
      }

      {:ok, session} = SchemaSession.start_link(schema)
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert {:ok, ["id"]} = DataFrame.columns(df)
    end
  end

  describe "dtypes/1" do
    test "returns dtype tuples from schema" do
      schema = %Spark.Connect.DataType.Struct{
        fields: [
          %Spark.Connect.DataType.StructField{
            name: "id",
            data_type: %Spark.Connect.DataType{kind: {:long, %Spark.Connect.DataType.Long{}}},
            nullable: true
          }
        ]
      }

      {:ok, session} = SchemaSession.start_link(schema)
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert {:ok, [{"id", "BIGINT"}]} = DataFrame.dtypes(df)
    end

    test "accepts {:ok, dataframe} for create_dataframe-style pipelines" do
      schema = %Spark.Connect.DataType.Struct{
        fields: [
          %Spark.Connect.DataType.StructField{
            name: "id",
            data_type: %Spark.Connect.DataType{kind: {:long, %Spark.Connect.DataType.Long{}}},
            nullable: true
          }
        ]
      }

      {:ok, session} = SchemaSession.start_link(schema)
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert {:ok, [{"id", "BIGINT"}]} = DataFrame.dtypes({:ok, df})
    end
  end

  describe "collect_as_map/1" do
    test "collects 2-column dataframe into map" do
      schema = %Spark.Connect.DataType.Struct{
        fields: [
          %Spark.Connect.DataType.StructField{
            name: "key",
            data_type: %Spark.Connect.DataType{kind: {:string, %Spark.Connect.DataType.String{}}},
            nullable: false
          },
          %Spark.Connect.DataType.StructField{
            name: "value",
            data_type: %Spark.Connect.DataType{
              kind: {:integer, %Spark.Connect.DataType.Integer{}}
            },
            nullable: true
          }
        ]
      }

      rows = [%{"key" => "a", "value" => 1}, %{"key" => "b", "value" => 2}]

      {:ok, session} = CollectAsMapSession.start_link(schema, rows)
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert {:ok, %{"a" => 1, "b" => 2}} = DataFrame.collect_as_map(df)
    end

    test "returns error when dataframe does not have exactly two columns" do
      schema = %Spark.Connect.DataType.Struct{
        fields: [
          %Spark.Connect.DataType.StructField{
            name: "a",
            data_type: %Spark.Connect.DataType{
              kind: {:integer, %Spark.Connect.DataType.Integer{}}
            },
            nullable: true
          }
        ]
      }

      {:ok, session} = CollectAsMapSession.start_link(schema, [%{"a" => 1}])
      df = DataFrame.new(session, {:sql, "SELECT 1", nil})

      assert {:error, :collect_as_map_requires_two_columns} = DataFrame.collect_as_map(df)
    end
  end

  describe "SparkEx.range/3" do
    test "creates a range DataFrame with defaults" do
      df = SparkEx.range(self(), 10)
      assert {:range, 0, 10, 1, nil} = SparkEx.Test.PlanHelpers.unwrap_plan(df)
    end

    test "creates a range DataFrame with options" do
      df = SparkEx.range(self(), 100, start: 10, step: 5, num_partitions: 4)
      assert {:range, 10, 100, 5, 4} = SparkEx.Test.PlanHelpers.unwrap_plan(df)
    end

    test "supports start/end signature" do
      df = SparkEx.range(self(), 10, 20)
      assert {:range, 10, 20, 1, nil} = SparkEx.Test.PlanHelpers.unwrap_plan(df)
    end

    test "supports start/end/step signature with opts" do
      df = SparkEx.range(self(), 10, 20, 2, num_partitions: 3)
      assert {:range, 10, 20, 2, 3} = SparkEx.Test.PlanHelpers.unwrap_plan(df)
    end
  end

  describe "SparkEx.udf/1" do
    test "returns UDF registration accessor" do
      assert SparkEx.udf(self()) == SparkEx.UDFRegistration
    end
  end

  describe "SparkEx.udtf/1" do
    test "returns UDTF registration accessor" do
      assert SparkEx.udtf(self()) == SparkEx.UDFRegistration
    end
  end

  describe "SparkEx.data_source/1" do
    test "returns data source registration accessor" do
      assert SparkEx.data_source(self()) == SparkEx.UDFRegistration
    end
  end

  describe "select/2" do
    test "creates project plan from Column structs" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.select(df, [Functions.col("a"), Functions.col("b")])

      assert {:project, {:sql, "SELECT * FROM t", nil}, [{:col, "a"}, {:col, "b"}]} =
               unwrap_plan(result)
    end

    test "creates project plan from string column names" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.select(df, ["a", "b"])

      assert {:project, {:sql, "SELECT * FROM t", nil}, [{:col, "a"}, {:col, "b"}]} =
               unwrap_plan(result)
    end

    test "creates project plan from atom column names" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.select(df, [:a, :b])

      assert {:project, {:sql, "SELECT * FROM t", nil}, [{:col, "a"}, {:col, "b"}]} =
               unwrap_plan(result)
    end
  end

  describe "col_regex/2" do
    test "returns Column with col_regex expression" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.col_regex(df, "^name_.*")

      assert %SparkEx.Column{} = result
      assert {:col_regex, "^name_.*", {:plan_id, _, {:sql, "SELECT * FROM t", nil}}} = result.expr
    end

    test "collect on col_regex result raises clear error" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      col = DataFrame.col_regex(df, "^name_.*")

      assert_raise ArgumentError, ~r/wrap it in DataFrame.select\/2 first/, fn ->
        DataFrame.collect(col)
      end
    end
  end

  describe "metadata_column/2" do
    test "returns Column with metadata_col expression" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.metadata_column(df, "_metadata")

      assert %SparkEx.Column{} = result

      assert {:metadata_col, "_metadata", {:plan_id, _, {:sql, "SELECT * FROM t", nil}}} =
               result.expr
    end
  end

  describe "col/2" do
    test "returns Column with DataFrame-scoped col expression" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.col(df, "id")

      assert %SparkEx.Column{} = result
      assert {:col, "id", {:plan_id, _, {:sql, "SELECT * FROM t", nil}}} = result.expr
    end
  end

  describe "rollup/2" do
    test "creates grouped data with rollup group type" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.rollup(df, ["id"])

      assert %SparkEx.GroupedData{group_type: :rollup, grouping_exprs: [{:col, "id"}]} = grouped
    end
  end

  describe "cube/2" do
    test "creates grouped data with cube group type" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.cube(df, ["id"])

      assert %SparkEx.GroupedData{group_type: :cube, grouping_exprs: [{:col, "id"}]} = grouped
    end
  end

  describe "grouping_sets/2" do
    test "creates grouped data with grouping sets" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.grouping_sets(df, [["id"], ["dept"]])

      assert %SparkEx.GroupedData{
               group_type: :grouping_sets,
               grouping_sets: [[{:col, "id"}], [{:col, "dept"}]]
             } = grouped
    end
  end

  describe "groupby/2" do
    test "aliases group_by" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      grouped = DataFrame.groupby(df, ["dept"])

      assert %SparkEx.GroupedData{group_type: :groupby, grouping_exprs: [{:col, "dept"}]} =
               grouped
    end
  end

  describe "filter/2" do
    test "creates filter plan from Column condition" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      condition = Column.gt(Functions.col("age"), Functions.lit(18))
      result = DataFrame.filter(df, condition)

      assert {:filter, {:sql, _, _}, {:fn, ">", [{:col, "age"}, {:lit, 18}], false}} =
               unwrap_plan(result)
    end
  end

  describe "with_column/3" do
    test "creates with_columns plan" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      expr = Column.plus(Functions.col("a"), Functions.lit(1))
      result = DataFrame.with_column(df, "a_plus_1", expr)

      assert {:with_columns, {:sql, _, _},
              [{:alias, {:fn, "+", [{:col, "a"}, {:lit, 1}], false}, "a_plus_1"}]} =
               unwrap_plan(result)
    end
  end

  describe "with_columns/2" do
    test "accepts map of column values" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.with_columns(df, %{"a" => 1, "b" => Functions.col("b")})

      assert {:with_columns, {:sql, _, _}, aliases} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
      assert {:alias, {:lit, 1}, "a"} in aliases
      assert {:alias, {:col, "b"}, "b"} in aliases
    end
  end

  describe "drop/2" do
    test "creates drop plan from string names" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.drop(df, ["temp", "debug"])

      assert {:drop, {:sql, _, _}, ["temp", "debug"], []} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "creates drop plan from atom names" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.drop(df, [:temp, :debug])

      assert {:drop, {:sql, _, _}, ["temp", "debug"], []} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  describe "order_by/2" do
    test "creates sort plan from Column with sort order" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.order_by(df, [Column.desc(Functions.col("age"))])

      assert {:sort, {:sql, _, _}, [{:sort_order, {:col, "age"}, :desc, :nulls_last}]} =
               unwrap_plan(result)
    end

    test "creates sort plan from string names (ascending default)" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.order_by(df, ["name"])

      assert {:sort, {:sql, _, _}, [{:sort_order, {:col, "name"}, :asc, :nulls_first}]} =
               unwrap_plan(result)
    end

    test "wraps bare Column in ascending sort order" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.order_by(df, [Functions.col("name")])

      assert {:sort, {:sql, _, _}, [{:sort_order, {:col, "name"}, :asc, :nulls_first}]} =
               unwrap_plan(result)
    end

    test "raises when ascending list includes non-boolean values" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/ascending list values must be booleans/, fn ->
        DataFrame.order_by(df, ["name"], ascending: ["oops"])
      end
    end
  end

  describe "sort/2" do
    test "aliases order_by" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.sort(df, ["name"])

      assert {:sort, {:sql, _, _}, [{:sort_order, {:col, "name"}, :asc, :nulls_first}]} =
               unwrap_plan(result)
    end
  end

  describe "observe/3" do
    test "creates collect_metrics plan" do
      {:ok, session} = ObserveSession.start_link(false)
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})
      obs = SparkEx.Observation.new("obs")

      result = DataFrame.observe(df, obs, [Functions.col("value")])

      assert {:collect_metrics, {:sql, _, _}, "obs", [{:col, "value"}]} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "raises for streaming dataframe" do
      {:ok, session} = ObserveSession.start_link(true)
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/Streaming DataFrame with Observation is not supported/, fn ->
        DataFrame.observe(df, "obs", [Functions.col("value")])
      end
    end

    test "registers metric aliases for positional observed metric payloads" do
      {:ok, session} = ObserveSession.start_link(false)
      obs = SparkEx.Observation.new("obs_#{System.unique_integer([:positive])}")
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      _observed =
        DataFrame.observe(df, obs, [
          Functions.count(Functions.col("value")) |> Column.alias_("cnt"),
          Functions.sum(Functions.col("value")) |> Column.alias_("total")
        ])

      :ok =
        SparkEx.Observation.store_observed_metrics(%{
          obs.name => %{"_1" => 5, "_2" => 15}
        })

      assert SparkEx.Observation.get(obs) == %{"cnt" => 5, "total" => 15}
    end
  end

  describe "transpose/2" do
    test "accepts list index_column option" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.transpose(df, index_column: ["id"])

      assert {:transpose, {:sql, _, _}, [{:col, "id"}]} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  describe "to_local_iterator/2" do
    test "returns lazy rows enumerable backed by ExecutePlan stream" do
      {:ok, session} = ArrowSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, rows} = DataFrame.to_local_iterator(df)
      assert Enum.to_list(rows) == [{:ok, %{"id" => 1}}]
      assert_receive :execute_plan_stream_called
      refute_receive :execute_collect_called
    end
  end

  describe "to_arrow/2" do
    test "delegates to session" do
      {:ok, session} = ArrowSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, :arrow_data} = DataFrame.to_arrow(df)
    end
  end

  describe "foreach_local/3" do
    test "applies function over collected rows" do
      {:ok, session} = ArrowSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert :ok = DataFrame.foreach_local(df, fn _ -> :ok end)
    end
  end

  describe "foreach_partition_local/3" do
    test "applies function over rows as a single partition" do
      {:ok, session} = ArrowSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert :ok = DataFrame.foreach_partition_local(df, fn _ -> :ok end)
    end
  end

  describe "limit/2" do
    test "creates limit plan" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.limit(df, 10)
      assert {:limit, {:sql, _, _}, 10} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "allows limit(0)" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.limit(df, 0)
      assert {:limit, {:sql, _, _}, 0} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  describe "repartition_by_range/2" do
    test "creates repartition_by_expression plan with sort orders" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.repartition_by_range(df, ["id"])

      assert {:repartition_by_expression, {:sql, _, _},
              [{:sort_order, {:col, "id"}, :asc, :nulls_first}], nil} = unwrap_plan(result)
    end
  end

  describe "repartition_by_range/3" do
    test "creates repartition_by_expression plan with num partitions" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.repartition_by_range(df, 10, [Functions.col("id")])

      assert {:repartition_by_expression, {:sql, _, _},
              [{:sort_order, {:col, "id"}, :asc, :nulls_first}], 10} = unwrap_plan(result)
    end

    test "raises when num_partitions is zero" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/num_partitions must be a positive integer/, fn ->
        DataFrame.repartition_by_range(df, 0, [Functions.col("id")])
      end
    end

    test "raises when num_partitions is negative" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/num_partitions must be a positive integer/, fn ->
        DataFrame.repartition_by_range(df, -1, [])
      end
    end

    test "raises when cols is empty" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/cols should not be empty/, fn ->
        DataFrame.repartition_by_range(df, 4, [])
      end
    end
  end

  describe "join/4" do
    test "creates inner join plan with Column condition" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      condition = Column.eq(Functions.col("a.id"), Functions.col("b.id"))
      result = DataFrame.join(df1, df2, condition)

      assert {:join, {:sql, "SELECT * FROM a", _}, {:sql, "SELECT * FROM b", _},
              {:fn, "==", [{:col, "a.id"}, {:col, "b.id"}], false}, :inner, []} =
               unwrap_plan(result)
    end

    test "creates left join plan" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      condition = Column.eq(Functions.col("id"), Functions.col("id"))
      result = DataFrame.join(df1, df2, condition, :left)

      assert {:join, _, _, _, :left, []} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "creates join plan with using columns" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.join(df1, df2, ["id", "name"], :inner)

      assert {:join, _, _, nil, :inner, ["id", "name"]} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "creates join plan with single using column name" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.join(df1, df2, "id", :inner)

      assert {:join, _, _, nil, :inner, ["id"]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "creates join plan with list of Column conditions" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})

      result =
        DataFrame.join(
          df1,
          df2,
          [
            Column.eq(Functions.col("a.id"), Functions.col("b.id")),
            Column.eq(Functions.col("a.dept"), Functions.col("b.dept"))
          ],
          :inner
        )

      assert {:join, _, _,
              {:fn, "and",
               [
                 {:fn, "==", [{:col, "a.id"}, {:col, "b.id"}], false},
                 {:fn, "==", [{:col, "a.dept"}, {:col, "b.dept"}], false}
               ], false}, :inner, []} = unwrap_plan(result)
    end

    test "creates cross join" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.join(df1, df2, [], :cross)

      assert {:join, _, _, nil, :cross, []} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "normalizes join type aliases" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})

      assert {:join, _, _, nil, :full, _} = unwrap_plan(DataFrame.join(df1, df2, ["id"], :outer))

      assert {:join, _, _, nil, :left, _} =
               unwrap_plan(DataFrame.join(df1, df2, ["id"], :left_outer))

      assert {:join, _, _, nil, :right, _} =
               unwrap_plan(DataFrame.join(df1, df2, ["id"], :rightouter))

      assert {:join, _, _, nil, :left_semi, _} =
               unwrap_plan(DataFrame.join(df1, df2, ["id"], :semi))

      assert {:join, _, _, nil, :left_anti, _} =
               unwrap_plan(DataFrame.join(df1, df2, ["id"], "left_anti"))
    end

    test "raises on unsupported join type" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})

      assert_raise ArgumentError, ~r/invalid join type/, fn ->
        DataFrame.join(df1, df2, ["id"], :sideways)
      end
    end

    test "raises on invalid join key type" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})

      assert_raise ArgumentError, ~r/expected join keys/, fn ->
        DataFrame.join(df1, df2, %{id: 1}, :inner)
      end
    end

    test "raises when joining DataFrames from different sessions" do
      df1 = DataFrame.new(:session_a, {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(:session_b, {:sql, "SELECT * FROM b", nil})

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.join(df1, df2, ["id"], :inner)
      end
    end
  end

  describe "as_of_join/5" do
    test "creates as-of join plan" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM t1", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM t2", nil})

      result =
        DataFrame.as_of_join(
          df1,
          df2,
          Functions.col("t1"),
          Functions.col("t2"),
          on: Functions.col("id"),
          tolerance: Functions.lit(5),
          allow_exact_matches: false,
          direction: "forward"
        )

      assert {:as_of_join, {:sql, _, _}, {:sql, _, _}, {:col, "t1"}, {:col, "t2"}, {:col, "id"},
              [], "inner", {:lit, 5}, false, "forward"} = unwrap_plan(result)
    end

    test "raises on invalid allow_exact_matches option type" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM t1", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM t2", nil})

      assert_raise ArgumentError, ~r/allow_exact_matches/, fn ->
        DataFrame.as_of_join(df1, df2, Functions.col("t1"), Functions.col("t2"),
          allow_exact_matches: "oops"
        )
      end
    end

    test "raises on invalid direction option value" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM t1", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM t2", nil})

      assert_raise ArgumentError, ~r/invalid as_of_join :direction/, fn ->
        DataFrame.as_of_join(df1, df2, Functions.col("t1"), Functions.col("t2"),
          direction: "sideways"
        )
      end
    end

    test "raises on invalid join_type option value" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM t1", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM t2", nil})

      assert_raise ArgumentError, ~r/invalid join type/, fn ->
        DataFrame.as_of_join(df1, df2, Functions.col("t1"), Functions.col("t2"), join_type: 123)
      end
    end

    test "string as-of column names bind to left/right plans" do
      left_plan = {:sql, "SELECT * FROM t1", nil}
      right_plan = {:sql, "SELECT * FROM t2", nil}
      df1 = DataFrame.new(self(), left_plan)
      df2 = DataFrame.new(self(), right_plan)

      result = DataFrame.as_of_join(df1, df2, "ts", "ts")

      assert %DataFrame{} = result

      assert {:plan_id, _,
              {:as_of_join, {:plan_id, _, ^left_plan}, {:plan_id, _, ^right_plan},
               {:col, "ts", {:plan_id, _, ^left_plan}}, {:col, "ts", {:plan_id, _, ^right_plan}},
               _, _, _, _, _, _}} = result.plan
    end
  end

  describe "lateral_join/4" do
    test "creates lateral join plan" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM t1", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM t2", nil})
      condition = Column.eq(Functions.col("t1.id"), Functions.col("t2.id"))

      result = DataFrame.lateral_join(df1, df2, condition, :left)

      assert {:lateral_join, {:sql, _, _}, {:sql, _, _}, _, :left} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "uses regular join plan when rhs is a TVF relation" do
      left = DataFrame.new(self(), {:sql, "SELECT * FROM t1", nil})
      right = TableValuedFunction.new(self()) |> TableValuedFunction.call("range", [1, 3])

      result = DataFrame.lateral_join(left, right, nil, :inner)

      assert {:join, {:sql, _, _}, {:table_valued_function, "range", _}, nil, :inner, []} =
               unwrap_plan(result)
    end
  end

  describe "in_subquery/2" do
    test "creates in subquery expression" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.in_subquery(df, [Functions.col("id")])

      assert %SparkEx.Column{} = result

      assert {:subquery, :in, {:plan_id, _, {:sql, _, _}}, [in_values: [{:col, "id"}]]} =
               result.expr
    end

    test "accepts column plus subquery dataframe signature" do
      subquery = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.in_subquery(Functions.col("id"), subquery)

      assert %SparkEx.Column{} = result

      assert {:subquery, :in, {:plan_id, _, {:sql, _, _}}, [in_values: [{:col, "id"}]]} =
               result.expr
    end
  end

  describe "distinct/1" do
    test "creates deduplicate plan" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.distinct(df)

      assert {:deduplicate, {:sql, _, _}, [], true} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  describe "drop_duplicates/2" do
    test "accepts column names and atoms" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.drop_duplicates(df, ["id", :dept])

      assert {:deduplicate, {:sql, _, _}, ["id", "dept"], false} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "accepts column struct with col expr" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.drop_duplicates(df, [Functions.col("id")])

      assert {:deduplicate, {:sql, _, _}, ["id"], false} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "raises for non-column expressions" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/expected column names/, fn ->
        DataFrame.drop_duplicates(df, [Column.desc(Functions.col("id"))])
      end
    end
  end

  describe "hint/3" do
    test "accepts list of columns and primitives" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      params = [Functions.col("id"), "broadcast"]
      result = DataFrame.hint(df, "merge", params)

      assert {:hint, {:sql, _, _}, "merge", [_, _]} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "rejects unsupported hint parameters" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/invalid hint parameter/, fn ->
        DataFrame.hint(df, "merge", [Date.utc_today()])
      end
    end
  end

  describe "transform/2" do
    test "applies transform function" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.transform(df, &DataFrame.limit(&1, 5))

      assert {:limit, {:sql, _, _}, 5} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "raises when transform returns non-DataFrame" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert_raise ArgumentError, ~r/expected transform function to return DataFrame/, fn ->
        DataFrame.transform(df, fn _ -> :ok end)
      end
    end
  end

  describe "union/2" do
    test "creates union all plan" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.union(df1, df2)

      assert {:set_operation, {:sql, "SELECT * FROM a", _}, {:sql, "SELECT * FROM b", _}, :union,
              true} = unwrap_plan(result)
    end

    test "raises when unioning DataFrames from different sessions" do
      df1 = DataFrame.new(:session_a, {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(:session_b, {:sql, "SELECT * FROM b", nil})

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.union(df1, df2)
      end
    end
  end

  describe "unionAll/2" do
    test "aliases union" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.unionAll(df1, df2)

      assert {:set_operation, _, _, :union, true} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  describe "subtract/2" do
    test "aliases except distinct" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.subtract(df1, df2)

      assert {:set_operation, _, _, :except, false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  describe "registerTempTable/3" do
    defmodule TempViewSession do
      use GenServer

      def start_link() do
        GenServer.start_link(__MODULE__, :ok, [])
      end

      @impl true
      def init(:ok), do: {:ok, :ok}

      @impl true
      def handle_call(
            {:execute_command, {:create_dataframe_view, _plan, name, false, true}, _opts},
            _from,
            state
          ) do
        {:reply, {:ok, name}, state}
      end
    end

    test "delegates to create_or_replace_temp_view" do
      {:ok, session} = TempViewSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, "tmp"} = DataFrame.registerTempTable(df, "tmp")
    end
  end

  describe "register_temp_table/3" do
    defmodule TempViewSnakeSession do
      use GenServer

      def start_link() do
        GenServer.start_link(__MODULE__, :ok, [])
      end

      @impl true
      def init(:ok), do: {:ok, :ok}

      @impl true
      def handle_call(
            {:execute_command, {:create_dataframe_view, _plan, name, false, true}, _opts},
            _from,
            state
          ) do
        {:reply, {:ok, name}, state}
      end
    end

    test "delegates to create_or_replace_temp_view" do
      {:ok, session} = TempViewSnakeSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, "tmp"} = DataFrame.register_temp_table(df, "tmp")
    end
  end

  describe "checkpoint/2" do
    defmodule CheckpointSession do
      use GenServer

      def start_link(test_pid) do
        GenServer.start_link(__MODULE__, test_pid, [])
      end

      @impl true
      def init(test_pid), do: {:ok, test_pid}

      @impl true
      def handle_call(
            {:execute_command_with_result, {:checkpoint, _plan, local, eager, storage_level},
             _opts},
            _from,
            test_pid
          ) do
        send(test_pid, {:checkpoint_args, local, eager, storage_level})

        result =
          %Spark.Connect.CheckpointCommandResult{
            relation: %Spark.Connect.CachedRemoteRelation{relation_id: "rel-1"}
          }

        {:reply, {:ok, {:checkpoint, result}}, test_pid}
      end
    end

    test "returns cached remote relation" do
      {:ok, session} = CheckpointSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:cached_remote_relation, "rel-1"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(DataFrame.checkpoint(df))

      assert_receive {:checkpoint_args, false, true, nil}
    end

    test "checkpoint/1 aliases checkpoint/2" do
      {:ok, session} = CheckpointSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:cached_remote_relation, "rel-1"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(DataFrame.checkpoint(df))

      assert_receive {:checkpoint_args, false, true, nil}
    end

    defmodule UnsupportedCheckpointSession do
      use GenServer

      def start_link(test_pid) do
        GenServer.start_link(__MODULE__, test_pid, [])
      end

      @impl true
      def init(test_pid), do: {:ok, test_pid}

      @impl true
      def handle_call(
            {:execute_command_with_result, {:checkpoint, _plan, _local, _eager, _storage_level},
             _opts},
            _from,
            test_pid
          ) do
        error = %SparkEx.Error.Remote{message: "checkpoint command not supported."}
        {:reply, {:error, error}, test_pid}
      end
    end

    test "checkpoint/1 surfaces server error when checkpoint command is unsupported" do
      {:ok, session} = UnsupportedCheckpointSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:error, %SparkEx.Error.Remote{message: "checkpoint command not supported."}} =
               DataFrame.checkpoint(df)
    end
  end

  describe "local_checkpoint/2" do
    defmodule LocalCheckpointSession do
      use GenServer

      def start_link(test_pid) do
        GenServer.start_link(__MODULE__, test_pid, [])
      end

      @impl true
      def init(test_pid), do: {:ok, test_pid}

      @impl true
      def handle_call(
            {:execute_command_with_result, {:checkpoint, _plan, local, eager, storage_level},
             _opts},
            _from,
            test_pid
          ) do
        send(test_pid, {:local_checkpoint_args, local, eager, storage_level})

        result =
          %Spark.Connect.CheckpointCommandResult{
            relation: %Spark.Connect.CachedRemoteRelation{relation_id: "rel-2"}
          }

        {:reply, {:ok, {:checkpoint, result}}, test_pid}
      end
    end

    test "uses local flag and storage level" do
      {:ok, session} = LocalCheckpointSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})
      storage_level = %Spark.Connect.StorageLevel{use_memory: true}

      assert {:cached_remote_relation, "rel-2"} =
               unwrap_plan(
                 DataFrame.local_checkpoint(df, eager: true, storage_level: storage_level)
               )

      assert_receive {:local_checkpoint_args, true, true, ^storage_level}
    end

    test "localCheckpoint/1 aliases local_checkpoint/2" do
      {:ok, session} = LocalCheckpointSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:cached_remote_relation, "rel-2"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(DataFrame.localCheckpoint(df))

      assert_receive {:local_checkpoint_args, true, true, nil}
    end

    test "local_checkpoint/1 aliases local_checkpoint/2" do
      {:ok, session} = LocalCheckpointSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:cached_remote_relation, "rel-2"} =
               SparkEx.Test.PlanHelpers.unwrap_plan(DataFrame.local_checkpoint(df))

      assert_receive {:local_checkpoint_args, true, true, nil}
    end

    test "local_checkpoint/1 surfaces server error when checkpoint command is unsupported" do
      {:ok, session} = __MODULE__.UnsupportedCheckpointSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:error, %SparkEx.Error.Remote{message: "checkpoint command not supported."}} =
               DataFrame.local_checkpoint(df)
    end
  end

  describe "global temp view API" do
    test "drop_global_temp_view/2 is exported" do
      assert function_exported?(DataFrame, :drop_global_temp_view, 2)
    end
  end

  describe "to/2" do
    test "wraps plan in to_schema" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})
      result = DataFrame.to(df, "id LONG")

      assert {:to_schema, {:sql, _, _}, "id LONG"} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end
  end

  describe "is_cached/1" do
    defmodule StorageLevelSession do
      use GenServer

      def start_link(storage_level) do
        GenServer.start_link(__MODULE__, storage_level, [])
      end

      @impl true
      def init(storage_level), do: {:ok, storage_level}

      @impl true
      def handle_call({:analyze_get_storage_level, _plan}, _from, storage_level) do
        {:reply, {:ok, storage_level}, storage_level}
      end
    end

    test "returns true when storage level is active" do
      {:ok, session} =
        StorageLevelSession.start_link(%Spark.Connect.StorageLevel{use_memory: true})

      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, true} = DataFrame.is_cached(df)
    end

    test "returns false when storage level is NONE" do
      {:ok, session} =
        StorageLevelSession.start_link(%Spark.Connect.StorageLevel{
          use_disk: false,
          use_memory: false,
          use_off_heap: false,
          replication: 0
        })

      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, false} = DataFrame.is_cached(df)
    end

    test "is_cached?/1 aliases is_cached" do
      {:ok, session} =
        StorageLevelSession.start_link(%Spark.Connect.StorageLevel{use_memory: true})

      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, true} = DataFrame.is_cached?(df)
    end
  end

  describe "cache/1" do
    defmodule PersistSession do
      use GenServer

      def start_link() do
        GenServer.start_link(__MODULE__, :ok, [])
      end

      @impl true
      def init(:ok), do: {:ok, :ok}

      @impl true
      def handle_call({:analyze_persist, _plan, _opts}, _from, state) do
        {:reply, :ok, state}
      end

      @impl true
      def handle_call({:analyze_unpersist, _plan, _opts}, _from, state) do
        {:reply, :ok, state}
      end
    end

    test "delegates to persist with defaults" do
      {:ok, session} = PersistSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert ^df = DataFrame.cache(df)
    end

    test "persist returns dataframe for chaining" do
      {:ok, session} = PersistSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert ^df = DataFrame.persist(df)
    end

    test "unpersist returns dataframe for chaining" do
      {:ok, session} = PersistSession.start_link()
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert ^df = DataFrame.unpersist(df)
    end
  end

  describe "option validation errors" do
    test "show/html/tree_string reject invalid option types without session crash" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert {:error, msg} = DataFrame.show(df, num_rows: "oops")
      assert msg =~ "num_rows"

      assert {:error, msg} = DataFrame.html_string(df, truncate: "oops")
      assert msg =~ "truncate"

      assert {:error, msg} = DataFrame.tree_string(df, level: "oops")
      assert msg =~ "level"
    end

    test "checkpoint and local_checkpoint validate eager/storage_level options" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert {:error, msg} = DataFrame.checkpoint(df, eager: "oops")
      assert msg =~ "eager"

      assert {:error, msg} = DataFrame.local_checkpoint(df, eager: "oops")
      assert msg =~ "eager"

      assert {:error, msg} = DataFrame.local_checkpoint(df, storage_level: "oops")
      assert msg =~ "storage_level"
    end

    test "persist/unpersist validate option types" do
      df = DataFrame.new(self(), {:sql, "SELECT * FROM t", nil})

      assert {:error, msg} = DataFrame.persist(df, storage_level: "oops")
      assert msg =~ "storage_level"

      assert {:error, msg} = DataFrame.unpersist(df, blocking: "oops")
      assert msg =~ "blocking"
    end
  end

  describe "take/3" do
    defmodule TakeSession do
      use GenServer

      def start_link(test_pid) do
        GenServer.start_link(__MODULE__, test_pid, [])
      end

      @impl true
      def init(test_pid), do: {:ok, test_pid}

      @impl true
      def handle_call({:execute_collect, {:limit, _plan, n}, _opts}, _from, test_pid) do
        send(test_pid, {:take_limit, n})
        rows = if n > 0, do: [%{"id" => 1}], else: []
        {:reply, {:ok, rows}, test_pid}
      end
    end

    test "allows take(0)" do
      {:ok, session} = TakeSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, []} = DataFrame.take(df, 0)
      assert_receive {:take_limit, 0}
    end

    test "head/1 returns a single row or nil semantics" do
      {:ok, session} = TakeSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, %{"id" => 1}} = DataFrame.head(df)
      assert_receive {:take_limit, 1}
    end

    test "head/2 accepts zero and returns list semantics" do
      {:ok, session} = TakeSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, []} = DataFrame.head(df, 0)
      assert_receive {:take_limit, 0}

      assert {:ok, [%{"id" => 1}]} = DataFrame.head(df, 1)
      assert_receive {:take_limit, 1}
    end
  end

  describe "is_empty?/1 and is_streaming?/1 aliases" do
    test "is_empty?/1 delegates to is_empty/1" do
      {:ok, session} = SparkEx.DataFrameTest.TakeSession.start_link(self())
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, false} = DataFrame.is_empty?(df)
      assert_receive {:take_limit, 1}
    end

    test "is_streaming?/1 delegates to is_streaming/1" do
      {:ok, session} = ObserveSession.start_link(true)
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, true} = DataFrame.is_streaming?(df)
    end
  end

  describe "execution_info/1" do
    defmodule ExecutionMetricsSession do
      use GenServer

      def start_link(metrics) do
        GenServer.start_link(__MODULE__, metrics, [])
      end

      @impl true
      def init(metrics), do: {:ok, metrics}

      @impl true
      def handle_call(:last_execution_metrics, _from, metrics) do
        {:reply, {:ok, metrics}, metrics}
      end
    end

    test "returns last execution metrics" do
      {:ok, session} = ExecutionMetricsSession.start_link(%{"stage" => 1})
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, %{"stage" => 1}} = DataFrame.execution_info(df)
    end

    test "executionInfo/1 aliases execution_info" do
      {:ok, session} = ExecutionMetricsSession.start_link(%{"stage" => 1})
      df = DataFrame.new(session, {:sql, "SELECT * FROM t", nil})

      assert {:ok, %{"stage" => 1}} = DataFrame.executionInfo(df)
    end
  end

  describe "spark_session/1" do
    test "returns the stored session" do
      df = DataFrame.new(:session_pid, {:sql, "SELECT 1", nil})
      assert DataFrame.spark_session(df) == :session_pid
    end

    test "sparkSession/1 aliases spark_session" do
      df = DataFrame.new(:session_pid, {:sql, "SELECT 1", nil})
      assert DataFrame.sparkSession(df) == :session_pid
    end
  end

  describe "union_distinct/2" do
    test "creates union distinct plan" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.union_distinct(df1, df2)

      assert {:set_operation, _, _, :union, false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "raises when union_distinct DataFrames are from different sessions" do
      df1 = DataFrame.new(:session_a, {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(:session_b, {:sql, "SELECT * FROM b", nil})

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.union_distinct(df1, df2)
      end
    end
  end

  describe "intersect/2" do
    test "creates intersect plan" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.intersect(df1, df2)

      assert {:set_operation, _, _, :intersect, false} =
               SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "raises when intersecting DataFrames from different sessions" do
      df1 = DataFrame.new(:session_a, {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(:session_b, {:sql, "SELECT * FROM b", nil})

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.intersect(df1, df2)
      end
    end
  end

  describe "except/2" do
    test "creates except plan" do
      df1 = DataFrame.new(self(), {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(self(), {:sql, "SELECT * FROM b", nil})
      result = DataFrame.except(df1, df2)

      assert {:set_operation, _, _, :except, false} = SparkEx.Test.PlanHelpers.unwrap_plan(result)
    end

    test "raises when excepting DataFrames from different sessions" do
      df1 = DataFrame.new(:session_a, {:sql, "SELECT * FROM a", nil})
      df2 = DataFrame.new(:session_b, {:sql, "SELECT * FROM b", nil})

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.except(df1, df2)
      end
    end
  end

  describe "chaining transforms" do
    test "builds up nested plan through pipeline" do
      df =
        DataFrame.new(self(), {:read_data_source, "parquet", ["/p"], nil, %{}})
        |> DataFrame.select(["name", "age"])
        |> DataFrame.filter(Column.gt(Functions.col("age"), Functions.lit(18)))
        |> DataFrame.order_by([Column.desc(Functions.col("age"))])
        |> DataFrame.limit(10)

      # Plan should be: limit -> sort -> filter -> project -> read
      assert {:limit, {:sort, {:filter, {:project, {:read_data_source, _, _, _, _}, _}, _}, _},
              10} = unwrap_plan(df)
    end
  end
end
