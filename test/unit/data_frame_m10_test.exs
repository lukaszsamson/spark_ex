defmodule SparkEx.Unit.DataFrameM10Test do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.Column
  alias SparkEx.Functions

  @base_plan {:sql, "SELECT * FROM t", nil}

  defp df(plan \\ @base_plan), do: %DataFrame{session: self(), plan: plan}

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

  # ── Projection/Rename ──

  describe "select_expr/2" do
    test "wraps SQL strings as expression nodes" do
      result = DataFrame.select_expr(df(), ["name", "age + 1 AS age_plus"])

      assert %DataFrame{
               plan: {:project, @base_plan, [{:expr, "name"}, {:expr, "age + 1 AS age_plus"}]}
             } = result
    end
  end

  describe "with_columns/2" do
    test "accepts list of {name, column} tuples" do
      result =
        DataFrame.with_columns(df(), [
          {"doubled", Column.multiply(Functions.col("x"), Functions.lit(2))}
        ])

      assert %DataFrame{
               plan:
                 {:with_columns, @base_plan,
                  [{:alias, {:fn, "*", [{:col, "x"}, {:lit, 2}], false}, "doubled"}]}
             } = result
    end
  end

  describe "to_df/2" do
    test "creates to_df plan with new column names" do
      result = DataFrame.to_df(df(), ["id", "full_name"])
      assert %DataFrame{plan: {:to_df, @base_plan, ["id", "full_name"]}} = result
    end
  end

  describe "with_column_renamed/3" do
    test "creates with_columns_renamed plan for single rename" do
      result = DataFrame.with_column_renamed(df(), "old", "new")
      assert %DataFrame{plan: {:with_columns_renamed, @base_plan, [{"old", "new"}]}} = result
    end
  end

  describe "with_columns_renamed/2" do
    test "creates with_columns_renamed plan from map" do
      result = DataFrame.with_columns_renamed(df(), %{"a" => "x", "b" => "y"})

      assert %DataFrame{plan: {:with_columns_renamed, @base_plan, renames}} = result
      assert Enum.sort(renames) == [{"a", "x"}, {"b", "y"}]
    end

    test "supports function form using schema round-trip" do
      schema = %Spark.Connect.DataType.Struct{
        fields: [
          %Spark.Connect.DataType.StructField{name: "id"},
          %Spark.Connect.DataType.StructField{name: "name"}
        ]
      }

      {:ok, session} = SchemaSession.start_link(schema)
      source_df = %DataFrame{session: session, plan: @base_plan}

      result = DataFrame.with_columns_renamed(source_df, &"renamed_#{&1}")

      assert %DataFrame{plan: {:with_columns_renamed, @base_plan, renames}} = result
      assert renames == [{"id", "renamed_id"}, {"name", "renamed_name"}]
    end
  end

  # ── Extended Set Operations ──

  describe "union_by_name/3" do
    test "creates set_operation with by_name" do
      df1 = df()
      df2 = df({:sql, "SELECT * FROM b", nil})
      result = DataFrame.union_by_name(df1, df2)

      assert %DataFrame{
               plan:
                 {:set_operation, @base_plan, {:sql, "SELECT * FROM b", nil}, :union, true,
                  [by_name: true, allow_missing_columns: false]}
             } = result
    end

    test "supports allow_missing option" do
      df1 = df()
      df2 = df({:sql, "SELECT * FROM b", nil})
      result = DataFrame.union_by_name(df1, df2, allow_missing: true)

      assert %DataFrame{
               plan:
                 {:set_operation, _, _, :union, true,
                  [by_name: true, allow_missing_columns: true]}
             } = result
    end

    test "raises for different sessions" do
      df1 = %DataFrame{session: :a, plan: @base_plan}
      df2 = %DataFrame{session: :b, plan: @base_plan}

      assert_raise ArgumentError, ~r/different sessions/, fn ->
        DataFrame.union_by_name(df1, df2)
      end
    end
  end

  describe "except_all/2" do
    test "creates set_operation with is_all=true" do
      df1 = df()
      df2 = df({:sql, "SELECT * FROM b", nil})
      result = DataFrame.except_all(df1, df2)

      assert %DataFrame{plan: {:set_operation, _, _, :except, true}} = result
    end
  end

  describe "intersect_all/2" do
    test "creates set_operation with is_all=true" do
      df1 = df()
      df2 = df({:sql, "SELECT * FROM b", nil})
      result = DataFrame.intersect_all(df1, df2)

      assert %DataFrame{plan: {:set_operation, _, _, :intersect, true}} = result
    end
  end

  # ── Partitioning ──

  describe "repartition/3" do
    test "creates repartition plan without columns" do
      result = DataFrame.repartition(df(), 10)
      assert %DataFrame{plan: {:repartition, @base_plan, 10, true}} = result
    end

    test "creates repartition_by_expression plan with columns" do
      result = DataFrame.repartition(df(), 10, [Functions.col("key")])

      assert %DataFrame{plan: {:repartition_by_expression, @base_plan, [{:col, "key"}], 10}} =
               result
    end

    test "accepts string column names" do
      result = DataFrame.repartition(df(), 5, ["a", "b"])

      assert %DataFrame{
               plan: {:repartition_by_expression, @base_plan, [{:col, "a"}, {:col, "b"}], 5}
             } = result
    end
  end

  describe "coalesce/2" do
    test "creates repartition plan with shuffle=false" do
      result = DataFrame.coalesce(df(), 1)
      assert %DataFrame{plan: {:repartition, @base_plan, 1, false}} = result
    end
  end

  describe "sort_within_partitions/2" do
    test "creates sort plan with is_global=false" do
      result = DataFrame.sort_within_partitions(df(), ["name"])

      assert %DataFrame{
               plan:
                 {:sort, @base_plan, [{:sort_order, {:col, "name"}, :asc, :nulls_first}], false}
             } = result
    end

    test "accepts Column with sort order" do
      result =
        DataFrame.sort_within_partitions(df(), [Column.desc(Functions.col("age"))])

      assert %DataFrame{
               plan:
                 {:sort, @base_plan, [{:sort_order, {:col, "age"}, :desc, :nulls_last}], false}
             } = result
    end
  end

  # ── Sampling ──

  describe "sample/3" do
    test "creates sample plan with defaults" do
      result = DataFrame.sample(df(), 0.1)

      assert %DataFrame{plan: {:sample, @base_plan, lower, 0.1, false, seed, false}} = result
      assert lower == +0.0
      assert is_integer(seed)
    end

    test "accepts with_replacement and seed" do
      result = DataFrame.sample(df(), 0.5, with_replacement: true, seed: 42)

      assert %DataFrame{plan: {:sample, @base_plan, lower, 0.5, true, 42, false}} = result
      assert lower == +0.0
    end
  end

  describe "random_split/3" do
    test "creates deterministic sample ranges from weights" do
      [s1, s2, s3] = DataFrame.random_split(df(), [1.0, 2.0, 3.0], 7)

      assert %DataFrame{plan: {:sample, @base_plan, l1, u1, false, 7, true}} = s1
      assert %DataFrame{plan: {:sample, @base_plan, l2, u2, false, 7, true}} = s2
      assert %DataFrame{plan: {:sample, @base_plan, l3, 1.0, false, 7, true}} = s3
      assert_in_delta l1, 0.0, 1.0e-12
      assert_in_delta u1, 1.0 / 6.0, 1.0e-12
      assert_in_delta l2, 1.0 / 6.0, 1.0e-12
      assert_in_delta u2, 3.0 / 6.0, 1.0e-12
      assert_in_delta l3, 3.0 / 6.0, 1.0e-12
    end
  end

  # ── Row Operations ──

  describe "offset/2" do
    test "creates offset plan" do
      result = DataFrame.offset(df(), 10)
      assert %DataFrame{plan: {:offset, @base_plan, 10}} = result
    end
  end

  describe "tail/2" do
    test "creates tail plan" do
      result = DataFrame.tail(df(), 5)
      assert %DataFrame{plan: {:tail, @base_plan, 5}} = result
    end
  end

  # ── Query Shaping ──

  describe "hint/3" do
    test "creates hint plan with no parameters" do
      result = DataFrame.hint(df(), "broadcast")
      assert %DataFrame{plan: {:hint, @base_plan, "broadcast", []}} = result
    end

    test "creates hint plan with parameters" do
      result = DataFrame.hint(df(), "coalesce", [Functions.lit(3)])
      assert %DataFrame{plan: {:hint, @base_plan, "coalesce", [{:lit, 3}]}} = result
    end

    test "accepts primitive parameter" do
      result = DataFrame.hint(df(), "coalesce", 3)
      assert %DataFrame{plan: {:hint, @base_plan, "coalesce", [{:lit, 3}]}} = result
    end

    test "accepts primitive list parameter" do
      result = DataFrame.hint(df(), "repartition", ["id", 3])

      assert %DataFrame{plan: {:hint, @base_plan, "repartition", [{:lit, "id"}, {:lit, 3}]}} =
               result
    end
  end

  describe "with_metadata/3" do
    test "encodes metadata update via with_columns alias metadata" do
      result = DataFrame.with_metadata(df(), "name", %{"k" => "v"})

      assert %DataFrame{
               plan:
                 {:with_columns, @base_plan, [{:alias, {:col, "name"}, "name", metadata_json}]}
             } = result

      assert {:ok, %{"k" => "v"}} = Jason.decode(metadata_json)
    end
  end

  describe "with_watermark/3" do
    test "creates with_watermark plan" do
      result = DataFrame.with_watermark(df(), "event_time", "10 minutes")

      assert %DataFrame{
               plan: {:with_watermark, @base_plan, "event_time", "10 minutes"}
             } = result
    end
  end

  describe "drop_duplicates/2" do
    test "with empty subset acts like distinct" do
      result = DataFrame.drop_duplicates(df())
      assert %DataFrame{plan: {:deduplicate, @base_plan, [], true}} = result
    end

    test "with subset creates deduplicate plan" do
      result = DataFrame.drop_duplicates(df(), ["id", "name"])
      assert %DataFrame{plan: {:deduplicate, @base_plan, ["id", "name"], false}} = result
    end
  end

  describe "drop_duplicates_within_watermark/2" do
    test "creates deduplicate plan with within_watermark=true" do
      result = DataFrame.drop_duplicates_within_watermark(df(), ["id"])
      assert %DataFrame{plan: {:deduplicate, @base_plan, ["id"], false, true}} = result
    end

    test "with empty subset uses all columns" do
      result = DataFrame.drop_duplicates_within_watermark(df())
      assert %DataFrame{plan: {:deduplicate, @base_plan, [], true, true}} = result
    end
  end

  # ── Reshaping ──

  describe "unpivot/5" do
    test "creates unpivot plan" do
      result = DataFrame.unpivot(df(), ["id"], ["col1", "col2"], "variable", "value")

      assert %DataFrame{
               plan:
                 {:unpivot, @base_plan, [{:col, "id"}], [{:col, "col1"}, {:col, "col2"}],
                  "variable", "value"}
             } = result
    end

    test "accepts nil values to unpivot all non-id columns" do
      result = DataFrame.unpivot(df(), ["id"], nil, "var", "val")

      assert %DataFrame{
               plan: {:unpivot, @base_plan, [{:col, "id"}], nil, "var", "val"}
             } = result
    end
  end

  describe "transpose/2" do
    test "creates transpose plan with no index" do
      result = DataFrame.transpose(df())
      assert %DataFrame{plan: {:transpose, @base_plan, []}} = result
    end

    test "creates transpose plan with index column" do
      result = DataFrame.transpose(df(), index_column: "id")
      assert %DataFrame{plan: {:transpose, @base_plan, [{:col, "id"}]}} = result
    end
  end

  describe "alias_/2" do
    test "creates subquery_alias plan" do
      result = DataFrame.alias_(df(), "t")
      assert %DataFrame{plan: {:subquery_alias, @base_plan, "t"}} = result
    end
  end

  describe "subquery helpers" do
    test "as_table returns table arg wrapper" do
      assert %SparkEx.TableArg{plan: @base_plan} = DataFrame.as_table(df())
    end

    test "scalar returns subquery expression column" do
      assert %Column{expr: {:subquery, :scalar, @base_plan, []}} = DataFrame.scalar(df())
    end

    test "exists returns subquery expression column" do
      assert %Column{expr: {:subquery, :exists, @base_plan, []}} = DataFrame.exists(df())
    end
  end

  # ── Convenience Aliases ──

  describe "where/2" do
    test "delegates to filter" do
      condition = Column.gt(Functions.col("age"), Functions.lit(18))
      filtered = DataFrame.filter(df(), condition)
      whered = DataFrame.where(df(), condition)
      assert filtered.plan == whered.plan
    end
  end

  describe "union_all/2" do
    test "delegates to union" do
      df1 = df()
      df2 = df({:sql, "SELECT * FROM b", nil})
      assert DataFrame.union_all(df1, df2).plan == DataFrame.union(df1, df2).plan
    end
  end

  describe "cross_join/2" do
    test "creates cross join plan" do
      df1 = df()
      df2 = df({:sql, "SELECT * FROM b", nil})
      result = DataFrame.cross_join(df1, df2)
      assert %DataFrame{plan: {:join, _, _, nil, :cross, []}} = result
    end
  end

  # ── Chaining ──

  describe "M10 chaining" do
    test "new transforms compose with existing ones" do
      result =
        df()
        |> DataFrame.select_expr(["name", "age"])
        |> DataFrame.with_column_renamed("name", "full_name")
        |> DataFrame.repartition(4)
        |> DataFrame.sort_within_partitions(["full_name"])
        |> DataFrame.limit(100)

      assert {:limit,
              {:sort, {:repartition, {:with_columns_renamed, {:project, _, _}, _}, _, _}, _, _},
              100} =
               result.plan
    end
  end
end
