defmodule SparkEx.M13.MergeIntoTest do
  use ExUnit.Case, async: true

  alias SparkEx.{Column, DataFrame, MergeIntoWriter}
  alias SparkEx.Functions

  defp make_df(plan \\ :source_plan) do
    %DataFrame{session: self(), plan: plan}
  end

  describe "new/2" do
    test "creates a MergeIntoWriter with source df and target table" do
      m = MergeIntoWriter.new(make_df(), "target_table")

      assert %MergeIntoWriter{
               source_df: %DataFrame{plan: :source_plan},
               target_table: "target_table",
               condition: nil,
               match_actions: [],
               not_matched_actions: [],
               not_matched_by_source_actions: [],
               schema_evolution: false
             } = m
    end
  end

  describe "on/2" do
    test "sets the merge condition" do
      condition = Column.eq(Functions.col("source.id"), Functions.col("target.id"))

      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.on(condition)

      assert m.condition == {:fn, "==", [{:col, "source.id"}, {:col, "target.id"}], false}
    end
  end

  describe "when_matched_delete/1" do
    test "adds delete action without condition" do
      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_matched_delete()

      assert m.match_actions == [{:delete, nil, []}]
    end

    test "adds delete action with condition" do
      cond_col = Column.gt(Functions.col("age"), Functions.lit(0))

      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_matched_delete(cond_col)

      assert [{:delete, {:fn, ">", [{:col, "age"}, {:lit, 0}], false}, []}] = m.match_actions
    end
  end

  describe "when_matched_update_all/1" do
    test "adds update_star action" do
      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_matched_update_all()

      assert m.match_actions == [{:update_star, nil, []}]
    end
  end

  describe "when_matched_update/2" do
    test "adds update action with assignments" do
      assignments = %{
        "name" => Functions.col("source.name"),
        "age" => Functions.col("source.age")
      }

      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_matched_update(assignments)

      assert [{:update, nil, assigns}] = m.match_actions
      assert length(assigns) == 2

      assert Enum.all?(assigns, fn {{:col, target}, value_expr} ->
               is_binary(target) and is_tuple(value_expr)
             end)
    end
  end

  describe "when_not_matched_insert_all/1" do
    test "adds insert_star action" do
      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_not_matched_insert_all()

      assert m.not_matched_actions == [{:insert_star, nil, []}]
    end
  end

  describe "when_not_matched_insert/2" do
    test "adds insert action with assignments" do
      assignments = %{"name" => Functions.lit("new")}

      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_not_matched_insert(assignments)

      assert [{:insert, nil, [{{:col, "name"}, {:lit, "new"}}]}] = m.not_matched_actions
    end
  end

  describe "when_not_matched_by_source_delete/1" do
    test "adds delete action to not_matched_by_source" do
      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_not_matched_by_source_delete()

      assert m.not_matched_by_source_actions == [{:delete, nil, []}]
    end
  end

  describe "when_not_matched_by_source_update_all/1" do
    test "adds update_star action to not_matched_by_source" do
      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.when_not_matched_by_source_update_all()

      assert m.not_matched_by_source_actions == [{:update_star, nil, []}]
    end
  end

  describe "with_schema_evolution/1" do
    test "enables schema evolution" do
      m =
        MergeIntoWriter.new(make_df(), "t")
        |> MergeIntoWriter.with_schema_evolution()

      assert m.schema_evolution == true
    end
  end

  describe "merge/1" do
    test "raises without condition" do
      m = MergeIntoWriter.new(make_df(), "t")

      assert_raise ArgumentError, ~r/merge condition/, fn ->
        MergeIntoWriter.merge(m)
      end
    end
  end

  describe "DataFrame.merge_into/2" do
    test "creates a MergeIntoWriter from DataFrame" do
      m = DataFrame.merge_into(make_df(), "my_table")

      assert %MergeIntoWriter{
               target_table: "my_table",
               source_df: %DataFrame{plan: :source_plan}
             } = m
    end
  end

  describe "chained builder" do
    test "builds complete merge command tuple" do
      condition = Column.eq(Functions.col("s.id"), Functions.col("t.id"))

      m =
        make_df()
        |> DataFrame.merge_into("target")
        |> MergeIntoWriter.on(condition)
        |> MergeIntoWriter.when_matched_update_all()
        |> MergeIntoWriter.when_not_matched_insert_all()
        |> MergeIntoWriter.when_not_matched_by_source_delete()

      assert m.target_table == "target"
      assert m.condition != nil
      assert length(m.match_actions) == 1
      assert length(m.not_matched_actions) == 1
      assert length(m.not_matched_by_source_actions) == 1
    end
  end
end
