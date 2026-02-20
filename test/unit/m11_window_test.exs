defmodule SparkEx.M11.WindowTest do
  use ExUnit.Case, async: true

  alias SparkEx.Window
  alias SparkEx.WindowSpec
  alias SparkEx.Functions

  describe "Window convenience constructors" do
    test "partition_by/1 creates a spec with partition columns" do
      spec = Window.partition_by(["dept", "role"])
      assert %WindowSpec{partition_spec: [{:col, "dept"}, {:col, "role"}]} = spec
      assert spec.order_spec == []
      assert spec.frame_spec == nil
    end

    test "partition_by/1 with Column structs" do
      spec = Window.partition_by([Functions.col("dept")])
      assert %WindowSpec{partition_spec: [{:col, "dept"}]} = spec
    end

    test "order_by/1 creates a spec with sort orders" do
      spec = Window.order_by(["salary"])

      assert %WindowSpec{
               order_spec: [{:sort_order, {:col, "salary"}, :asc, :nulls_first}],
               partition_spec: []
             } = spec
    end

    test "order_by/1 with sort-order Column" do
      spec = Window.order_by([SparkEx.Column.desc(Functions.col("salary"))])

      assert %WindowSpec{
               order_spec: [{:sort_order, {:col, "salary"}, :desc, :nulls_last}]
             } = spec
    end

    test "rows_between/2" do
      spec = Window.rows_between(-1, 1)
      assert %WindowSpec{frame_spec: {:rows, -1, 1}} = spec
    end

    test "range_between/2" do
      spec = Window.range_between(:unbounded, :current_row)
      assert %WindowSpec{frame_spec: {:range, :unbounded, :current_row}} = spec
    end

    test "boundary constants" do
      assert Window.unbounded_preceding() == :unbounded
      assert Window.unbounded_following() == :unbounded
      assert Window.current_row() == :current_row
    end
  end

  describe "WindowSpec pipeline" do
    test "partition_by then order_by" do
      spec =
        Window.partition_by(["dept"])
        |> WindowSpec.order_by(["salary"])

      assert %WindowSpec{
               partition_spec: [{:col, "dept"}],
               order_spec: [{:sort_order, {:col, "salary"}, :asc, :nulls_first}]
             } = spec
    end

    test "partition_by then order_by then rows_between" do
      spec =
        Window.partition_by(["dept"])
        |> WindowSpec.order_by(["salary"])
        |> WindowSpec.rows_between(-2, 2)

      assert %WindowSpec{
               partition_spec: [{:col, "dept"}],
               order_spec: [{:sort_order, {:col, "salary"}, :asc, :nulls_first}],
               frame_spec: {:rows, -2, 2}
             } = spec
    end

    test "full pipeline with unbounded frame" do
      spec =
        Window.partition_by(["dept"])
        |> WindowSpec.order_by(["hire_date"])
        |> WindowSpec.range_between(:unbounded, :current_row)

      assert %WindowSpec{
               frame_spec: {:range, :unbounded, :current_row}
             } = spec
    end
  end

  describe "WindowSpec.partition_by/2 replaces partition columns" do
    test "replaces existing partition spec" do
      spec =
        %WindowSpec{partition_spec: [{:col, "old"}]}
        |> WindowSpec.partition_by(["new1", "new2"])

      assert spec.partition_spec == [{:col, "new1"}, {:col, "new2"}]
    end
  end

  describe "WindowSpec.order_by/2 replaces order columns" do
    test "replaces existing order spec" do
      spec =
        %WindowSpec{order_spec: [{:sort_order, {:col, "old"}, :asc, :nulls_first}]}
        |> WindowSpec.order_by(["new"])

      assert spec.order_spec == [{:sort_order, {:col, "new"}, :asc, :nulls_first}]
    end
  end

  describe "WindowSpec with atom column names" do
    test "accepts atom names" do
      spec = WindowSpec.partition_by(%WindowSpec{}, [:dept])
      assert spec.partition_spec == [{:col, "dept"}]
    end
  end
end
