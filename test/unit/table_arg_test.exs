defmodule SparkEx.TableArgTest do
  use ExUnit.Case, async: true

  alias SparkEx.DataFrame
  alias SparkEx.TableArg

  test "order_by appends sort keys across chained calls" do
    table_arg =
      %DataFrame{session: self(), plan: {:sql, "SELECT * FROM t", nil}}
      |> DataFrame.as_table()
      |> TableArg.partition_by(["dept"])
      |> TableArg.order_by(["age"])
      |> TableArg.order_by(["name"])

    assert table_arg.order_spec == [
             {:sort_order, {:col, "age"}, :asc, :nulls_first},
             {:sort_order, {:col, "name"}, :asc, :nulls_first}
           ]
  end
end
