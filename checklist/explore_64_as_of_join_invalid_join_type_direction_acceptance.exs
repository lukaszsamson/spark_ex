import SparkEx.Functions
alias SparkEx.DataFrame

df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}, tags: []}
right = %DataFrame{session: self(), plan: {:sql, "SELECT 2", nil}, tags: []}

result =
  try do
    asof =
      DataFrame.as_of_join(
        df,
        right,
        col("ts"),
        col("ts2"),
        on: ["id"],
        join_type: 123,
        direction: "sideways"
      )

    {:ok, asof.plan}
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  end

IO.inspect(result, label: "as_of_join accepts invalid join_type/direction")
