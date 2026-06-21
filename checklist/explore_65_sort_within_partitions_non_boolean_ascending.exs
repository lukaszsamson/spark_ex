alias SparkEx.DataFrame

df = %DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}, tags: []}

result =
  try do
    sorted = DataFrame.sort_within_partitions(df, ["id"], ascending: ["oops"])
    {:ok, sorted.plan}
  rescue
    e -> {:rescued, e.__struct__, Exception.message(e)}
  end

IO.inspect(result, label: "sort_within_partitions accepts non-boolean ascending")
