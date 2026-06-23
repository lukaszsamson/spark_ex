# Repro: GroupedData numeric shortcut methods with scalar string arg raise CaseClauseError
# Expected: clean ArgumentError like other input validation paths.
import SparkEx.Functions
alias SparkEx.{DataFrame, GroupedData}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

{:ok, df} = SparkEx.create_dataframe(s, [%{"grp" => "x", "v" => 1}, %{"grp" => "y", "v" => 2}], schema: "grp STRING, v INT")
gd = DataFrame.group_by(df, [col("grp")])

# control: list form works
IO.inspect(GroupedData.sum(gd, ["v"]) |> DataFrame.collect(), label: "control sum([\"v\"]) ok")

for fun <- [:sum, :avg, :min, :max, :mean] do
  res =
    try do
      apply(GroupedData, fun, [gd, "v"]) |> DataFrame.collect()
    rescue
      e -> {:error, Exception.message(e)}
    end

  IO.inspect(res, label: "#{fun}(\"v\")")
end

IO.puts("session_alive?: #{Process.alive?(s)}")
