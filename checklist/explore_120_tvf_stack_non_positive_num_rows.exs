# Repro: TableValuedFunction.stack/3 accepts non-positive num_rows and only fails remotely.
import SparkEx.Functions
alias SparkEx.{TableValuedFunction, DataFrame}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))
tvf = TableValuedFunction.new(s)

ok = TableValuedFunction.stack(tvf, 2, [lit("A"), lit(1), lit("B"), lit(2)]) |> DataFrame.collect()

bad0 =
  try do
    TableValuedFunction.stack(tvf, 0, [lit("A"), lit(1)]) |> DataFrame.collect()
  rescue
    e -> {:error, Exception.message(e)}
  end

badn =
  try do
    TableValuedFunction.stack(tvf, -1, [lit("A"), lit(1)]) |> DataFrame.collect()
  rescue
    e -> {:error, Exception.message(e)}
  end

IO.inspect(ok, label: "stack_pos_control")
IO.inspect(bad0, label: "stack_zero")
IO.inspect(badn, label: "stack_negative")
IO.puts("session_alive?: #{Process.alive?(s)}")
