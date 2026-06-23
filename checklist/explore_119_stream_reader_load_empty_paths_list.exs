# Repro: StreamReader.load/2 accepts empty path list and fails remotely instead of local validation
alias SparkEx.{StreamReader, DataFrame}

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

reader =
  StreamReader.new(s)
  |> StreamReader.format("json")
  |> StreamReader.option("maxFilesPerTrigger", 1)

result =
  try do
    df = StreamReader.load(reader, [])
    DataFrame.collect(df)
  rescue
    e -> {:error, Exception.message(e)}
  end

IO.inspect(result, label: "collect_empty_paths")
IO.puts("session_alive?: #{Process.alive?(s)}")
