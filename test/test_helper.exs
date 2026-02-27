ExUnit.start(exclude: [:integration])

# When integration tests are included, detect the Spark version and exclude
# tests tagged with min_spark versions higher than the running server.
# Only attempt connection if --include integration is passed or SPARK_REMOTE is set.
run_integration? =
  System.get_env("SPARK_REMOTE") != nil or
    Enum.any?(System.argv(), &(&1 == "integration"))

if run_integration? do
  spark_remote = System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  with {:ok, session} <- SparkEx.connect(url: spark_remote),
       {:ok, vsn} <- SparkEx.spark_version(session) do
    SparkEx.Session.stop(session)
    spark_ver = vsn |> String.split(".") |> Enum.take(2) |> Enum.join(".")

    excludes =
      for v <- ["4.0", "4.1"],
          Version.compare(Version.parse!("#{spark_ver}.0"), Version.parse!("#{v}.0")) == :lt,
          do: {:min_spark, v}

    if excludes != [] do
      IO.puts("Spark #{vsn} detected — excluding tests requiring: #{inspect(excludes)}")
      existing = ExUnit.configuration()[:exclude] || []
      ExUnit.configure(exclude: existing ++ excludes)
    else
      IO.puts("Spark #{vsn} detected — all version gates satisfied")
    end
  else
    _ -> IO.puts("Warning: could not detect Spark version, skipping version gating")
  end
end
