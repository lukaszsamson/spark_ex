ExUnit.start(exclude: [:integration])

# When SPARK_REMOTE is set, detect the Spark version and configure ExUnit to
# run integration tests while excluding tests that require a newer Spark version.
#
# We intentionally avoid `--include integration` on the CLI because ExUnit's
# include overrides ALL excludes for matching tests, which would defeat our
# min_spark version gating. Instead, we set SPARK_REMOTE and replace the
# exclude list here (removing :integration so those tests run).
if System.get_env("SPARK_REMOTE") do
  spark_remote = System.get_env("SPARK_REMOTE")

  with {:ok, session} <- SparkEx.connect(url: spark_remote),
       {:ok, vsn} <- SparkEx.spark_version(session) do
    SparkEx.Session.stop(session)
    spark_ver = vsn |> String.split(".") |> Enum.take(2) |> Enum.join(".")

    excludes =
      for v <- ["4.0", "4.1"],
          Version.compare(Version.parse!("#{spark_ver}.0"), Version.parse!("#{v}.0")) == :lt,
          do: {:min_spark, v}

    # Replace the exclude list: :integration is removed so integration tests
    # run, and version-specific excludes are added.
    ExUnit.configure(exclude: excludes)

    if excludes != [] do
      IO.puts("Spark #{vsn} detected — excluding tests requiring: #{inspect(excludes)}")
    else
      IO.puts("Spark #{vsn} detected — all version gates satisfied")
    end
  else
    _ -> IO.puts("Warning: could not detect Spark version, skipping version gating")
  end
end
