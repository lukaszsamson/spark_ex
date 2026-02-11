#!/usr/bin/env elixir

defmodule ParityRunner do
  @moduledoc false

  @default_pyspark_root "/Users/lukaszsamson/claude_fun/spark/python/pyspark/sql/tests/connect"

  @suite_globs %{
    "test_parity_dataframe" => ["test/integration/dataframe*_test.exs"],
    "test_parity_column" => ["test/unit/*column*_test.exs"],
    "test_parity_readwriter" => [
      "test/unit/reader*_test.exs",
      "test/unit/writer*_test.exs",
      "test/integration/writer*_test.exs"
    ],
    "test_parity_catalog" => ["test/integration/m12_catalog_test.exs"],
    "test_parity_functions" => [
      "test/unit/m11_functions_test.exs",
      "test/unit/function_parity_test.exs",
      "test/unit/function_gen_test.exs"
    ],
    "test_parity_sql" => ["test/integration/query_test.exs"],
    "test_parity_types" => [
      "test/unit/type_mapper*_test.exs",
      "test/integration/create_dataframe_test.exs"
    ],
    "test_parity_stat" => ["test/unit/m13_stat_test.exs", "test/integration/m13_na_stat_test.exs"],
    "test_parity_subquery" => ["test/integration/m11_subquery_grouped_data_test.exs"],
    "test_parity_tvf" => ["test/integration/m13_udf_tvf_test.exs"],
    "test_parity_conf" => [
      "test/unit/config_ops_test.exs",
      "test/integration/config_ops_test.exs"
    ],
    "test_parity_group" => ["test/unit/*grouped_data*_test.exs"],
    "test_parity_errors" => ["test/unit/errors_test.exs", "test/unit/limit_exceeded_test.exs"],
    "test_parity_datasources" => [
      "test/unit/reader*_test.exs",
      "test/integration/dataframe*_test.exs"
    ],
    "test_connect_channel" => ["test/unit/channel_test.exs"],
    "test_connect_retry" => ["test/unit/retry_test.exs"],
    "test_connect_reattach" => ["test/unit/reattach_test.exs"],
    "test_connect_plan" => ["test/unit/*plan_encoder*_test.exs"],
    "test_connect_function" => ["test/unit/functions_test.exs"],
    "test_connect_column" => ["test/unit/column_test.exs"],
    "test_connect_readwriter" => ["test/unit/reader*_test.exs", "test/unit/writer*_test.exs"],
    "test_connect_error" => ["test/unit/errors_test.exs"],
    "test_connect_stat" => ["test/unit/m13_stat_test.exs"],
    "test_connect_session" => ["test/unit/session_test.exs"],
    "test_connect_creation" => ["test/unit/create_dataframe_test.exs"],
    "test_connect_basic" => [
      "test/integration/spark_version_test.exs",
      "test/integration/session_lifecycle_test.exs"
    ],
    "test_parity_streaming" => ["test/integration/m14_streaming_test.exs"],
    "test_parity_listener" => ["test/integration/m14_streaming_test.exs"],
    "test_parity_foreach" => ["test/integration/m14_streaming_test.exs"],
    "test_parity_foreach_batch" => ["test/integration/m14_streaming_test.exs"]
  }

  def main(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        switches: [
          pyspark_root: :string,
          sparkex_root: :string,
          generate_stubs: :boolean,
          output: :string
        ],
        aliases: [p: :pyspark_root, s: :sparkex_root, g: :generate_stubs, o: :output]
      )

    sparkex_root = opts[:sparkex_root] || Path.expand(Path.join(__DIR__, "."))
    pyspark_root = opts[:pyspark_root] || @default_pyspark_root

    suites = discover_pyspark_suites(pyspark_root)
    sparkex_files = discover_sparkex_tests(sparkex_root)

    results =
      Enum.map(suites, fn suite ->
        covered = covered?(suite.key, sparkex_root, sparkex_files)
        Map.put(suite, :covered, covered)
      end)

    missing = Enum.filter(results, &(!&1.covered))

    report = render_report(results, missing)

    case opts[:output] do
      nil -> IO.puts(report)
      path -> File.write!(path, report)
    end

    if opts[:generate_stubs] do
      generate_stubs(missing, sparkex_root)
    end
  end

  defp discover_pyspark_suites(pyspark_root) do
    pyspark_root = Path.expand(pyspark_root)
    tests_root = Path.expand(Path.join(pyspark_root, ".."))
    classic_streaming_root = Path.join(tests_root, "streaming")

    connect_files = Path.wildcard(Path.join(pyspark_root, "**/test_*.py"))
    classic_streaming_files = Path.wildcard(Path.join(classic_streaming_root, "test_*.py"))

    connect_suites =
      Enum.map(connect_files, fn path ->
        rel = Path.relative_to(path, tests_root)
        %{key: Path.basename(path, ".py"), rel: rel, kind: suite_kind(rel)}
      end)

    classic_suites =
      Enum.map(classic_streaming_files, fn path ->
        rel = Path.relative_to(path, tests_root)
        %{key: Path.basename(path, ".py"), rel: rel, kind: :classic_streaming}
      end)

    (connect_suites ++ classic_suites)
    |> Enum.uniq_by(& &1.key)
    |> Enum.sort_by(& &1.key)
  end

  defp discover_sparkex_tests(sparkex_root) do
    sparkex_root = Path.expand(sparkex_root)

    integration = Path.wildcard(Path.join(sparkex_root, "test/integration/**/*_test.exs"))
    unit = Path.wildcard(Path.join(sparkex_root, "test/unit/**/*_test.exs"))

    (integration ++ unit)
    |> Enum.map(&Path.relative_to(&1, sparkex_root))
    |> MapSet.new()
  end

  defp suite_kind(rel_path) do
    cond do
      String.contains?(rel_path, "connect/streaming/") -> :connect_streaming
      String.contains?(rel_path, "connect/pandas/") -> :connect_pandas
      String.contains?(rel_path, "connect/arrow/") -> :connect_arrow
      String.contains?(rel_path, "connect/client/") -> :connect_client
      String.contains?(rel_path, "connect/shell/") -> :connect_shell
      String.contains?(rel_path, "connect/") -> :connect
      true -> :unknown
    end
  end

  defp covered?(suite_key, sparkex_root, sparkex_files) do
    case Map.get(@suite_globs, suite_key) do
      nil ->
        false

      globs ->
        globs
        |> Enum.flat_map(fn glob -> Path.wildcard(Path.join(sparkex_root, glob)) end)
        |> Enum.map(&Path.relative_to(&1, sparkex_root))
        |> Enum.any?(&MapSet.member?(sparkex_files, &1))
    end
  end

  defp render_report(results, missing) do
    total = length(results)
    missing_count = length(missing)
    covered_count = total - missing_count

    header =
      [
        "SparkEx parity runner report",
        "- total suites: #{total}",
        "- covered: #{covered_count}",
        "- missing: #{missing_count}",
        "",
        "Missing suites:"
      ]
      |> Enum.join("\n")

    missing_lines =
      missing
      |> Enum.map(fn suite -> "- #{suite.key} (#{suite.kind}) [#{suite.rel}]" end)
      |> Enum.join("\n")

    header <> "\n" <> missing_lines
  end

  defp generate_stubs(missing, sparkex_root) do
    stubs_root = Path.join(sparkex_root, "test/integration/parity")
    File.mkdir_p!(stubs_root)

    Enum.each(missing, fn suite ->
      file_base = suite.key |> String.replace_prefix("test_", "")
      file_path = Path.join(stubs_root, "#{file_base}_test.exs")

      unless File.exists?(file_path) do
        module_name =
          file_base
          |> String.split("_")
          |> Enum.map(&Macro.camelize/1)
          |> Enum.join()

        content = stub_content(module_name, suite.rel)
        File.write!(file_path, content)
      end
    end)
  end

  defp stub_content(module_name, pyspark_rel_path) do
    """
    defmodule SparkEx.Integration.Parity.#{module_name}Test do
      use ExUnit.Case

      @moduletag [:integration, :parity, :parity_missing]

      @pyspark_suite "#{pyspark_rel_path}"

      @tag :skip
      test "parity scaffold" do
        flunk("Not implemented: map PySpark suite #{@pyspark_suite}")
      end
    end
    """
  end
end

ParityRunner.main(System.argv())
