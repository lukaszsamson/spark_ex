defmodule FindMissingFunctions do
  def run do
    files = [
      "../spark/python/pyspark/sql/connect/functions/builtin.py",
      "../spark/python/pyspark/sql/connect/functions/partitioning.py",
      "../spark/python/pyspark/sql/connect/avro/functions.py",
      "../spark/python/pyspark/sql/connect/protobuf/functions.py"
    ]

    python_funcs =
      files
      |> Enum.flat_map(fn file ->
        if File.exists?(file) do
          content = File.read!(file)
          Regex.scan(~r/^def ([a-zA-Z0-9_]+)\(/m, content)
          |> Enum.map(fn [_, name] -> name end)
        else
          IO.puts(:stderr, "Cannot find #{file}")
          []
        end
      end)
      |> Enum.reject(&String.starts_with?(&1, "_"))
      |> MapSet.new()

    normalize = fn name ->
      name
      |> to_string()
      |> String.downcase()
      |> String.replace("_", "")
    end

    python_normalized =
      python_funcs
      |> Enum.map(&{normalize.(&1), &1})
      |> Enum.into(%{})

    funcs = SparkEx.Functions.__info__(:functions) |> Enum.map(&elem(&1, 0))
    macros = SparkEx.Functions.__info__(:macros) |> Enum.map(&elem(&1, 0))

    elixir_normalized =
      (funcs ++ macros)
      |> Enum.reject(&String.starts_with?(to_string(&1), "_"))
      |> Enum.map(&normalize.(&1))
      |> MapSet.new()

    missing_normalized_keys =
      MapSet.difference(Map.keys(python_normalized) |> MapSet.new(), elixir_normalized)

    missing_funcs =
      missing_normalized_keys
      |> Enum.map(&Map.get(python_normalized, &1))
      |> Enum.sort()

    IO.puts("Python function definitions found: #{MapSet.size(python_funcs)}")
    IO.puts("Elixir SparkEx.Functions (funcs & macros): #{length(funcs ++ macros)}")
    IO.puts("Missing functions in Elixir (Count: #{length(missing_funcs)}):")
    IO.puts("---")
    Enum.each(missing_funcs, fn f -> IO.puts("- `#{f}`") end)
  end
end

FindMissingFunctions.run()
