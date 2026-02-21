if Code.ensure_loaded?(Kino) do
  defmodule SparkEx.Livebook do
    @moduledoc """
    Livebook helpers for `SparkEx.DataFrame`.

    All helpers return Kino terms (not raw strings/lists) and do not call
    `Kino.render/1` internally — the caller controls rendering.

    Requires the `:kino` and `:explorer` optional dependencies.
    """

    alias SparkEx.DataFrame

    @preview_rows_default 100

    @doc """
    Returns a `Kino.DataTable` preview of the DataFrame.

    Always executes a bounded preview query (LIMIT is injected).
    Raises `RuntimeError` if preview execution fails.

    ## Options

    - `:num_rows` — number of rows to preview (default: 100)
    - `:name` — table name passed to `Kino.DataTable`
    - `:sorting_enabled` — enable sorting in the table (default: true)
    - `:formatter` — optional formatter function
    """
    @spec preview(DataFrame.t(), keyword()) :: Kino.DataTable.t()
    def preview(%DataFrame{} = df, opts \\ []) do
      num_rows = Keyword.get(opts, :num_rows, @preview_rows_default)

      case DataFrame.to_explorer(df, max_rows: num_rows) do
        {:ok, explorer_df} ->
          table_opts =
            opts
            |> Keyword.take([:name, :sorting_enabled, :formatter])
            |> Keyword.put_new(:sorting_enabled, true)

          Kino.DataTable.new(explorer_df, table_opts)

        {:error, reason} ->
          raise RuntimeError, "Livebook preview failed: #{inspect(reason)}"
      end
    end

    @doc """
    Returns a `Kino.Text` with the explain plan string.
    Raises `RuntimeError` if explain execution fails.

    ## Options

    - `:mode` — explain mode (default: `:extended`)
    """
    @spec explain(DataFrame.t(), keyword()) :: Kino.Text.t()
    def explain(%DataFrame{} = df, opts \\ []) do
      mode = Keyword.get(opts, :mode, :extended)

      case DataFrame.explain(df, mode) do
        {:ok, text} ->
          Kino.Text.new(text)

        {:error, reason} ->
          raise RuntimeError, "Livebook explain failed: #{inspect(reason)}"
      end
    end

    @doc """
    Returns a `Kino.DataTable` with a random sample of rows.

    Equivalent to `preview/2` but conceptually for sampling.

    ## Options

    Same as `preview/2`.
    """
    @spec sample(DataFrame.t(), keyword()) :: Kino.DataTable.t()
    def sample(%DataFrame{} = df, opts \\ []) do
      preview(df, opts)
    end

    @doc """
    Returns a `Kino.Text` with the DataFrame's schema information.
    Raises `RuntimeError` if schema retrieval fails.
    """
    @spec schema(DataFrame.t()) :: Kino.Text.t()
    def schema(%DataFrame{} = df) do
      case DataFrame.schema(df) do
        {:ok, schema} ->
          text = format_schema(schema)
          Kino.Text.new(text)

        {:error, reason} ->
          raise RuntimeError, "Livebook schema failed: #{inspect(reason)}"
      end
    end

    defp format_schema(%Spark.Connect.DataType{kind: {:struct, struct}}) do
      struct.fields
      |> Enum.map(fn field ->
        nullable = if field.nullable, do: " (nullable)", else: ""
        type_str = format_data_type(field.data_type)
        "#{field.name}: #{type_str}#{nullable}"
      end)
      |> Enum.join("\n")
    end

    defp format_schema(other), do: inspect(other, pretty: true)

    defp format_data_type(%Spark.Connect.DataType{kind: {tag, _}}), do: to_string(tag)
    defp format_data_type(other), do: inspect(other)
  end
end
