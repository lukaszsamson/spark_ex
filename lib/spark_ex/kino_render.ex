if Code.ensure_loaded?(Kino.Render) do
  defimpl Kino.Render, for: SparkEx.DataFrame do
    @moduledoc false

    @preview_rows 100

    def to_livebook(df) do
      tabs = [
        {"Schema", schema_tab(df)},
        {"Preview", preview_tab(df)},
        {"Explain", explain_tab(df)},
        {"Raw", raw_tab(df)}
      ]

      tabs
      |> Kino.Layout.tabs()
      |> Kino.Render.to_livebook()
    end

    defp schema_tab(df) do
      case SparkEx.DataFrame.schema(df) do
        {:ok, schema} ->
          text = format_schema(schema)
          Kino.Text.new(text)

        {:error, reason} ->
          Kino.Text.new("Schema unavailable: #{inspect(reason)}")
      end
    end

    defp preview_tab(df) do
      case SparkEx.DataFrame.to_explorer(df, max_rows: @preview_rows) do
        {:ok, explorer_df} ->
          Kino.DataTable.new(explorer_df, sorting_enabled: true)

        {:error, reason} ->
          Kino.Text.new("Preview unavailable: #{inspect(reason)}")
      end
    end

    defp explain_tab(df) do
      case SparkEx.DataFrame.explain(df, :extended) do
        {:ok, text} ->
          Kino.Text.new(text)

        {:error, reason} ->
          Kino.Text.new("Explain unavailable: #{inspect(reason)}")
      end
    end

    defp raw_tab(df) do
      Kino.Text.new(inspect(df, pretty: true))
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
