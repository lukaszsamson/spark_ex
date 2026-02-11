defmodule SparkEx.SqlFormatter do
  @moduledoc """
  Formats SQL strings with auto-quoting of parameters.
  """

  @spec format(String.t(), list() | map() | nil) :: String.t()
  def format(sql, nil) when is_binary(sql), do: sql

  def format(sql, args) when is_binary(sql) and is_list(args) do
    {result, rest} =
      sql
      |> String.graphemes()
      |> Enum.reduce({[], args}, fn
        "?", {acc, [value | tail]} -> {[quote_value(value) | acc], tail}
        ch, {acc, remaining} -> {[ch | acc], remaining}
      end)

    if rest == [] do
      result |> Enum.reverse() |> IO.iodata_to_binary()
    else
      raise ArgumentError, "not enough placeholders for positional args"
    end
  end

  def format(sql, args) when is_binary(sql) and is_map(args) and not is_struct(args) do
    Enum.reduce(args, sql, fn {key, value}, acc ->
      String.replace(acc, ":#{key}", quote_value(value))
    end)
  end

  def format(sql, args) do
    raise ArgumentError,
          "expected sql as string and args as list, map, or nil, got: #{inspect({sql, args})}"
  end

  defp quote_value(nil), do: "NULL"
  defp quote_value(true), do: "TRUE"
  defp quote_value(false), do: "FALSE"
  defp quote_value(%Date{} = date), do: "DATE '#{Date.to_iso8601(date)}'"
  defp quote_value(%NaiveDateTime{} = dt), do: "TIMESTAMP '#{NaiveDateTime.to_iso8601(dt)}'"
  defp quote_value(%DateTime{} = dt), do: "TIMESTAMP '#{DateTime.to_iso8601(dt)}'"
  defp quote_value(value) when is_integer(value), do: Integer.to_string(value)
  defp quote_value(value) when is_float(value), do: Float.to_string(value)

  defp quote_value(value) when is_binary(value) do
    escaped = String.replace(value, "'", "''")
    "'#{escaped}'"
  end

  defp quote_value(value) do
    escaped = value |> to_string() |> String.replace("'", "''")
    "'#{escaped}'"
  end
end
