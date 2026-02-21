defmodule SparkEx.SqlFormatter do
  @moduledoc """
  Formats SQL strings with auto-quoting of parameters.

  Performs token-aware replacement that respects SQL string literals
  (single-quoted strings). Placeholders inside string literals are
  left untouched.

  ## Positional parameters

      SparkEx.SqlFormatter.format("SELECT ? AS a, ? AS b", [1, 2])
      #=> "SELECT 1 AS a, 2 AS b"

  ## Named parameters

      SparkEx.SqlFormatter.format("SELECT :id AS id", %{id: 42})
      #=> "SELECT 42 AS id"

  Raises `ArgumentError` when:
  - There are more `?` placeholders than positional args
  - There are more positional args than `?` placeholders
  - Named placeholders remain unresolved after substitution
  - Extra named args are provided that don't match any placeholder
  """

  @spec format(String.t(), list() | map() | nil) :: String.t()
  def format(sql, nil) when is_binary(sql), do: sql

  def format(sql, args) when is_binary(sql) and is_list(args) do
    {result, rest} = format_positional(sql, args)

    if rest != [] do
      raise ArgumentError, "not enough placeholders for positional args"
    end

    IO.iodata_to_binary(result)
  end

  def format(sql, args) when is_binary(sql) and is_map(args) and not is_struct(args) do
    format_named(sql, args)
  end

  def format(sql, args) do
    raise ArgumentError,
          "expected sql as string and args as list, map, or nil, got: #{inspect({sql, args})}"
  end

  # Single-pass tokenizer for positional args that skips string literals
  defp format_positional(sql, args) do
    do_format_positional(sql, args, [])
  end

  defp do_format_positional(<<>>, args, acc) do
    {Enum.reverse(acc), args}
  end

  # Single-quoted string literal — skip contents
  defp do_format_positional(<<"'", rest::binary>>, args, acc) do
    {literal, remaining} = consume_string_literal(rest, ["'"])
    do_format_positional(remaining, args, [literal | acc])
  end

  # Placeholder
  defp do_format_positional(<<"?", rest::binary>>, [value | tail], acc) do
    do_format_positional(rest, tail, [quote_value(value) | acc])
  end

  defp do_format_positional(<<"?", _rest::binary>>, [], _acc) do
    raise ArgumentError, "not enough args for positional placeholders"
  end

  # Regular character
  defp do_format_positional(<<ch::utf8, rest::binary>>, args, acc) do
    do_format_positional(rest, args, [<<ch::utf8>> | acc])
  end

  # Consume a single-quoted string literal (handling escaped '' inside)
  defp consume_string_literal(<<"''", rest::binary>>, acc) do
    consume_string_literal(rest, ["''" | acc])
  end

  defp consume_string_literal(<<"'", rest::binary>>, acc) do
    {IO.iodata_to_binary(Enum.reverse(["'" | acc])), rest}
  end

  defp consume_string_literal(<<ch::utf8, rest::binary>>, acc) do
    consume_string_literal(rest, [<<ch::utf8>> | acc])
  end

  defp consume_string_literal(<<>>, acc) do
    # Unterminated string literal — return what we have
    {IO.iodata_to_binary(Enum.reverse(acc)), <<>>}
  end

  # Single-pass tokenizer for named args that skips string literals
  defp format_named(sql, args) do
    string_keys = Map.new(args, fn {k, v} -> {to_string(k), v} end)
    {result, used_keys} = do_format_named(sql, string_keys, [], %{})

    result_str = IO.iodata_to_binary(Enum.reverse(result))

    # Check for unresolved placeholders (skipping string literals)
    remaining = scan_unresolved_placeholders(result_str)

    if remaining != [] do
      raise ArgumentError,
            "unresolved named placeholders: #{inspect(remaining)}"
    end

    # Check for unused args
    unused = Map.keys(string_keys) -- Map.keys(used_keys)

    if unused != [] do
      raise ArgumentError,
            "unused named args: #{inspect(unused)}"
    end

    result_str
  end

  defp do_format_named(<<>>, _args, acc, used) do
    {acc, used}
  end

  # Single-quoted string literal — skip contents
  defp do_format_named(<<"'", rest::binary>>, args, acc, used) do
    {literal, remaining} = consume_string_literal(rest, ["'"])
    do_format_named(remaining, args, [literal | acc], used)
  end

  # Named parameter — :word_chars
  defp do_format_named(<<":", rest::binary>>, args, acc, used) do
    {name, remaining} = consume_identifier(rest, [])

    if name != "" and Map.has_key?(args, name) do
      value = Map.fetch!(args, name)
      do_format_named(remaining, args, [quote_value(value) | acc], Map.put(used, name, true))
    else
      do_format_named(remaining, args, [name, ":" | acc], used)
    end
  end

  # Regular character
  defp do_format_named(<<ch::utf8, rest::binary>>, args, acc, used) do
    do_format_named(rest, args, [<<ch::utf8>> | acc], used)
  end

  # Scans for unresolved :name placeholders, skipping string literals
  defp scan_unresolved_placeholders(sql), do: do_scan_unresolved(sql, [])

  defp do_scan_unresolved(<<>>, acc), do: Enum.reverse(acc)

  defp do_scan_unresolved(<<"'", rest::binary>>, acc) do
    {_literal, remaining} = consume_string_literal(rest, ["'"])
    do_scan_unresolved(remaining, acc)
  end

  defp do_scan_unresolved(<<":", rest::binary>>, acc) do
    {name, remaining} = consume_identifier(rest, [])

    if name != "" do
      do_scan_unresolved(remaining, [name | acc])
    else
      do_scan_unresolved(remaining, acc)
    end
  end

  defp do_scan_unresolved(<<_::utf8, rest::binary>>, acc) do
    do_scan_unresolved(rest, acc)
  end

  defp consume_identifier(<<ch::utf8, rest::binary>>, acc)
       when (ch >= ?a and ch <= ?z) or (ch >= ?A and ch <= ?Z) or
              (ch >= ?0 and ch <= ?9) or ch == ?_ do
    consume_identifier(rest, [<<ch::utf8>> | acc])
  end

  defp consume_identifier(rest, acc) do
    {IO.iodata_to_binary(Enum.reverse(acc)), rest}
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
