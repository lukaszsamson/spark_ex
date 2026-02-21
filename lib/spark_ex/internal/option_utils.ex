defmodule SparkEx.Internal.OptionUtils do
  @moduledoc false

  @doc """
  Converts an option value to string. Accepts strings, integers, floats, and booleans.

  Raises `ArgumentError` for unsupported types.
  """
  @spec normalize_option_value(term()) :: String.t()
  def normalize_option_value(value) when is_binary(value), do: value
  def normalize_option_value(value) when is_integer(value), do: Integer.to_string(value)
  def normalize_option_value(value) when is_float(value), do: Float.to_string(value)
  def normalize_option_value(value) when is_boolean(value), do: to_string(value)

  def normalize_option_value(value) do
    raise ArgumentError,
          "option value must be a primitive (string, integer, float, boolean), got: #{inspect(value)}"
  end

  @doc """
  Converts a map or keyword list to a string-keyed, string-valued map.

  Nil values are kept. Use `stringify_options_reject_nil/1` to reject them.
  """
  @spec stringify_options(map() | keyword()) :: %{String.t() => String.t()}
  def stringify_options(opts) when is_map(opts) do
    Map.new(opts, fn {k, v} -> {to_string(k), normalize_option_value(v)} end)
  end

  def stringify_options(opts) when is_list(opts) do
    opts |> Enum.into(%{}) |> stringify_options()
  end

  @doc """
  Like `stringify_options/1` but rejects nil values.
  """
  @spec stringify_options_reject_nil(map() | keyword()) :: %{String.t() => String.t()}
  def stringify_options_reject_nil(opts) when is_map(opts) do
    opts
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new(fn {k, v} -> {to_string(k), normalize_option_value(v)} end)
  end

  def stringify_options_reject_nil(opts) when is_list(opts) do
    opts |> Enum.into(%{}) |> stringify_options_reject_nil()
  end
end
