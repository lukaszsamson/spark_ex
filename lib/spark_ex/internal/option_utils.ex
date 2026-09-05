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

  # Only plain lowercase snake_case atoms are camelized. Anything with a dot,
  # dash, uppercase letter, or a leading/trailing underscore is a key from a
  # case-sensitive pass-through namespace (JDBC connection properties,
  # `kafka.*`, `oracle.net.*`, …) and must survive verbatim.
  @snake_case ~r/^[a-z][a-z0-9]*(_[a-z0-9]+)+$/

  @doc """
  Normalizes an option key to the name Spark expects on the wire.

  Atom keys that are plain `snake_case` are converted to the `camelCase` names
  Spark's data sources use (`:multi_line` -> `"multiLine"`, `:infer_schema` ->
  `"inferSchema"`). Every other atom, and every string key, is passed through
  as-is — so `:"oracle.net.CONNECT_TIMEOUT"`, `:CLIENT_SESSION_KEEP_ALIVE` and
  `"multi_line"` all reach the server unchanged.
  """
  @spec normalize_option_key(atom() | String.t()) :: String.t()
  def normalize_option_key(key) when is_binary(key), do: key

  def normalize_option_key(key) when is_atom(key) and not is_nil(key) and not is_boolean(key) do
    string = Atom.to_string(key)

    if Regex.match?(@snake_case, string), do: camelize(string), else: string
  end

  def normalize_option_key(key) do
    raise ArgumentError, "option key must be a string or atom, got: #{inspect(key)}"
  end

  @doc """
  Key normalizer for pass-through namespaces (JDBC connection properties,
  table properties): stringify without any camelization.
  """
  @spec verbatim_option_key(atom() | String.t()) :: String.t()
  def verbatim_option_key(key) when is_binary(key), do: key

  def verbatim_option_key(key) when is_atom(key) and not is_nil(key) and not is_boolean(key),
    do: Atom.to_string(key)

  def verbatim_option_key(key) do
    raise ArgumentError, "option key must be a string or atom, got: #{inspect(key)}"
  end

  defp camelize(string) do
    case String.split(string, "_") do
      [single] ->
        single

      [head | rest] ->
        head <> Enum.map_join(rest, &upcase_first/1)
    end
  end

  defp upcase_first(""), do: ""

  defp upcase_first(segment) do
    {first, rest} = String.split_at(segment, 1)
    String.upcase(first) <> rest
  end

  @doc """
  Collapses a keyword list or map of options into a plain map without
  touching keys or values. Used to accept both shapes for `:options`.
  """
  @spec to_option_map(map() | keyword() | nil) :: map()
  def to_option_map(nil), do: %{}
  def to_option_map(opts) when is_map(opts), do: opts

  def to_option_map(opts) when is_list(opts) do
    valid? =
      Enum.all?(opts, fn
        {k, _v} -> is_atom(k) or is_binary(k)
        _other -> false
      end)

    if valid? do
      Map.new(opts)
    else
      raise ArgumentError,
            "options must be a map or a list of {key, value} pairs, got: #{inspect(opts)}"
    end
  end

  def to_option_map(opts) do
    raise ArgumentError, "options must be a map or keyword list, got: #{inspect(opts)}"
  end

  @doc """
  Converts a map or keyword list to a string-keyed, string-valued map.

  Keys are normalized via `normalize_option_key/1`.

  Raises `ArgumentError` for nil values (via `normalize_option_value/1`).
  Use `stringify_options_reject_nil/1` if you want nils filtered out.
  """
  @spec stringify_options(map() | keyword(), (atom() | String.t() -> String.t())) ::
          %{String.t() => String.t()}
  def stringify_options(opts, key_fun \\ &normalize_option_key/1) do
    opts
    |> to_option_map()
    |> Map.new(fn {k, v} -> {key_fun.(k), normalize_option_value(v)} end)
  end

  @doc """
  Like `stringify_options/1` but silently drops entries whose value is `nil`.
  Non-nil values are still validated by `normalize_option_value/1`.
  """
  @spec stringify_options_reject_nil(map() | keyword(), (atom() | String.t() -> String.t())) ::
          %{String.t() => String.t()}
  def stringify_options_reject_nil(opts, key_fun \\ &normalize_option_key/1) do
    opts
    |> to_option_map()
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    |> Map.new(fn {k, v} -> {key_fun.(k), normalize_option_value(v)} end)
  end

  @doc """
  Merges call-time top-level options with the nested `:options` map, raising
  when the same normalized key is supplied through both routes.
  """
  @spec merge_exclusive!(%{String.t() => String.t()}, %{String.t() => String.t()}) ::
          %{String.t() => String.t()}
  def merge_exclusive!(top_level_options, nested_options) do
    # Compare spelling-insensitively so `multi_line:` + `%{"multi_line" => ...}`
    # is still caught even though only the atom side gets camelized.
    nested_canonical = MapSet.new(Map.keys(nested_options), &canonical_key/1)

    duplicates =
      top_level_options
      |> Map.keys()
      |> Enum.filter(&MapSet.member?(nested_canonical, canonical_key(&1)))
      |> Enum.sort()

    if duplicates != [] do
      raise ArgumentError,
            "got multiple values for keyword argument(s) " <>
              Enum.map_join(duplicates, ", ", &inspect/1) <>
              " — pass each option either as a top-level keyword OR inside :options, not both"
    end

    Map.merge(top_level_options, nested_options)
  end

  defp canonical_key(key) do
    key |> String.replace("_", "") |> String.downcase()
  end

  @doc """
  Shared implementation of the `merge_source_options/2` used by the reader,
  writer and streaming builders: normalize the nested `:options` value and the
  remaining top-level keywords (minus `reserved_keys`), then merge them with a
  duplicate check.
  """
  @spec merge_source_options(keyword(), [atom()]) :: %{String.t() => String.t()}
  def merge_source_options(opts, reserved_keys) when is_list(opts) and is_list(reserved_keys) do
    nested_options = opts |> Keyword.get(:options) |> stringify_options_reject_nil()

    top_level_options =
      opts
      |> Keyword.drop([:options | reserved_keys])
      |> stringify_options_reject_nil()

    merge_exclusive!(top_level_options, nested_options)
  end
end
