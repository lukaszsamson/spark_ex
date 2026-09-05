defmodule SparkEx.Internal.ColumnName do
  @moduledoc false

  # Shared column-name normalization (T-38).
  #
  # PySpark accepts `str` column names everywhere a `ColumnOrName` is expected.
  # SparkEx additionally accepts atoms as a convenience, but the conversion was
  # duplicated (and inconsistently applied) across DataFrame, GroupedData,
  # WindowSpec, NA and Functions. This module is the single implementation:
  #
  #   * binaries pass through unchanged
  #   * atoms are stringified, except `nil` / `true` / `false`, which used to
  #     silently become the column names "nil" / "true" / "false"
  #   * everything else raises `ArgumentError`

  @doc """
  Normalizes a column name to a string.

  Raises `ArgumentError` for `nil`, booleans and any non-name term.
  """
  @spec normalize!(String.t() | atom()) :: String.t()
  def normalize!(name) when is_binary(name), do: name

  def normalize!(name) when name in [nil, true, false] do
    raise ArgumentError,
          "expected a column name (string or atom), got: #{inspect(name)}"
  end

  def normalize!(name) when is_atom(name), do: Atom.to_string(name)

  def normalize!(other) do
    raise ArgumentError,
          "expected a column name (string or atom), got: #{inspect(other)}"
  end

  @doc """
  Normalizes a column name and routes it through Spark's star handling.

  Mirrors `SparkEx.Functions.col/1`: `"*"` / `".*"` become `{:star}` and a
  qualified `"a.b.*"` becomes `{:star, name}` (PySpark's `UnresolvedStar`
  requires the trailing `".*"` in `unparsed_target`).
  """
  @spec to_col_expr(String.t() | atom()) :: {:col, String.t()} | {:star} | {:star, String.t()}
  def to_col_expr(name) do
    case normalize!(name) do
      "*" ->
        {:star}

      ".*" ->
        {:star}

      name ->
        if String.ends_with?(name, ".*") do
          {:star, name}
        else
          {:col, name}
        end
    end
  end
end
