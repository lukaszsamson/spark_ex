defmodule SparkEx.TableArg do
  @moduledoc """
  Minimal table-argument wrapper for DataFrame subquery usage.
  """

  alias SparkEx.Column

  defstruct plan: nil, partition_spec: [], order_spec: [], with_single_partition: nil

  @type t :: %__MODULE__{
          plan: term(),
          partition_spec: [term()],
          order_spec: [term()],
          with_single_partition: boolean() | nil
        }

  @spec partition_by(t(), Column.t() | String.t() | atom() | [Column.t() | String.t() | atom()]) ::
          t()
  def partition_by(%__MODULE__{} = arg, cols) do
    %{arg | partition_spec: normalize_cols(cols)}
  end

  @spec order_by(t(), Column.t() | String.t() | atom() | [Column.t() | String.t() | atom()]) ::
          t()
  def order_by(%__MODULE__{} = arg, cols) do
    %{arg | order_spec: Enum.map(List.wrap(cols), &normalize_sort_expr/1)}
  end

  @spec with_single_partition(t()) :: t()
  def with_single_partition(%__MODULE__{} = arg) do
    %{arg | with_single_partition: true}
  end

  defp normalize_cols(cols) do
    cols
    |> List.wrap()
    |> Enum.map(fn
      %Column{} = c -> c.expr
      name when is_binary(name) -> {:col, name}
      name when is_atom(name) -> {:col, Atom.to_string(name)}
    end)
  end

  defp normalize_sort_expr(%Column{expr: {:sort_order, _, _, _} = expr}), do: expr
  defp normalize_sort_expr(%Column{} = c), do: {:sort_order, c.expr, :asc, nil}
  defp normalize_sort_expr(name) when is_binary(name), do: {:sort_order, {:col, name}, :asc, nil}

  defp normalize_sort_expr(name) when is_atom(name),
    do: {:sort_order, {:col, Atom.to_string(name)}, :asc, nil}
end
