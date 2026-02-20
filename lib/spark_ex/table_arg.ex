defmodule SparkEx.TableArg do
  @moduledoc """
  Minimal table-argument wrapper for DataFrame subquery usage.
  """

  alias SparkEx.Column

  defstruct plan: nil,
            partition_spec: [],
            order_spec: [],
            with_single_partition: nil,
            state: :init

  @type t :: %__MODULE__{
          plan: term(),
          partition_spec: [term()],
          order_spec: [term()],
          with_single_partition: boolean() | nil,
          state: :init | :partitioned | :ordered
        }

  @spec partition_by(t(), Column.t() | String.t() | atom() | [Column.t() | String.t() | atom()]) ::
          t()
  def partition_by(%__MODULE__{} = arg, cols) do
    ensure_not_partitioned!(arg, :partition_by)
    %{arg | partition_spec: normalize_cols(cols), state: :partitioned}
  end

  @spec order_by(t(), Column.t() | String.t() | atom() | [Column.t() | String.t() | atom()]) ::
          t()
  def order_by(%__MODULE__{} = arg, cols) do
    ensure_partitioned!(arg, :order_by)

    new_order_spec = Enum.map(List.wrap(cols), &normalize_sort_expr/1)
    %{arg | order_spec: arg.order_spec ++ new_order_spec, state: :ordered}
  end

  @spec with_single_partition(t()) :: t()
  def with_single_partition(%__MODULE__{} = arg) do
    ensure_not_partitioned!(arg, :with_single_partition)
    %{arg | with_single_partition: true, state: :partitioned}
  end

  @spec to_subquery_expr(t()) :: SparkEx.Column.expr()
  def to_subquery_expr(%__MODULE__{} = arg) do
    opts =
      [
        table_arg_options: [
          partition_spec: arg.partition_spec,
          order_spec: arg.order_spec,
          with_single_partition: arg.with_single_partition
        ]
      ]

    {:subquery, :table_arg, arg.plan, opts}
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
  defp normalize_sort_expr(%Column{} = c), do: {:sort_order, c.expr, :asc, :nulls_first}

  defp normalize_sort_expr(name) when is_binary(name),
    do: {:sort_order, {:col, name}, :asc, :nulls_first}

  defp normalize_sort_expr(name) when is_atom(name),
    do: {:sort_order, {:col, Atom.to_string(name)}, :asc, :nulls_first}

  defp ensure_not_partitioned!(%__MODULE__{state: :init}, _op), do: :ok

  defp ensure_not_partitioned!(%__MODULE__{}, op) do
    raise ArgumentError,
          "cannot call #{op} after partition_by/2 or with_single_partition/1 has been called"
  end

  defp ensure_partitioned!(%__MODULE__{state: :partitioned}, _op), do: :ok
  defp ensure_partitioned!(%__MODULE__{state: :ordered}, _op), do: :ok

  defp ensure_partitioned!(%__MODULE__{}, op) do
    raise ArgumentError,
          "cannot call #{op} before partition_by/2 or with_single_partition/1"
  end
end
