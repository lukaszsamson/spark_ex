defmodule SparkEx.WindowSpec do
  @moduledoc """
  Represents a window specification for window functions.

  A `WindowSpec` defines partitioning, ordering, and frame boundaries for
  window functions used with `SparkEx.Column.over/2`.

  ## Examples

      import SparkEx.Functions, only: [col: 1]

      spec =
        SparkEx.Window.partition_by(["dept"])
        |> SparkEx.WindowSpec.order_by(["salary"])
        |> SparkEx.WindowSpec.rows_between(-1, 1)

      col("salary") |> SparkEx.Functions.row_number() |> SparkEx.Column.over(spec)
  """

  alias SparkEx.Column

  defstruct partition_spec: [], order_spec: [], frame_spec: nil

  @type boundary :: :unbounded | :current_row | integer()

  @type frame_spec ::
          nil
          | {:rows, boundary(), boundary()}
          | {:range, boundary(), boundary()}

  @type t :: %__MODULE__{
          partition_spec: [Column.expr()],
          order_spec: [Column.expr()],
          frame_spec: frame_spec()
        }

  @doc "Adds partition-by columns to the window specification."
  @spec partition_by(t(), [Column.t() | String.t()]) :: t()
  def partition_by(%__MODULE__{} = spec, cols) when is_list(cols) do
    %__MODULE__{spec | partition_spec: Enum.map(cols, &to_expr/1)}
  end

  @doc "Adds order-by columns to the window specification."
  @spec order_by(t(), [Column.t() | String.t()]) :: t()
  def order_by(%__MODULE__{} = spec, cols) when is_list(cols) do
    order_exprs =
      Enum.map(cols, fn
        %Column{expr: {:sort_order, _, _, _}} = c -> c.expr
        other -> {:sort_order, to_expr(other), :asc, :nulls_first}
      end)

    %__MODULE__{spec | order_spec: order_exprs}
  end

  @doc """
  Defines a row-based window frame between `start` and `end_` boundaries.

  Boundary values:
  - `:unbounded` — unbounded preceding/following
  - `:current_row` — current row
  - negative integer — N rows preceding
  - positive integer — N rows following
  - `0` — current row
  """
  @spec rows_between(t(), boundary(), boundary()) :: t()
  def rows_between(%__MODULE__{} = spec, start, end_) do
    %__MODULE__{spec | frame_spec: {:rows, clamp_boundary(start), clamp_boundary(end_)}}
  end

  @doc """
  Defines a range-based window frame between `start` and `end_` boundaries.

  Boundary values:
  - `:unbounded` — unbounded preceding/following
  - `:current_row` — current row
  - negative integer — N preceding
  - positive integer — N following
  - `0` — current row
  """
  @spec range_between(t(), boundary(), boundary()) :: t()
  def range_between(%__MODULE__{} = spec, start, end_) do
    %__MODULE__{spec | frame_spec: {:range, clamp_boundary(start), clamp_boundary(end_)}}
  end

  # PySpark clamps extreme boundary values to unbounded.
  # _PRECEDING_THRESHOLD = -(1 << 31) + 1, _FOLLOWING_THRESHOLD = (1 << 31) - 1
  import Bitwise, only: [bsl: 2]
  @preceding_threshold -bsl(1, 31) + 1
  @following_threshold bsl(1, 31) - 1

  defp clamp_boundary(value) when is_integer(value) and value <= @preceding_threshold,
    do: :unbounded

  defp clamp_boundary(value) when is_integer(value) and value >= @following_threshold,
    do: :unbounded

  defp clamp_boundary(value), do: value

  defp to_expr(%Column{expr: e}), do: e
  defp to_expr(name) when is_binary(name), do: {:col, name}
  defp to_expr(name) when is_atom(name), do: {:col, Atom.to_string(name)}
end
