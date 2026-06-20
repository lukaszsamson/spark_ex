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

  @type boundary ::
          :unbounded
          | :unbounded_preceding
          | :unbounded_following
          | :current_row
          | integer()

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
  @spec partition_by(t(), [Column.t() | String.t() | atom()]) :: t()
  def partition_by(%__MODULE__{} = spec, cols) when is_list(cols) do
    %__MODULE__{spec | partition_spec: Enum.map(cols, &to_expr/1)}
  end

  @doc "Adds order-by columns to the window specification."
  @spec order_by(t(), [Column.t() | String.t() | atom()]) :: t()
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

  Boundary values for `start` (lower bound):
  - `:unbounded_preceding` — frame starts at the partition's first row
  - `:unbounded` — legacy alias resolved to `:unbounded_preceding`
  - `:current_row` (or `0`) — frame starts at the current row
  - negative integer — N rows preceding the current row
  - positive integer — N rows following the current row

  Boundary values for `end_` (upper bound):
  - `:unbounded_following` — frame ends at the partition's last row
  - `:unbounded` — legacy alias resolved to `:unbounded_following`
  - `:current_row` (or `0`) — frame ends at the current row
  - negative integer — N rows preceding the current row
  - positive integer — N rows following the current row

  Passing `:unbounded_following` as `start` or `:unbounded_preceding` as `end_`
  raises `ArgumentError`; the directional sentinels can only be used in their
  matching position.
  """
  @spec rows_between(t(), boundary(), boundary()) :: t()
  def rows_between(%__MODULE__{} = spec, start, end_) do
    %__MODULE__{
      spec
      | frame_spec: {:rows, clamp_boundary(start, :lower), clamp_boundary(end_, :upper)}
    }
  end

  @doc """
  Defines a range-based window frame between `start` and `end_` boundaries.

  Same boundary rules as `rows_between/3`: `:unbounded_preceding` is only valid
  as the lower bound, `:unbounded_following` is only valid as the upper bound,
  `:unbounded` is a legacy alias resolved by position, and integer offsets are
  measured in the order-by column's units.
  """
  @spec range_between(t(), boundary(), boundary()) :: t()
  def range_between(%__MODULE__{} = spec, start, end_) do
    %__MODULE__{
      spec
      | frame_spec: {:range, clamp_boundary(start, :lower), clamp_boundary(end_, :upper)}
    }
  end

  # PySpark clamps extreme boundary values to unbounded using its threshold
  # sentinels (pyspark/sql/window.py:59-60):
  #   _PRECEDING_THRESHOLD = max(-sys.maxsize, JVM_LONG_MIN)
  #   _FOLLOWING_THRESHOLD = min(sys.maxsize, JVM_LONG_MAX)
  # On a 64-bit runtime sys.maxsize == 2^63 - 1, so the preceding threshold is
  # -(2^63 - 1) (one greater than JVM_LONG_MIN) and the following threshold is
  # 2^63 - 1 (== JVM_LONG_MAX). A lower bound <= -(2^63 - 1) becomes
  # unbounded_preceding; an upper bound >= 2^63 - 1 becomes unbounded_following.
  import Bitwise, only: [bsl: 2]
  # Written as `1 - bsl(...)` because `@attr -(expr)` parses as a binary
  # subtraction against the undefined attribute on Elixir < 1.20.
  @preceding_threshold 1 - bsl(1, 63)
  @following_threshold bsl(1, 63) - 1

  defp clamp_boundary(:unbounded_following, :lower) do
    raise ArgumentError,
          ":unbounded_following cannot be used as the lower frame bound; " <>
            "use :unbounded_preceding, :current_row, or an integer offset"
  end

  defp clamp_boundary(:unbounded_preceding, :upper) do
    raise ArgumentError,
          ":unbounded_preceding cannot be used as the upper frame bound; " <>
            "use :unbounded_following, :current_row, or an integer offset"
  end

  defp clamp_boundary(value, _position)
       when value in [:unbounded_preceding, :unbounded_following],
       do: value

  defp clamp_boundary(:unbounded, :lower), do: :unbounded_preceding
  defp clamp_boundary(:unbounded, :upper), do: :unbounded_following

  defp clamp_boundary(value, _position) when is_integer(value) and value <= @preceding_threshold,
    do: :unbounded_preceding

  defp clamp_boundary(value, _position) when is_integer(value) and value >= @following_threshold,
    do: :unbounded_following

  defp clamp_boundary(value, _position), do: value

  defp to_expr(%Column{expr: e}), do: e
  defp to_expr(name) when is_binary(name), do: {:col, name}
  defp to_expr(name) when is_atom(name), do: {:col, Atom.to_string(name)}
end
