defmodule SparkEx.Window do
  @moduledoc """
  Convenience constructors for window specifications.

  Provides top-level entry points for building `SparkEx.WindowSpec` structs
  used with `SparkEx.Column.over/2` for window functions.

  ## Examples

      import SparkEx.Functions, only: [col: 1]

      w = SparkEx.Window.partition_by(["dept"]) |> SparkEx.WindowSpec.order_by(["salary"])
      SparkEx.Functions.row_number() |> SparkEx.Column.over(w)

      w = SparkEx.Window.order_by([SparkEx.Column.desc(col("ts"))])
      SparkEx.Functions.lag("value", offset: 1) |> SparkEx.Column.over(w)
  """

  alias SparkEx.WindowSpec

  @doc "Creates a window spec partitioned by the given columns."
  @spec partition_by([SparkEx.Column.t() | String.t()]) :: WindowSpec.t()
  def partition_by(cols) when is_list(cols) do
    WindowSpec.partition_by(%WindowSpec{}, cols)
  end

  @doc "Creates a window spec ordered by the given columns."
  @spec order_by([SparkEx.Column.t() | String.t()]) :: WindowSpec.t()
  def order_by(cols) when is_list(cols) do
    WindowSpec.order_by(%WindowSpec{}, cols)
  end

  @doc "Adds order-by columns to an existing window spec."
  @spec order_by(WindowSpec.t(), [SparkEx.Column.t() | String.t()]) :: WindowSpec.t()
  def order_by(%WindowSpec{} = spec, cols) when is_list(cols) do
    WindowSpec.order_by(spec, cols)
  end

  @doc "Creates a window spec with a row-based frame."
  @spec rows_between(WindowSpec.boundary(), WindowSpec.boundary()) :: WindowSpec.t()
  def rows_between(start, end_) do
    WindowSpec.rows_between(%WindowSpec{}, start, end_)
  end

  @doc "Adds a row-based frame to an existing window spec."
  @spec rows_between(WindowSpec.t(), WindowSpec.boundary(), WindowSpec.boundary()) :: WindowSpec.t()
  def rows_between(%WindowSpec{} = spec, start, end_) do
    WindowSpec.rows_between(spec, start, end_)
  end

  @doc "Creates a window spec with a range-based frame."
  @spec range_between(WindowSpec.boundary(), WindowSpec.boundary()) :: WindowSpec.t()
  def range_between(start, end_) do
    WindowSpec.range_between(%WindowSpec{}, start, end_)
  end

  @doc "Adds a range-based frame to an existing window spec."
  @spec range_between(WindowSpec.t(), WindowSpec.boundary(), WindowSpec.boundary()) :: WindowSpec.t()
  def range_between(%WindowSpec{} = spec, start, end_) do
    WindowSpec.range_between(spec, start, end_)
  end

  @doc "Unbounded preceding boundary constant."
  def unbounded_preceding, do: :unbounded

  @doc "Unbounded following boundary constant."
  def unbounded_following, do: :unbounded

  @doc "Current row boundary constant."
  def current_row, do: :current_row
end
