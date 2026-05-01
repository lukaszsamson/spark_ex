defmodule SparkEx.Connect.UDTRegistry do
  @moduledoc """
  Registry for User-Defined Type (UDT) deserialization callbacks.

  Spark Connect UDT columns arrive at the client as raw Arrow values
  (typically struct/binary, depending on the underlying `sql_type`).
  By default the decoder passes those values through unchanged so
  downstream code can handle them. Applications that want to rehydrate
  UDT values into domain structs can register a 1-arity callback per
  UDT class and the result decoder will invoke it for every cell of a
  matching column.

  Lookup precedence — the first match wins, in order:

    1. `:jvm_class`
    2. `:python_class`
    3. `:type` (the UDT's logical type name)

  The default behaviour is unchanged: when no callback is registered
  the raw value is returned as-is.

  ## Examples

      SparkEx.Connect.UDTRegistry.register("org.example.PointUDT", fn raw ->
        # Raw is the Arrow-decoded value (e.g. a list of doubles for a
        # SQL ARRAY<DOUBLE> sql_type). Convert to your own struct.
        {:ok, [x, y]} = {:ok, raw}
        %MyApp.Point{x: x, y: y}
      end)
  """

  @table :spark_ex_udt_deserializers

  @typedoc "A UDT deserialization callback applied to each cell value."
  @type deserializer :: (term() -> term())

  @doc false
  @spec ensure_table() :: :ok
  def ensure_table do
    if :ets.whereis(@table) == :undefined do
      try do
        :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  @doc """
  Registers a deserialization callback for a UDT class name.

  `class_name` should match one of the strings the server sends in the
  UDT proto: a JVM class name (e.g. `"org.apache.spark.ml.linalg.VectorUDT"`),
  a Python class name, or the logical `type` field. The same callback can
  be registered under multiple names by calling this function repeatedly.
  """
  @spec register(String.t(), deserializer()) :: :ok
  def register(class_name, fun) when is_binary(class_name) and is_function(fun, 1) do
    ensure_table()
    :ets.insert(@table, {class_name, fun})
    :ok
  end

  @doc """
  Removes a previously-registered UDT deserializer.
  """
  @spec unregister(String.t()) :: :ok
  def unregister(class_name) when is_binary(class_name) do
    ensure_table()
    :ets.delete(@table, class_name)
    :ok
  end

  @doc false
  @spec lookup_deserializer(Spark.Connect.DataType.UDT.t()) :: deserializer() | nil
  def lookup_deserializer(%Spark.Connect.DataType.UDT{} = udt) do
    ensure_table()

    [udt.jvm_class, udt.python_class, udt.type]
    |> Enum.reject(&(is_nil(&1) or &1 == ""))
    |> Enum.find_value(fn name ->
      case :ets.lookup(@table, name) do
        [{^name, fun}] when is_function(fun, 1) -> fun
        _ -> nil
      end
    end)
  end
end
