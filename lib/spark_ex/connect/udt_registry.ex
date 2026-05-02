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

  ## Multi-tenancy

  The registry is process-global — there is a single ETS table shared by
  every `SparkEx.Session` running in the same VM. Re-registering a class
  name from one session changes the deserializer that every other session
  observes. If you run multiple sessions against different Spark clusters
  with conflicting UDT classes, register a different callback under each
  cluster's logical `type` field rather than its `jvm_class`, or wrap your
  registrations so different sessions cannot clobber each other.

  Registering the same `(class_name, fun)` pair twice is a no-op. Replacing
  an existing callback with a different function emits a warning at runtime
  to surface the multi-tenant clobber risk; pass `replace?: true` to
  `register/3` to opt out of the warning when the replacement is intentional.

  ## Examples

      SparkEx.Connect.UDTRegistry.register("org.example.PointUDT", fn raw ->
        # Raw is the Arrow-decoded value (e.g. a list of doubles for a
        # SQL ARRAY<DOUBLE> sql_type). Convert to your own struct.
        {:ok, [x, y]} = {:ok, raw}
        %MyApp.Point{x: x, y: y}
      end)
  """

  require Logger

  @table :spark_ex_udt_deserializers

  @typedoc "A UDT deserialization callback applied to each cell value."
  @type deserializer :: (term() -> term())

  @doc false
  @spec ensure_table() :: :ok
  def ensure_table do
    # Owned by `SparkEx.EtsTableOwner` so the table (and all registrations)
    # survive for the lifetime of the application rather than dying with
    # whichever process happened to call `register/2` first.
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
  end

  @doc """
  Registers a deserialization callback for a UDT class name.

  `class_name` should match one of the strings the server sends in the
  UDT proto: a JVM class name (e.g. `"org.apache.spark.ml.linalg.VectorUDT"`),
  a Python class name, or the logical `type` field. The same callback can
  be registered under multiple names by calling this function repeatedly.

  ## Options

  - `:replace?` — when `true`, suppress the warning emitted when an existing
    registration for `class_name` is overwritten with a different function.
    Defaults to `false` so accidental cross-session clobbers are visible.
  """
  @spec register(String.t(), deserializer(), keyword()) :: :ok
  def register(class_name, fun, opts \\ [])
      when is_binary(class_name) and is_function(fun, 1) and is_list(opts) do
    ensure_table()
    replace? = Keyword.get(opts, :replace?, false)

    case :ets.lookup(@table, class_name) do
      [{^class_name, ^fun}] ->
        :ok

      [{^class_name, existing}] when is_function(existing, 1) and not replace? ->
        Logger.warning(fn ->
          "SparkEx.Connect.UDTRegistry: replacing existing deserializer for #{inspect(class_name)} " <>
            "with a different function. The registry is process-global and shared across all " <>
            "SparkEx.Session instances; pass `replace?: true` to silence this warning if the " <>
            "replacement is intentional."
        end)

        :ets.insert(@table, {class_name, fun})
        :ok

      _ ->
        :ets.insert(@table, {class_name, fun})
        :ok
    end
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
