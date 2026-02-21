defmodule SparkEx.EtsTableOwner do
  @moduledoc false

  use GenServer

  @tables [
    {:spark_ex_retry_policies, :set},
    {:spark_ex_user_context_extensions, :set},
    {:spark_ex_observations, :set},
    {:spark_ex_progress_handlers, :bag},
    {:spark_ex_streaming_listener_buses, :bag}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, :ok, Keyword.put_new(opts, :name, __MODULE__))
  end

  @spec ensure_table!(atom(), :set | :bag) :: :ok
  def ensure_table!(table, type) when type in [:set, :bag] do
    case :ets.whereis(table) do
      :undefined ->
        try do
          :ets.new(table, [:named_table, :public, type])
          :ok
        rescue
          ArgumentError ->
            # Race condition: another process may have created the table.
            # Verify it now exists; if not, re-raise.
            case :ets.whereis(table) do
              :undefined ->
                raise ArgumentError,
                      "failed to create ETS table #{inspect(table)} and it does not exist"

              _tid ->
                :ok
            end
        end

      _tid ->
        :ok
    end
  end

  @impl true
  def init(:ok) do
    Enum.each(@tables, fn {table, type} ->
      ensure_table!(table, type)
    end)

    {:ok, %{}}
  end
end
