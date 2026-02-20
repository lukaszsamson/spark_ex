defmodule SparkEx.ProgressHandlerRegistry do
  @moduledoc """
  Registry for session-scoped progress handlers driven by telemetry events.
  """

  @table :spark_ex_progress_handlers
  @event [:spark_ex, :result, :progress]

  @spec register(String.t(), (map() -> any())) :: :ok
  def register(session_id, handler)
      when is_binary(session_id) and is_function(handler, 1) do
    handler_id = {__MODULE__, session_id, make_ref()}

    :telemetry.attach(handler_id, @event, &__MODULE__.dispatch/4, %{
      session_id: session_id,
      handler: handler
    })

    :ets.insert(@table, {session_id, handler_id, handler})
    :ok
  end

  @spec remove(String.t(), (map() -> any())) :: :ok
  def remove(session_id, handler)
      when is_binary(session_id) and is_function(handler, 1) do
    case find_handler_entry(session_id, handler) do
      {:ok, {^session_id, handler_id, ^handler}} ->
        :telemetry.detach(handler_id)
        :ets.delete_object(@table, {session_id, handler_id, handler})
        :ok

      :error ->
        :ok
    end
  end

  @spec clear(String.t()) :: :ok
  def clear(session_id) when is_binary(session_id) do
    entries = :ets.lookup(@table, session_id)

    Enum.each(entries, fn {^session_id, handler_id, _handler} ->
      :telemetry.detach(handler_id)
    end)

    :ets.delete(@table, session_id)
    :ok
  end

  @doc false
  def dispatch(event, measurements, metadata, %{session_id: session_id, handler: handler}) do
    if Map.get(metadata, :session_id) == session_id do
      invoke(handler, %{event: event, measurements: measurements, metadata: metadata})
    end

    :ok
  end

  defp invoke(handler, payload) do
    handler.(payload)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end

  defp find_handler_entry(session_id, handler) do
    entries = :ets.lookup(@table, session_id)

    case Enum.find(entries, fn {^session_id, _handler_id, stored} -> stored == handler end) do
      nil -> :error
      entry -> {:ok, entry}
    end
  end
end
