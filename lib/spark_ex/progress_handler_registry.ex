defmodule SparkEx.ProgressHandlerRegistry do
  @moduledoc """
  Registry for session-scoped progress handlers driven by telemetry events.

  ## Idempotency

  When called without an `:id` option, `register/3` and `remove/3` deduplicate
  by *function term equality*. Two anonymous functions defined in different
  places (or across code reloads) compare unequal even when their source is
  identical, so re-registering after a reload will produce a duplicate entry.

  Pass `id: term` to deduplicate deterministically:

      SparkEx.ProgressHandlerRegistry.register(session_id, handler, id: :my_handler)
      SparkEx.ProgressHandlerRegistry.remove(session_id, handler, id: :my_handler)

  Identifiers are scoped per session.
  """

  require Logger

  @table :spark_ex_progress_handlers
  @event [:spark_ex, :result, :progress]

  @type handler :: (map() -> any())
  @type opts :: [id: term()]

  @spec register(String.t(), handler()) :: :ok
  def register(session_id, handler), do: register(session_id, handler, [])

  @spec register(String.t(), handler(), opts()) :: :ok
  def register(session_id, handler, opts)
      when is_binary(session_id) and is_function(handler, 1) and is_list(opts) do
    id = Keyword.get(opts, :id)

    case find_handler_entry(session_id, handler, id) do
      {:ok, _} ->
        :ok

      :error ->
        handler_id = {__MODULE__, session_id, make_ref()}

        :telemetry.attach(handler_id, @event, &__MODULE__.dispatch/4, %{
          session_id: session_id,
          handler: handler
        })

        :ets.insert(@table, {session_id, handler_id, handler, id})
        :ok
    end
  end

  @spec remove(String.t(), handler()) :: :ok
  def remove(session_id, handler), do: remove(session_id, handler, [])

  @spec remove(String.t(), handler(), opts()) :: :ok
  def remove(session_id, handler, opts)
      when is_binary(session_id) and is_function(handler, 1) and is_list(opts) do
    id = Keyword.get(opts, :id)

    case find_handler_entry(session_id, handler, id) do
      {:ok, {^session_id, handler_id, stored_handler, stored_id}} ->
        :telemetry.detach(handler_id)
        :ets.delete_object(@table, {session_id, handler_id, stored_handler, stored_id})
        :ok

      :error ->
        :ok
    end
  end

  @spec clear(String.t()) :: :ok
  def clear(session_id) when is_binary(session_id) do
    entries = :ets.lookup(@table, session_id)

    Enum.each(entries, fn entry ->
      handler_id = elem(entry, 1)
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
    e ->
      Logger.warning(
        "Progress handler callback failed: #{Exception.format(:error, e, __STACKTRACE__)}"
      )

      :ok
  catch
    kind, reason ->
      Logger.warning("Progress handler callback failed: #{inspect({kind, reason})}")
      :ok
  end

  defp find_handler_entry(session_id, handler, id) do
    entries = :ets.lookup(@table, session_id)

    matcher =
      if is_nil(id) do
        fn {^session_id, _handler_id, stored, _stored_id} -> stored == handler end
      else
        fn {^session_id, _handler_id, _stored, stored_id} -> stored_id == id end
      end

    case Enum.find(entries, matcher) do
      nil -> :error
      entry -> {:ok, entry}
    end
  end
end
