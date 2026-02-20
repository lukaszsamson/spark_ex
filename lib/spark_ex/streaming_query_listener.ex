defmodule SparkEx.StreamingQueryListener do
  @moduledoc """
  Behaviour for streaming query event listeners.

  Implement this behaviour to receive events from the
  `SparkEx.StreamingQueryListenerBus`.

  ## Example

      defmodule MyListener do
        @behaviour SparkEx.StreamingQueryListener

        @impl true
        def on_query_progress(event) do
          IO.puts("Query progress: \#{inspect(event.data)}")
        end

        @impl true
        def on_query_terminated(event) do
          IO.puts("Query terminated: \#{inspect(event.data)}")
        end

        @impl true
        def on_query_idle(event) do
          IO.puts("Query idle: \#{inspect(event.data)}")
        end
      end

  ## Event Structure

  Each callback receives a map with:

    * `:type` — `:started`, `:progress`, `:terminated`, or `:idle`
    * `:data` — parsed JSON event data (map)
    * `:raw_json` — the original JSON string from the server
  """

  @type event :: %{
          type: :started | :progress | :terminated | :idle,
          data: map() | String.t(),
          raw_json: String.t()
        }

  @callback on_query_started(event()) :: any()
  @callback on_query_progress(event()) :: any()
  @callback on_query_terminated(event()) :: any()
  @callback on_query_idle(event()) :: any()

  @optional_callbacks on_query_started: 1
end
