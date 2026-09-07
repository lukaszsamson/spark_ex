defmodule SparkEx.CallSite do
  @moduledoc false

  alias Spark.Connect.FetchErrorDetailsResponse.{Error, StackTraceElement}

  @scope {__MODULE__, :trace}
  @type_url "type.googleapis.com/spark.connect.FetchErrorDetailsResponse.Error"

  def capture do
    if enabled?() do
      {:current_stacktrace, frames} = Process.info(self(), :current_stacktrace)

      stack =
        frames
        |> Enum.reject(&internal_frame?/1)
        |> Enum.take(32)
        |> Enum.map(&encode_frame/1)

      if stack != [] do
        %Google.Protobuf.Any{
          type_url: @type_url,
          value: Error.encode(%Error{stack_trace: stack})
        }
      end
    end
  end

  def extensions do
    case Process.get(@scope, :uncaptured) do
      :uncaptured -> List.wrap(capture())
      trace -> List.wrap(trace)
    end
  end

  def with_trace(trace, fun) do
    previous = Process.get(@scope, :uncaptured)
    Process.put(@scope, trace)

    try do
      fun.()
    after
      if previous == :uncaptured,
        do: Process.delete(@scope),
        else: Process.put(@scope, previous)
    end
  end

  defp enabled? do
    Application.get_env(:spark_ex, :debug_client_call_stack, false) == true or
      String.downcase(System.get_env("SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK", "false")) in [
        "true",
        "1"
      ]
  end

  defp internal_frame?({module, _, _, location}) do
    file = location |> Keyword.get(:file, ~c"") |> to_string() |> String.replace("\\", "/")

    module in [__MODULE__, Process, GenServer, :gen, :gen_server, :erlang, :proc_lib] or
      String.starts_with?(file, "lib/spark_ex/") or file == "lib/spark_ex.ex" or
      String.contains?(file, "/lib/spark_ex/") or String.ends_with?(file, "/lib/spark_ex.ex")
  end

  defp encode_frame({module, function, _arity, location}) do
    %StackTraceElement{
      declaring_class: inspect(module),
      method_name: Atom.to_string(function),
      file_name: location |> Keyword.get(:file, ~c"") |> to_string(),
      line_number: Keyword.get(location, :line, 0)
    }
  end
end
