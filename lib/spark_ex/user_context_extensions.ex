defmodule SparkEx.UserContextExtensions do
  @moduledoc """
  Manages user context extensions attached to Spark Connect requests.
  """

  alias Google.Protobuf.Any
  alias Spark.Connect.UserContext

  @table :spark_ex_user_context_extensions

  @spec add_threadlocal_user_context_extension(Any.t()) :: :ok
  def add_threadlocal_user_context_extension(%Any{} = extension) do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.insert(@table, {{thread_scope(self()), extension.type_url}, extension})
    :ok
  end

  @spec add_global_user_context_extension(Any.t()) :: :ok
  def add_global_user_context_extension(%Any{} = extension) do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.insert(@table, {{:global, extension.type_url}, extension})
    :ok
  end

  @spec remove_user_context_extension(String.t()) :: :ok
  def remove_user_context_extension(extension_id) when is_binary(extension_id) do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.delete(@table, {:global, extension_id})
    :ets.delete(@table, {thread_scope(self()), extension_id})
    :ok
  end

  @spec clear_user_context_extensions() :: :ok
  def clear_user_context_extensions() do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.match_delete(@table, {{:global, :_}, :_})
    :ets.match_delete(@table, {{thread_scope(self()), :_}, :_})
    :ok
  end

  @spec clear_threadlocal_user_context_extensions() :: :ok
  def clear_threadlocal_user_context_extensions() do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.match_delete(@table, {{thread_scope(self()), :_}, :_})
    :ok
  end

  @spec build_user_context(String.t(), [Any.t()]) :: UserContext.t()
  def build_user_context(user_id, extra \\ []) when is_binary(user_id) and is_list(extra) do
    %UserContext{user_id: user_id, extensions: collect_extensions(extra)}
  end

  @doc false
  @spec collect_extensions([Any.t()]) :: [Any.t()]
  def collect_extensions(extra \\ []) when is_list(extra) do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)

    extensions =
      extra
      |> Enum.reduce(extensions_map(), fn %Any{} = ext, acc ->
        Map.put(acc, ext.type_url, ext)
      end)

    Map.values(extensions)
  end

  defp extensions_map() do
    global_extensions()
    |> Map.merge(threadlocal_extensions(self()))
  end

  defp global_extensions() do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)

    @table
    |> :ets.match_object({{:global, :_}, :_})
    |> Map.new(fn {{:global, id}, extension} -> {id, extension} end)
  end

  defp threadlocal_extensions(pid) when is_pid(pid) do
    scope = thread_scope(pid)

    @table
    |> :ets.match_object({{scope, :_}, :_})
    |> Map.new(fn {{^scope, id}, extension} -> {id, extension} end)
  end

  defp thread_scope(pid), do: {:pid, pid}
end
