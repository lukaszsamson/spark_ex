defmodule SparkEx.UserContextExtensions do
  @moduledoc """
  Manages user context extensions attached to Spark Connect requests.
  """

  alias Google.Protobuf.Any
  alias Spark.Connect.UserContext

  @table :spark_ex_user_context_extensions
  @thread_key {:spark_ex, :user_context_extensions}

  @spec add_threadlocal_user_context_extension(Any.t()) :: :ok
  def add_threadlocal_user_context_extension(%Any{} = extension) do
    extensions = threadlocal_extensions()
    updated = Map.put(extensions, extension.type_url, extension)
    Process.put(@thread_key, updated)
    :ok
  end

  @spec add_global_user_context_extension(Any.t()) :: :ok
  def add_global_user_context_extension(%Any{} = extension) do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.insert(@table, {extension.type_url, extension})
    :ok
  end

  @spec remove_user_context_extension(String.t()) :: :ok
  def remove_user_context_extension(extension_id) when is_binary(extension_id) do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.delete(@table, extension_id)

    extensions = threadlocal_extensions()
    Process.put(@thread_key, Map.delete(extensions, extension_id))
    :ok
  end

  @spec clear_user_context_extensions() :: :ok
  def clear_user_context_extensions() do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)
    :ets.delete_all_objects(@table)
    Process.put(@thread_key, %{})
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
    |> Map.merge(threadlocal_extensions())
  end

  defp threadlocal_extensions() do
    case Process.get(@thread_key) do
      nil -> %{}
      extensions when is_map(extensions) -> extensions
      _ -> %{}
    end
  end

  defp global_extensions() do
    SparkEx.EtsTableOwner.ensure_table!(@table, :set)

    @table
    |> :ets.tab2list()
    |> Map.new(fn {id, extension} -> {id, extension} end)
  end

end
