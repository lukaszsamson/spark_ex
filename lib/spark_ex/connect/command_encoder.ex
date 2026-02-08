defmodule SparkEx.Connect.CommandEncoder do
  @moduledoc """
  Encodes SparkEx command representations into Spark Connect protobuf messages.

  Commands use `Plan.op_type: {:command, command_proto}` and are executed via
  the same `ExecutePlan` RPC as relation plans. They represent side-effecting
  operations like writes and view creation.
  """

  alias Spark.Connect.{
    Command,
    CreateDataFrameViewCommand,
    Plan,
    WriteOperation,
    WriteOperationV2
  }

  alias SparkEx.Connect.PlanEncoder

  @doc """
  Encodes a command tuple into a `Spark.Connect.Plan` with `{:command, ...}` op_type.

  Returns `{plan, new_counter}`.
  """
  @spec encode(term(), non_neg_integer()) :: {Plan.t(), non_neg_integer()}
  def encode(command_tuple, counter) do
    {command, counter} = encode_command(command_tuple, counter)
    {%Plan{op_type: {:command, command}}, counter}
  end

  @doc """
  Encodes a command tuple into a `Spark.Connect.Command` proto.

  Returns `{command, new_counter}`.
  """
  @spec encode_command(term(), non_neg_integer()) :: {Command.t(), non_neg_integer()}

  # --- CreateDataFrameView ---

  def encode_command({:create_dataframe_view, df_plan, name, is_global, replace}, counter) do
    {relation, counter} = PlanEncoder.encode_relation(df_plan, counter)

    command = %Command{
      command_type:
        {:create_dataframe_view,
         %CreateDataFrameViewCommand{
           input: relation,
           name: name,
           is_global: is_global,
           replace: replace
         }}
    }

    {command, counter}
  end

  # --- WriteOperation (V1) ---

  def encode_command({:write_operation, df_plan, write_opts}, counter) do
    {relation, counter} = PlanEncoder.encode_relation(df_plan, counter)

    save_type = encode_save_type(write_opts)
    mode = encode_save_mode(Keyword.get(write_opts, :mode, :error_if_exists))
    source = Keyword.get(write_opts, :format, nil)
    options = write_opts |> Keyword.get(:options, %{}) |> stringify_options()
    sort_column_names = Keyword.get(write_opts, :sort_by, [])
    partitioning_columns = Keyword.get(write_opts, :partition_by, [])
    clustering_columns = Keyword.get(write_opts, :cluster_by, [])
    bucket_by = encode_bucket_by(write_opts)

    write_op = %WriteOperation{
      input: relation,
      source: source,
      mode: mode,
      sort_column_names: sort_column_names,
      partitioning_columns: partitioning_columns,
      bucket_by: bucket_by,
      options: options,
      clustering_columns: clustering_columns
    }

    write_op = apply_save_type(write_op, save_type)

    command = %Command{command_type: {:write_operation, write_op}}
    {command, counter}
  end

  # --- WriteOperationV2 ---

  def encode_command({:write_operation_v2, df_plan, table_name, v2_opts}, counter) do
    {relation, counter} = PlanEncoder.encode_relation(df_plan, counter)

    mode = encode_v2_mode(Keyword.get(v2_opts, :mode, :create))
    provider = Keyword.get(v2_opts, :provider, nil)
    options = v2_opts |> Keyword.get(:options, %{}) |> stringify_options()
    table_properties = v2_opts |> Keyword.get(:table_properties, %{}) |> stringify_options()
    clustering_columns = Keyword.get(v2_opts, :cluster_by, [])

    partitioning_columns =
      v2_opts
      |> Keyword.get(:partitioned_by, [])
      |> Enum.map(&PlanEncoder.encode_expression/1)

    overwrite_condition =
      case Keyword.get(v2_opts, :overwrite_condition, nil) do
        nil -> nil
        expr -> PlanEncoder.encode_expression(expr)
      end

    write_v2 = %WriteOperationV2{
      input: relation,
      table_name: table_name,
      provider: provider,
      partitioning_columns: partitioning_columns,
      options: options,
      table_properties: table_properties,
      mode: mode,
      overwrite_condition: overwrite_condition,
      clustering_columns: clustering_columns
    }

    command = %Command{command_type: {:write_operation_v2, write_v2}}
    {command, counter}
  end

  # --- Private helpers ---

  defp encode_save_type(opts) do
    path = Keyword.get(opts, :path, nil)
    table = Keyword.get(opts, :table, nil)
    insert_into = Keyword.get(opts, :insert_into, nil)

    cond do
      path != nil ->
        {:path, path}

      table != nil ->
        {:table,
         %WriteOperation.SaveTable{
           table_name: table,
           save_method: :TABLE_SAVE_METHOD_SAVE_AS_TABLE
         }}

      insert_into != nil ->
        {:table,
         %WriteOperation.SaveTable{
           table_name: insert_into,
           save_method: :TABLE_SAVE_METHOD_INSERT_INTO
         }}

      true ->
        nil
    end
  end

  defp apply_save_type(write_op, nil), do: write_op
  defp apply_save_type(write_op, save_type), do: %{write_op | save_type: save_type}

  defp encode_bucket_by(opts) do
    case Keyword.get(opts, :bucket_by, nil) do
      {num_buckets, columns} when is_integer(num_buckets) and is_list(columns) ->
        %WriteOperation.BucketBy{
          num_buckets: num_buckets,
          bucket_column_names: columns
        }

      nil ->
        nil
    end
  end

  defp encode_save_mode(:append), do: :SAVE_MODE_APPEND
  defp encode_save_mode(:overwrite), do: :SAVE_MODE_OVERWRITE
  defp encode_save_mode(:error_if_exists), do: :SAVE_MODE_ERROR_IF_EXISTS
  defp encode_save_mode(:ignore), do: :SAVE_MODE_IGNORE

  defp encode_save_mode(other),
    do: raise(ArgumentError, "invalid save mode: #{inspect(other)}")

  defp encode_v2_mode(:create), do: :MODE_CREATE
  defp encode_v2_mode(:overwrite), do: :MODE_OVERWRITE
  defp encode_v2_mode(:overwrite_partitions), do: :MODE_OVERWRITE_PARTITIONS
  defp encode_v2_mode(:append), do: :MODE_APPEND
  defp encode_v2_mode(:replace), do: :MODE_REPLACE
  defp encode_v2_mode(:create_or_replace), do: :MODE_CREATE_OR_REPLACE

  defp encode_v2_mode(other),
    do: raise(ArgumentError, "invalid V2 write mode: #{inspect(other)}")

  defp stringify_options(opts) when is_map(opts) do
    Map.new(opts, fn {k, v} -> {to_string(k), to_string(v)} end)
  end
end
