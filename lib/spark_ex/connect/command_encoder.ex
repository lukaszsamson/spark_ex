defmodule SparkEx.Connect.CommandEncoder do
  @moduledoc """
  Encodes SparkEx command representations into Spark Connect protobuf messages.

  Commands use `Plan.op_type: {:command, command_proto}` and are executed via
  the same `ExecutePlan` RPC as relation plans. They represent side-effecting
  operations like writes and view creation.
  """

  alias Spark.Connect.{
    Command,
    CommonInlineUserDefinedFunction,
    CommonInlineUserDefinedTableFunction,
    CreateDataFrameViewCommand,
    Expression,
    JavaUDF,
    MergeAction,
    MergeIntoTableCommand,
    Plan,
    PythonUDTF,
    StreamingQueryCommand,
    StreamingQueryInstanceId,
    StreamingQueryListenerBusCommand,
    StreamingQueryManagerCommand,
    WriteOperation,
    WriteOperationV2,
    WriteStreamOperationStart
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

  # --- MergeIntoTableCommand ---

  def encode_command(
        {:merge_into_table, source_plan, target_table, condition_expr, match_actions,
         not_matched_actions, not_matched_by_source_actions, schema_evolution},
        counter
      ) do
    {source_relation, counter} = PlanEncoder.encode_relation(source_plan, counter)
    merge_condition = PlanEncoder.encode_expression(condition_expr)

    command = %Command{
      command_type:
        {:merge_into_table_command,
         %MergeIntoTableCommand{
           target_table_name: target_table,
           source_table_plan: source_relation,
           merge_condition: merge_condition,
           match_actions: Enum.map(match_actions, &encode_merge_action/1),
           not_matched_actions: Enum.map(not_matched_actions, &encode_merge_action/1),
           not_matched_by_source_actions:
             Enum.map(not_matched_by_source_actions, &encode_merge_action/1),
           with_schema_evolution: schema_evolution
         }}
    }

    {command, counter}
  end

  # --- RegisterJavaUDF ---

  def encode_command({:register_java_udf, name, class_name, return_type, aggregate}, counter) do
    java_udf = %JavaUDF{
      class_name: class_name,
      output_type: return_type,
      aggregate: aggregate
    }

    fun = %CommonInlineUserDefinedFunction{
      function_name: name,
      deterministic: true,
      function: {:java_udf, java_udf}
    }

    command = %Command{command_type: {:register_function, fun}}
    {command, counter}
  end

  # --- RegisterUDTF (register_table_function) ---

  def encode_command(
        {:register_udtf, name, python_command, return_type, eval_type, python_ver, deterministic},
        counter
      ) do
    python_udtf = %PythonUDTF{
      return_type: return_type,
      eval_type: eval_type,
      command: python_command,
      python_ver: python_ver
    }

    udtf = %CommonInlineUserDefinedTableFunction{
      function_name: name,
      deterministic: deterministic,
      function: {:python_udtf, python_udtf}
    }

    command = %Command{command_type: {:register_table_function, udtf}}
    {command, counter}
  end

  # --- RegisterDataSource ---

  def encode_command({:register_data_source, name, python_command, python_ver}, counter) do
    data_source = %Spark.Connect.CommonInlineUserDefinedDataSource{
      name: name,
      data_source:
        {:python_data_source,
         %Spark.Connect.PythonDataSource{command: python_command, python_ver: python_ver}}
    }

    command = %Command{command_type: {:register_data_source, data_source}}
    {command, counter}
  end

  # --- WriteStreamOperationStart ---

  def encode_command({:write_stream_operation_start, df_plan, write_opts}, counter) do
    {relation, counter} = PlanEncoder.encode_relation(df_plan, counter)

    format = Keyword.get(write_opts, :format, nil)
    options = write_opts |> Keyword.get(:options, %{}) |> stringify_options()
    output_mode = Keyword.get(write_opts, :output_mode, nil)
    query_name = Keyword.get(write_opts, :query_name, nil)
    partitioning_columns = Keyword.get(write_opts, :partition_by, [])
    clustering_columns = Keyword.get(write_opts, :cluster_by, [])

    write_proto = %WriteStreamOperationStart{
      input: relation,
      format: format || "",
      options: options,
      output_mode: output_mode || "",
      query_name: query_name || "",
      partitioning_column_names: partitioning_columns,
      clustering_column_names: clustering_columns
    }

    write_proto = apply_trigger(write_proto, Keyword.get(write_opts, :trigger, nil))
    write_proto = apply_sink_destination(write_proto, write_opts)
    write_proto = apply_foreach(write_proto, write_opts)

    command = %Command{command_type: {:write_stream_operation_start, write_proto}}
    {command, counter}
  end

  # --- StreamingQueryCommand ---

  def encode_command({:streaming_query_command, query_id, run_id, cmd_type}, counter) do
    query_id_proto = %StreamingQueryInstanceId{id: query_id, run_id: run_id}

    cmd = %StreamingQueryCommand{
      query_id: query_id_proto,
      command: encode_sq_command(cmd_type)
    }

    command = %Command{command_type: {:streaming_query_command, cmd}}
    {command, counter}
  end

  # --- StreamingQueryListenerBusCommand ---

  def encode_command({:streaming_query_listener_bus_command, cmd_type}, counter) do
    cmd = %StreamingQueryListenerBusCommand{command: encode_listener_bus_command(cmd_type)}
    command = %Command{command_type: {:streaming_query_listener_bus_command, cmd}}
    {command, counter}
  end

  # --- StreamingQueryManagerCommand ---

  def encode_command({:streaming_query_manager_command, cmd_type}, counter) do
    cmd = %StreamingQueryManagerCommand{command: encode_sqm_command(cmd_type)}
    command = %Command{command_type: {:streaming_query_manager_command, cmd}}
    {command, counter}
  end

  # --- Private helpers ---

  defp encode_merge_action({action_type, condition_expr, assignments}) do
    action_type_enum =
      case action_type do
        :delete -> :ACTION_TYPE_DELETE
        :insert -> :ACTION_TYPE_INSERT
        :insert_star -> :ACTION_TYPE_INSERT_STAR
        :update -> :ACTION_TYPE_UPDATE
        :update_star -> :ACTION_TYPE_UPDATE_STAR
      end

    condition =
      case condition_expr do
        nil -> nil
        expr -> PlanEncoder.encode_expression(expr)
      end

    encoded_assignments =
      Enum.map(assignments, fn {key_expr, value_expr} ->
        %MergeAction.Assignment{
          key: PlanEncoder.encode_expression(key_expr),
          value: PlanEncoder.encode_expression(value_expr)
        }
      end)

    %Expression{
      expr_type:
        {:merge_action,
         %MergeAction{
           action_type: action_type_enum,
           condition: condition,
           assignments: encoded_assignments
         }}
    }
  end

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

  # --- Streaming helpers ---

  defp apply_trigger(proto, nil), do: proto

  defp apply_trigger(proto, {:processing_time, interval}),
    do: %{proto | trigger: {:processing_time_interval, interval}}

  defp apply_trigger(proto, :available_now), do: %{proto | trigger: {:available_now, true}}
  defp apply_trigger(proto, :once), do: %{proto | trigger: {:once, true}}

  defp apply_trigger(proto, {:continuous, interval}),
    do: %{proto | trigger: {:continuous_checkpoint_interval, interval}}

  defp apply_sink_destination(proto, opts) do
    path = Keyword.get(opts, :path, nil)
    table_name = Keyword.get(opts, :table_name, nil)

    cond do
      path != nil -> %{proto | sink_destination: {:path, path}}
      table_name != nil -> %{proto | sink_destination: {:table_name, table_name}}
      true -> proto
    end
  end

  defp apply_foreach(proto, opts) do
    foreach_writer = Keyword.get(opts, :foreach_writer, nil)
    foreach_batch = Keyword.get(opts, :foreach_batch, nil)

    proto =
      if foreach_writer do
        %{proto | foreach_writer: foreach_writer}
      else
        proto
      end

    if foreach_batch do
      %{proto | foreach_batch: foreach_batch}
    else
      proto
    end
  end

  defp encode_sq_command({:status}), do: {:status, true}
  defp encode_sq_command({:stop}), do: {:stop, true}
  defp encode_sq_command({:process_all_available}), do: {:process_all_available, true}
  defp encode_sq_command({:recent_progress}), do: {:recent_progress, true}
  defp encode_sq_command({:last_progress}), do: {:last_progress, true}
  defp encode_sq_command({:exception}), do: {:exception, true}

  defp encode_sq_command({:explain, extended}) do
    {:explain, %StreamingQueryCommand.ExplainCommand{extended: extended}}
  end

  defp encode_sq_command({:await_termination, timeout_ms}) do
    {:await_termination, %StreamingQueryCommand.AwaitTerminationCommand{timeout_ms: timeout_ms}}
  end

  defp encode_sqm_command({:active}), do: {:active, true}
  defp encode_sqm_command({:get_query, id}), do: {:get_query, id}
  defp encode_sqm_command({:reset_terminated}), do: {:reset_terminated, true}
  defp encode_sqm_command({:list_listeners}), do: {:list_listeners, true}

  defp encode_sqm_command({:await_any_termination, timeout_ms}) do
    {:await_any_termination,
     %StreamingQueryManagerCommand.AwaitAnyTerminationCommand{timeout_ms: timeout_ms}}
  end

  defp encode_sqm_command({:add_listener, listener_id, listener_payload}) do
    {:add_listener,
     %StreamingQueryManagerCommand.StreamingQueryListenerCommand{
       id: listener_id,
       listener_payload: listener_payload
     }}
  end

  defp encode_sqm_command({:remove_listener, listener_id, listener_payload}) do
    {:remove_listener,
     %StreamingQueryManagerCommand.StreamingQueryListenerCommand{
       id: listener_id,
       listener_payload: listener_payload
     }}
  end

  defp encode_listener_bus_command(:add), do: {:add_listener_bus_listener, true}
  defp encode_listener_bus_command(:remove), do: {:remove_listener_bus_listener, true}
end
