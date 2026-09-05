defmodule SparkEx.Wave4DecoderTest do
  # Not async: progress tests attach global telemetry handlers and the UDT
  # registry is process-global.
  use ExUnit.Case, async: false

  alias Explorer.DataFrame, as: DF
  alias Explorer.Series
  alias Spark.Connect.DataType
  alias Spark.Connect.ExecutePlanResponse
  alias SparkEx.Connect.ResultDecoder
  alias SparkEx.Connect.TypeMapper
  alias SparkEx.Connect.UDTRegistry

  @progress_event [:spark_ex, :result, :progress]

  # ── Schema helpers ──────────────────────────────────────────────────────

  defp t(kind, value), do: %DataType{kind: {kind, value}}
  defp int_type, do: t(:integer, %DataType.Integer{})
  defp string_type, do: t(:string, %DataType.String{})
  defp timestamp_type, do: t(:timestamp, %DataType.Timestamp{})
  defp timestamp_ntz_type, do: t(:timestamp_ntz, %DataType.TimestampNTZ{})
  defp variant_type, do: t(:variant, %DataType.Variant{})
  defp map_type(kt, vt), do: t(:map, %DataType.Map{key_type: kt, value_type: vt})
  defp array_type(et), do: t(:array, %DataType.Array{element_type: et})

  defp udt_type(class, sql_type),
    do: t(:udt, %DataType.UDT{type: "udt", jvm_class: class, sql_type: sql_type})

  defp struct_type(fields) do
    t(:struct, %DataType.Struct{
      fields:
        Enum.map(fields, fn {name, dt} ->
          %DataType.StructField{name: name, data_type: dt, nullable: true}
        end)
    })
  end

  defp schema(fields), do: struct_type(fields)

  # ── Frame helpers ───────────────────────────────────────────────────────

  defp arrow_frame(df, schema, opts \\ []) do
    {:ok, ipc} = DF.dump_ipc_stream(df)

    {:ok,
     %ExecutePlanResponse{
       schema: schema,
       operation_id: Keyword.get(opts, :operation_id, ""),
       response_type:
         {:arrow_batch,
          %ExecutePlanResponse.ArrowBatch{
            data: ipc,
            row_count: DF.n_rows(df),
            start_offset: 0,
            chunk_index: 0,
            num_chunks_in_batch: 1
          }}
     }}
  end

  defp complete_frame(schema \\ nil) do
    {:ok,
     %ExecutePlanResponse{
       schema: schema,
       response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}}
     }}
  end

  defp progress_frame(stages, inflight, opts) do
    progress = %ExecutePlanResponse.ExecutionProgress{
      stages:
        Enum.map(stages, fn {id, total, completed} ->
          %ExecutePlanResponse.ExecutionProgress.StageInfo{
            stage_id: id,
            num_tasks: total,
            num_completed_tasks: completed,
            input_bytes_read: 0,
            done: total == completed
          }
        end),
      num_inflight_tasks: inflight
    }

    {:ok,
     %ExecutePlanResponse{
       operation_id: Keyword.get(opts, :operation_id, ""),
       response_type: {:execution_progress, progress}
     }}
  end

  defp attach_progress(session_id) do
    ref = make_ref()
    parent = self()
    id = "wave4-progress-#{inspect(ref)}"

    :telemetry.attach(
      id,
      @progress_event,
      fn _event, measurements, metadata, _ ->
        if metadata.session_id == session_id do
          send(parent, {:progress, ref, measurements, metadata})
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
    ref
  end

  defp new_session do
    id = "wave4-#{System.unique_integer([:positive])}"
    %SparkEx.Session{session_id: id}
  end

  # ── T-30: schema policy applied to non-empty frames ─────────────────────

  describe "T-30 schema policy" do
    test "TIMESTAMP columns are normalized to Etc/UTC on non-empty frames" do
      warsaw =
        Series.from_list([~U[2024-01-01 10:00:00.000000Z]],
          dtype: {:datetime, :microsecond, "Europe/Warsaw"}
        )

      df = DF.new([{"ts", warsaw}, {"ntz", Series.from_list([~N[2024-01-01 10:00:00.000000]])}])
      schema = schema([{"ts", timestamp_type()}, {"ntz", timestamp_ntz_type()}])

      assert {:ok, %{dataframe: out}} =
               ResultDecoder.decode_stream_explorer(
                 [arrow_frame(df, schema), complete_frame()],
                 nil
               )

      assert DF.dtypes(out) == %{
               "ts" => {:datetime, :microsecond, "Etc/UTC"},
               "ntz" => {:naive_datetime, :microsecond}
             }

      # The instant is preserved; only the zone label changes.
      assert [%{"ts" => ~U[2024-01-01 10:00:00.000000Z]}] = DF.to_rows(DF.select(out, ["ts"]))
    end

    test "empty and non-empty frames agree on dtypes" do
      warsaw =
        Series.from_list([~U[2024-01-01 10:00:00.000000Z]],
          dtype: {:datetime, :microsecond, "Europe/Warsaw"}
        )

      df = DF.new([{"ts", warsaw}, {"n", Series.from_list([1])}])
      schema = schema([{"ts", timestamp_type()}, {"n", t(:long, %DataType.Long{})}])

      {:ok, %{dataframe: full}} =
        ResultDecoder.decode_stream_explorer([arrow_frame(df, schema), complete_frame()], nil)

      {:ok, %{dataframe: empty}} =
        ResultDecoder.decode_stream_explorer([complete_frame(schema)], nil)

      assert DF.n_rows(empty) == 0
      assert DF.dtypes(full) == DF.dtypes(empty)
    end

    test "empty map / variant / UDT columns get a structural dtype instead of :null" do
      schema =
        schema([
          {"m", map_type(string_type(), int_type())},
          {"v", variant_type()},
          {"u", udt_type("com.example.NoDeserializer", int_type())},
          {"ym", t(:year_month_interval, %DataType.YearMonthInterval{})}
        ])

      {:ok, %{dataframe: empty}} =
        ResultDecoder.decode_stream_explorer([complete_frame(schema)], nil)

      assert DF.dtypes(empty) == %{
               "m" => {:list, {:struct, [{"key", :string}, {"value", {:s, 32}}]}},
               "v" => {:struct, [{"value", :binary}, {"metadata", :binary}]},
               "u" => {:s, 32},
               # Known limitation: Explorer has no interval dtype for year-month
               # intervals, so the empty column is inferred as :null.
               "ym" => :null
             }
    end

    test "a cast Explorer rejects leaves the column untouched" do
      df = DF.new([{"s", ["not-a-map"]}])
      schema = schema([{"s", map_type(string_type(), int_type())}])

      assert {:ok, %{dataframe: out}} =
               ResultDecoder.decode_stream_explorer(
                 [arrow_frame(df, schema), complete_frame()],
                 nil
               )

      assert DF.dtypes(out) == %{"s" => :string}
    end
  end

  # ── T-31: nested UDT deserialization ───────────────────────────────────

  describe "T-31 nested UDT deserialization" do
    setup do
      class = "com.example.Wave4UDT#{System.unique_integer([:positive])}"
      :ok = UDTRegistry.register(class, fn v -> {:udt, v} end)
      on_exit(fn -> UDTRegistry.unregister(class) end)
      %{class: class}
    end

    test "column_value_transform recurses through array, map and struct", %{class: class} do
      udt = udt_type(class, int_type())

      arr = ResultDecoder.column_value_transform(array_type(udt))
      assert arr.([1, 2]) == [{:udt, 1}, {:udt, 2}]
      assert arr.(nil) == nil

      map = ResultDecoder.column_value_transform(map_type(string_type(), udt))
      assert map.([%{"key" => "a", "value" => 1}]) == [%{"key" => "a", "value" => {:udt, 1}}]
      assert map.(%{"a" => 1}) == %{"a" => {:udt, 1}}
      assert map.(nil) == nil

      st = ResultDecoder.column_value_transform(struct_type([{"x", udt}, {"y", int_type()}]))
      assert st.(%{"x" => 1, "y" => 2}) == %{"x" => {:udt, 1}, "y" => 2}

      deep =
        ResultDecoder.column_value_transform(array_type(struct_type([{"x", array_type(udt)}])))

      assert deep.([%{"x" => [3]}]) == [%{"x" => [{:udt, 3}]}]

      assert ResultDecoder.column_value_transform(array_type(int_type())) == nil
      assert ResultDecoder.column_value_transform(struct_type([{"x", int_type()}])) == nil
    end

    test "apply_row_transforms deserializes UDTs nested in containers", %{class: class} do
      udt = udt_type(class, int_type())

      schema =
        schema([
          {"arr", array_type(udt)},
          {"st", struct_type([{"inner", udt}])},
          {"m", map_type(string_type(), udt)},
          {"plain", int_type()}
        ])

      rows = [
        %{
          "arr" => [1, 2],
          "st" => %{"inner" => 3},
          "m" => [%{"key" => "k", "value" => 4}],
          "plain" => 5
        }
      ]

      assert [
               %{
                 "arr" => [{:udt, 1}, {:udt, 2}],
                 "st" => %{"inner" => {:udt, 3}},
                 "m" => [%{"key" => "k", "value" => {:udt, 4}}],
                 "plain" => 5
               }
             ] = ResultDecoder.apply_row_transforms(rows, schema)
    end

    test "Explorer path deserializes nested UDTs and leaves the column to inference", %{
      class: class
    } do
      # The deserializer keeps values Explorer-representable so the frame can
      # be rebuilt; the assertion is on the transformed cells.
      :ok = UDTRegistry.register(class, fn v -> v * 10 end, replace?: true)
      udt = udt_type(class, int_type())

      df = DF.new([{"arr", [[1, 2], [3]]}, {"plain", [7, 8]}])
      schema = schema([{"arr", array_type(udt)}, {"plain", t(:long, %DataType.Long{})}])

      assert {:ok, %{dataframe: out}} =
               ResultDecoder.decode_stream_explorer(
                 [arrow_frame(df, schema), complete_frame()],
                 nil
               )

      assert DF.names(out) == ["arr", "plain"]

      assert DF.to_rows(out) == [
               %{"arr" => [10, 20], "plain" => 7},
               %{"arr" => [30], "plain" => 8}
             ]
    end
  end

  # ── T-61: explorer decoder keeps command results ────────────────────────

  describe "T-61 explorer-mode command results" do
    test "decode_stream_explorer accumulates command-result variants like rows mode" do
      sql = %ExecutePlanResponse.SqlCommandResult{}

      checkpoint = %Spark.Connect.CheckpointCommandResult{
        relation: %Spark.Connect.CachedRemoteRelation{relation_id: "rel-1"}
      }

      stream = [
        {:ok, %ExecutePlanResponse{response_type: {:sql_command_result, sql}}},
        {:ok, %ExecutePlanResponse{response_type: {:checkpoint_command_result, checkpoint}}},
        {:ok, %ExecutePlanResponse{response_type: {:extension, %Google.Protobuf.Any{}}}},
        complete_frame()
      ]

      assert {:ok, explorer} = ResultDecoder.decode_stream_explorer(stream, nil)
      assert {:ok, rows} = ResultDecoder.decode_stream(stream)

      assert explorer.command_result == {:checkpoint, checkpoint}
      assert explorer.command_results == [{:sql_command, sql}, {:checkpoint, checkpoint}]
      assert explorer.command_results == rows.command_results
      assert DF.n_rows(explorer.dataframe) == 0
    end
  end

  # ── T-36: progress handler events ──────────────────────────────────────

  describe "T-36 progress events" do
    test "progress frames carry operation_id and a terminal done event follows" do
      session = new_session()
      ref = attach_progress(session.session_id)

      stream = [
        progress_frame([{0, 10, 2}], 3, operation_id: "op-1"),
        progress_frame([{0, 10, 10}, {1, 4, 1}], 1, operation_id: "op-1"),
        complete_frame()
      ]

      assert {:ok, _} = ResultDecoder.decode_stream(stream, session)

      assert_receive {:progress, ^ref, %{num_inflight_tasks: 3}, meta1}
      assert meta1.operation_id == "op-1"
      assert meta1.done == false
      assert [%{stage_id: 0, num_tasks: 10, num_completed_tasks: 2}] = meta1.stages

      assert_receive {:progress, ^ref, %{num_inflight_tasks: 1}, meta2}
      assert meta2.done == false
      assert length(meta2.stages) == 2

      # Terminal event: done: true, carrying the last known stages.
      assert_receive {:progress, ^ref, %{num_inflight_tasks: 1}, meta3}
      assert meta3.done == true
      assert meta3.operation_id == "op-1"
      assert meta3.stages == meta2.stages

      refute_receive {:progress, ^ref, _, _}, 20
    end

    test "empty stage gates (total tasks == 0) do not invoke handlers" do
      session = new_session()
      ref = attach_progress(session.session_id)

      stream = [
        progress_frame([], 0, operation_id: "op-empty"),
        progress_frame([{0, 0, 0}], 0, operation_id: "op-empty"),
        complete_frame()
      ]

      assert {:ok, _} = ResultDecoder.decode_stream(stream, session)

      # Only the terminal event fires; operation_id was still captured.
      assert_receive {:progress, ^ref, _, %{done: true, operation_id: "op-empty", stages: []}}
      refute_receive {:progress, ^ref, _, _}, 20
    end

    test "a terminal error emits exactly one done event" do
      session = new_session()
      ref = attach_progress(session.session_id)

      stream = [
        progress_frame([{0, 2, 1}], 1, operation_id: "op-err"),
        {:error, :timeout}
      ]

      assert {:error, :timeout} = ResultDecoder.decode_stream(stream, session)

      assert_receive {:progress, ^ref, _, %{done: false}}
      assert_receive {:progress, ^ref, _, %{done: true, operation_id: "op-err"}}
      refute_receive {:progress, ^ref, _, _}, 20
    end

    test "explorer and arrow decoders emit the same done event" do
      for decode <- [
            &ResultDecoder.decode_stream_explorer(&1, &2),
            &ResultDecoder.decode_stream_arrow(&1, &2)
          ] do
        session = new_session()
        ref = attach_progress(session.session_id)

        stream = [progress_frame([{0, 5, 5}], 0, operation_id: "op-x"), complete_frame()]
        assert {:ok, _} = decode.(stream, session)

        assert_receive {:progress, ^ref, _, %{done: false, operation_id: "op-x"}}
        assert_receive {:progress, ^ref, _, %{done: true, operation_id: "op-x"}}
        refute_receive {:progress, ^ref, _, _}, 20
      end
    end

    test "rows_stream emits progress and a single done event" do
      session = new_session()
      ref = attach_progress(session.session_id)

      stream = [progress_frame([{0, 5, 1}], 4, operation_id: "op-iter"), complete_frame()]

      assert [] = stream |> ResultDecoder.rows_stream(session) |> Enum.to_list()

      assert_receive {:progress, ^ref, %{num_inflight_tasks: 4}, %{done: false}}
      assert_receive {:progress, ^ref, _, %{done: true, operation_id: "op-iter"}}
      refute_receive {:progress, ^ref, _, _}, 20
    end

    test "registered handlers receive PySpark-shaped payloads" do
      session_id = "wave4-registry-#{System.unique_integer([:positive])}"
      parent = self()
      handler = fn payload -> send(parent, {:handled, payload}) end

      :ok = SparkEx.ProgressHandlerRegistry.register(session_id, handler)
      on_exit(fn -> SparkEx.ProgressHandlerRegistry.clear(session_id) end)

      session = %SparkEx.Session{session_id: session_id}
      stream = [progress_frame([{0, 3, 1}], 2, operation_id: "op-h"), complete_frame()]
      assert {:ok, _} = ResultDecoder.decode_stream(stream, session)

      assert_receive {:handled, payload}
      assert payload.operation_id == "op-h"
      assert payload.inflight_tasks == 2
      assert payload.done == false
      assert [%{stage_id: 0, num_tasks: 3}] = payload.stages
      # Legacy keys are still present for existing callers.
      assert payload.metadata.session_id == session_id
      assert payload.measurements.num_inflight_tasks == 2

      assert_receive {:handled, %{done: true, operation_id: "op-h"}}
    end
  end

  # ── TypeMapper sanity for the new structural dtypes ────────────────────

  describe "TypeMapper structural dtypes" do
    test "nested map inside array/struct maps to nested list/struct dtypes" do
      dt = array_type(map_type(string_type(), int_type()))

      assert {:ok, {:list, {:list, {:struct, [{"key", :string}, {"value", {:s, 32}}]}}}} =
               TypeMapper.to_explorer_dtype(dt)
    end
  end
end
