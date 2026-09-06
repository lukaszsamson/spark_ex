defmodule SparkEx.Integration.Spark42ZeroColumnResultTest do
  use ExUnit.Case

  @moduletag :integration

  alias Spark.Connect.{DataType, ExecutePlanResponse}
  alias SparkEx.{DataFrame, Session}
  alias SparkEx.Connect.ResultDecoder

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "zero-column Arrow batches preserve row cardinality in row materializations", %{
    session: session
  } do
    df = SparkEx.range(session, 3) |> DataFrame.select([])

    assert {:ok, [%{}, %{}, %{}]} = DataFrame.collect(df)
    assert {:ok, stream} = DataFrame.to_local_iterator(df)
    assert Enum.to_list(stream) == [{:ok, %{}}, {:ok, %{}}, {:ok, %{}}]
    assert {:ok, 3} = DataFrame.count(df)
  end

  test "Explorer preserves single-batch height and rejects lossy multi-batch concatenation", %{
    session: session
  } do
    df = SparkEx.range(session, 3, num_partitions: 3) |> DataFrame.select([])

    assert {:error, {:unsupported_zero_column_explorer, %{row_count: 3}}} =
             DataFrame.to_explorer(df)

    assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :rows, actual_value: 3}} =
             DataFrame.to_explorer(df, max_rows: 2, unsafe: true)

    assert {:ok, arrow} = DataFrame.to_arrow(df)
    assert is_binary(arrow) or is_list(arrow)

    assert {:ok, single} =
             SparkEx.range(session, 1) |> DataFrame.select([]) |> DataFrame.to_explorer()

    assert Explorer.DataFrame.shape(single) == {1, 0}
  end

  test "zero-column reconstruction rejects hostile envelope metadata", %{session: session} do
    df = SparkEx.range(session, 1) |> DataFrame.select([])
    assert {:ok, arrow} = DataFrame.to_arrow(df)
    ipc = if is_list(arrow), do: hd(arrow), else: arrow

    frame = fn row_count, schema ->
      {:ok,
       %ExecutePlanResponse{
         schema: schema,
         response_type:
           {:arrow_batch,
            %ExecutePlanResponse.ArrowBatch{
              data: ipc,
              row_count: row_count,
              start_offset: 0
            }}
       }}
    end

    assert {:error, {:invalid_arrow_batch, message}} =
             ResultDecoder.decode_stream([frame.(-1, nil)])

    assert message =~ "row_count must be non-negative"

    nonempty_schema = %DataType{
      kind:
        {:struct,
         %DataType.Struct{
           fields: [
             %DataType.StructField{
               name: "id",
               data_type: %DataType{kind: {:long, %DataType.Long{}}},
               nullable: false
             }
           ]
         }}
    }

    assert {:error, {:invalid_arrow_batch_row_count, %{expected: 1, got: 0}}} =
             ResultDecoder.decode_stream([frame.(1, nonempty_schema)])

    assert {:error, {:invalid_arrow_batch_row_count, %{expected: 1_000_001, got: 1}}} =
             ResultDecoder.decode_stream([frame.(1_000_001, nil)])
  end

  test "Explorer preserves a representable zero-column height across empty batch orderings", %{
    session: session
  } do
    df = SparkEx.range(session, 1) |> DataFrame.select([])
    assert {:ok, arrow} = DataFrame.to_arrow(df)
    one_row_ipc = if is_list(arrow), do: hd(arrow), else: arrow

    empty_df = Explorer.DataFrame.new([])
    assert {:ok, empty_ipc} = Explorer.DataFrame.dump_ipc_stream(empty_df)

    frame = fn ipc, row_count, start_offset ->
      {:ok,
       %ExecutePlanResponse{
         response_type:
           {:arrow_batch,
            %ExecutePlanResponse.ArrowBatch{
              data: ipc,
              row_count: row_count,
              start_offset: start_offset
            }}
       }}
    end

    for frames <- [
          [frame.(one_row_ipc, 1, 0), frame.(empty_ipc, 0, 1)],
          [frame.(empty_ipc, 0, 0), frame.(one_row_ipc, 1, 0)]
        ] do
      assert {:ok, result} = ResultDecoder.decode_stream_explorer(frames, nil)
      assert Explorer.DataFrame.shape(result.dataframe) == {1, 0}
    end

    assert {:ok, result} =
             ResultDecoder.decode_stream_explorer(
               [frame.(empty_ipc, 0, 0), frame.(empty_ipc, 0, 0)],
               nil
             )

    assert Explorer.DataFrame.shape(result.dataframe) == {0, 0}
  end
end
