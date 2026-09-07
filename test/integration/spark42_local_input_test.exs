defmodule SparkEx.Integration.Spark42LocalInputTest do
  use ExUnit.Case
  @moduletag :integration

  alias SparkEx.{DataFrame, Session, Types}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "local empty rows preserve their cardinality independently of field count", %{
    session: session
  } do
    for rows <- [[%{}, %{}, %{}], [{}, {}, {}], [[], [], []]],
        opts <- [[], [schema: Types.struct_type([])], [schema: ""]] do
      assert {:ok, df} = SparkEx.create_dataframe(session, rows, opts)
      assert {:ok, []} = DataFrame.columns(df)
      assert {:ok, 3} = DataFrame.count(df)
      assert {:ok, [%{}, %{}, %{}]} = DataFrame.collect(df)
    end

    assert {:ok, empty} = SparkEx.empty_dataframe(session, Types.struct_type([]))
    assert {:ok, 0} = DataFrame.count(empty)
    assert {:ok, []} = DataFrame.collect(empty)
    assert {:ok, native} = DataFrame.to_explorer(empty)
    assert Explorer.DataFrame.n_rows(native) == 0
    assert Explorer.DataFrame.n_columns(native) == 0

    assert {:ok, native_rows} =
             SparkEx.range(session, 3, num_partitions: 1)
             |> DataFrame.select([])
             |> DataFrame.to_explorer()

    assert Explorer.DataFrame.shape(native_rows) == {3, 0}
    assert {:ok, uploaded} = SparkEx.create_dataframe(session, native_rows)
    assert {:ok, 3} = DataFrame.count(uploaded)
    assert {:ok, [%{}, %{}, %{}]} = DataFrame.collect(uploaded)
  end
end
