defmodule SparkEx.Integration.Spark42P0Test do
  use ExUnit.Case

  @moduletag :integration
  @moduletag min_spark: "4.2"

  alias SparkEx.{Column, DataFrame, Functions, Observation, Session, Writer}

  setup do
    {:ok, session} = SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"))
    Process.unlink(session)
    on_exit(fn -> if Process.alive?(session), do: Session.stop(session) end)
    %{session: session}
  end

  test "failed observed metrics preserve collect, show, iterator and write success", %{
    session: session
  } do
    for action <- [:collect, :show, :iterator, :write, :arrow, :explorer] do
      obs = Observation.new("failure_#{action}_#{System.unique_integer([:positive])}")

      df =
        SparkEx.range(session, 3)
        |> DataFrame.observe(obs, [
          Column.alias_(Functions.sum("id"), "sum_id"),
          Column.alias_(Functions.expr("raise_error('metric failure')"), "failure")
        ])

      case action do
        :collect ->
          assert {:ok, [_, _, _]} = DataFrame.collect(df)

        :show ->
          assert {:ok, text} = DataFrame.show(df)
          assert text =~ "id"

        :iterator ->
          caller = self()

          assert {:ok, rows} =
                   DataFrame.to_local_iterator(df,
                     on_metrics: fn metrics -> send(caller, {:metrics, metrics}) end
                   )

          assert length(Enum.to_list(rows)) == 3
          assert_received {:metrics, %{observed_metrics: metrics}}
          assert map_size(metrics) == 1

        :write ->
          path =
            Path.join(
              System.tmp_dir!(),
              "spark42-observation-#{System.unique_integer([:positive])}"
            )

          on_exit(fn -> File.rm_rf!(path) end)
          assert :ok = df |> DataFrame.write() |> Writer.format("parquet") |> Writer.save(path)

        :arrow ->
          assert {:ok, _} = DataFrame.to_arrow(df)

        :explorer ->
          assert {:ok, _} = DataFrame.to_explorer(df)
      end

      assert {:error, %SparkEx.Error.Remote{message: message, error_class: error_class}} =
               Observation.get(obs)

      assert message =~ "metric failure"
      assert is_binary(error_class)
    end
  end

  test "cache overrides cannot bypass server size limit, config changes invalidate cache", %{
    session: session
  } do
    source = Explorer.DataFrame.new(%{"id" => [1, 2, 3]})
    assert :ok = Session.config_set(session, [{"spark.sql.session.localRelationSizeLimit", "1"}])

    assert {:error, %SparkEx.Error.Remote{error_class: "LOCAL_RELATION_SIZE_LIMIT_EXCEEDED"}} =
             SparkEx.create_dataframe(session, source, cache_threshold: 0, cache_chunk_size: 1024)

    assert :ok =
             Session.config_set(session, [{"spark.sql.session.localRelationSizeLimit", "1048576"}])

    assert {:ok, df} =
             SparkEx.create_dataframe(session, source, cache_threshold: 0, cache_chunk_size: 1024)

    assert {:ok, rows} = DataFrame.collect(df)
    assert Enum.sort(Enum.map(rows, & &1["id"])) == [1, 2, 3]
  end
end
