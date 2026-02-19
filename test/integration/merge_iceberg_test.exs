defmodule SparkEx.Integration.MergeIcebergTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session) do
        SparkEx.sql(session, "DROP TABLE IF EXISTS iceberg_merge_target") |> DataFrame.collect()
      end
    end)

    %{session: session}
  end

  test "merge into iceberg table updates and inserts", %{session: session} do
    assert {:ok, _} =
             SparkEx.sql(
               session,
               """
               CREATE TABLE iceberg_merge_target (id INT, val STRING)
               USING iceberg
               """
             )
             |> DataFrame.collect()

    assert {:ok, _} =
             SparkEx.sql(
               session,
               "INSERT INTO iceberg_merge_target VALUES (1, 'a'), (2, 'b')"
             )
             |> DataFrame.collect()

    assert {:ok, _} =
             SparkEx.sql(
               session,
               """
               MERGE INTO iceberg_merge_target t
               USING (SELECT 2 AS id, 'bb' AS val UNION ALL SELECT 3 AS id, 'c' AS val) s
               ON t.id = s.id
               WHEN MATCHED THEN UPDATE SET t.val = s.val
               WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
               """
             )
             |> DataFrame.collect()

    result = SparkEx.sql(session, "SELECT id, val FROM iceberg_merge_target ORDER BY id")
    assert {:ok, rows} = DataFrame.collect(result)
    assert Enum.map(rows, &{&1["id"], &1["val"]}) == [{1, "a"}, {2, "bb"}, {3, "c"}]
  end
end
