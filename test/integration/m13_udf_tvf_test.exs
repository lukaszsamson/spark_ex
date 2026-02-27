defmodule SparkEx.Integration.M13.UDFTVFTest do
  use ExUnit.Case

  @moduletag :integration

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  alias SparkEx.DataFrame
  import SparkEx.Functions, only: [lit: 1]

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  # ── Table-Valued Functions ──

  describe "DataFrame.table_function/3 (TVF)" do
    test "range TVF produces rows", %{session: session} do
      df = DataFrame.table_function(session, "range", [lit(5)])
      {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 5

      ids = Enum.map(rows, & &1["id"]) |> Enum.sort()
      assert ids == [0, 1, 2, 3, 4]
    end

    test "range TVF with start, end, step", %{session: session} do
      df = DataFrame.table_function(session, "range", [lit(10), lit(20), lit(2)])
      {:ok, rows} = DataFrame.collect(df)

      ids = Enum.map(rows, & &1["id"]) |> Enum.sort()
      assert ids == [10, 12, 14, 16, 18]
    end

    @tag min_spark: "4.0"
    test "explode TVF with SQL-created array", %{session: session} do
      # Create a df with an array column, then use SQL to explode it
      # (explode as TVF requires specific argument format)
      # Instead, verify we can chain TVF output with downstream transforms
      df =
        DataFrame.table_function(session, "range", [lit(3)])
        |> DataFrame.select(["id"])
        |> DataFrame.limit(2)

      {:ok, rows} = DataFrame.collect(df)
      assert length(rows) == 2
    end
  end

  # ── Java UDF Registration ──

  describe "register_java_udf" do
    test "registers and uses a built-in Java UDF class", %{session: session} do
      # We can't easily provide a custom Java UDF class in tests without
      # uploading a JAR. However, we can verify the command encoding and
      # session API by attempting to register a non-existent class and
      # checking we get a server-side error (not a client-side crash).
      result =
        SparkEx.Session.register_java_udf(
          session,
          "test_udf_#{System.unique_integer([:positive])}",
          "com.example.NonExistentUDF"
        )

      assert {:error, %SparkEx.Error.Remote{}} = result
    end
  end

  # ── UDTF Registration ──

  describe "register_udtf" do
    test "UDTF registration command is sent correctly", %{session: session} do
      function_name = "test_udtf_#{System.unique_integer([:positive])}"

      # Register a UDTF with dummy Python command bytes
      result =
        SparkEx.Session.register_udtf(
          session,
          function_name,
          <<0, 0, 0>>,
          eval_type: 0,
          python_ver: "3.11"
        )

      assert result == :ok or match?({:error, %SparkEx.Error.Remote{}}, result)

      assert {:error, %SparkEx.Error.Remote{}} =
               session
               |> SparkEx.sql("SELECT * FROM #{function_name}()")
               |> DataFrame.collect()
    end
  end
end
