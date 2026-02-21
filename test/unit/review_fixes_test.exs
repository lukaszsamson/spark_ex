defmodule SparkEx.ReviewFixesTest do
  use ExUnit.Case, async: true

  alias SparkEx.StreamWriter
  alias SparkEx.EtsTableOwner

  # ── #7: timeout validation ──

  describe "Session.call_timeout (via public API validation)" do
    # We test the call_timeout logic indirectly through ArgumentError validation
    # since call_timeout is private. The fix ensures nil and :infinity don't crash.
    test "nil timeout should not cause arithmetic crash" do
      # This tests the fix where `timeout + 5000` would crash for nil.
      # We can't call Session directly without a real session, but we verify
      # the contract at the StreamingQuery level.
      query = %SparkEx.StreamingQuery{
        session: self(),
        query_id: "q1",
        run_id: "r1",
        name: nil
      }

      # The call should not crash with ArithmeticError; it may fail
      # for other reasons (not a real session) but not arithmetic.
      try do
        SparkEx.StreamingQuery.await_termination(query, timeout: nil)
      rescue
        ArgumentError -> :ok
      catch
        :exit, _ -> :ok
      end
    end
  end

  # ── #8: trigger validation ──

  describe "StreamWriter.trigger/2 validation" do
    setup do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      writer = %StreamWriter{df: df}
      %{writer: writer}
    end

    test "rejects once: false", %{writer: writer} do
      assert_raise ArgumentError, ~r/once must be true/, fn ->
        StreamWriter.trigger(writer, once: false)
      end
    end

    test "rejects available_now: false", %{writer: writer} do
      assert_raise ArgumentError, ~r/available_now must be true/, fn ->
        StreamWriter.trigger(writer, available_now: false)
      end
    end

    test "rejects processing_time: nil", %{writer: writer} do
      assert_raise ArgumentError, ~r/processing_time must be a non-empty string/, fn ->
        StreamWriter.trigger(writer, processing_time: nil)
      end
    end

    test "rejects continuous: nil", %{writer: writer} do
      assert_raise ArgumentError, ~r/continuous must be a non-empty string/, fn ->
        StreamWriter.trigger(writer, continuous: nil)
      end
    end

    test "accepts once: true", %{writer: writer} do
      result = StreamWriter.trigger(writer, once: true)
      assert result.trigger == :once
    end

    test "accepts available_now: true", %{writer: writer} do
      result = StreamWriter.trigger(writer, available_now: true)
      assert result.trigger == :available_now
    end

    test "accepts processing_time with valid string", %{writer: writer} do
      result = StreamWriter.trigger(writer, processing_time: "5 seconds")
      assert result.trigger == {:processing_time, "5 seconds"}
    end

    test "trims processing_time whitespace", %{writer: writer} do
      result = StreamWriter.trigger(writer, processing_time: "  5 seconds  ")
      assert result.trigger == {:processing_time, "5 seconds"}
    end

    test "accepts continuous with valid string", %{writer: writer} do
      result = StreamWriter.trigger(writer, continuous: "1 second")
      assert result.trigger == {:continuous, "1 second"}
    end

    test "rejects multiple triggers", %{writer: writer} do
      assert_raise ArgumentError, ~r/only one trigger/, fn ->
        StreamWriter.trigger(writer, once: true, available_now: true)
      end
    end

    test "rejects empty trigger opts", %{writer: writer} do
      assert_raise ArgumentError, ~r/expected one of/, fn ->
        StreamWriter.trigger(writer, [])
      end
    end
  end

  # ── #6: ETS table verification after rescue ──

  describe "EtsTableOwner.ensure_table!" do
    test "succeeds when table already exists" do
      # Create a table first, then ensure_table! should be idempotent
      table = :spark_ex_test_ensure_existing
      :ets.new(table, [:named_table, :public, :set])
      assert :ok = EtsTableOwner.ensure_table!(table, :set)
      :ets.delete(table)
    end

    test "creates table when it does not exist" do
      table = :spark_ex_test_ensure_new
      assert :ok = EtsTableOwner.ensure_table!(table, :set)
      assert :ets.whereis(table) != :undefined
      :ets.delete(table)
    end
  end

  # ── #12: SQL injection prevention ──

  describe "Catalog DDL SQL injection prevention" do
    test "build_drop_table_sql quotes malicious table names" do
      sql = SparkEx.Catalog.build_drop_table_sql("t1; DROP TABLE t2", if_exists: true)
      # The injection payload is safely inside backtick-quoted identifier
      assert sql == "DROP TABLE IF EXISTS `t1; DROP TABLE t2`"
    end

    test "build_create_database_sql quotes identifiers" do
      sql = SparkEx.Catalog.build_create_database_sql("db; --", [])
      assert sql =~ "`db; --`"
    end

    test "build_drop_function_sql quotes function names" do
      sql = SparkEx.Catalog.build_drop_function_sql("f; DROP TABLE t", [])
      assert sql =~ "`f; DROP TABLE t`"
    end
  end

  # ── REV_OPUS fixes ──

  describe "CommandEncoder pos_arguments (REV_OPUS #7)" do
    test "uses pos_arguments field with Expression instead of pos_args with Literal" do
      {plan, _counter} =
        SparkEx.Connect.CommandEncoder.encode({:sql_command, "SELECT ?", [42]}, 0)

      %Spark.Connect.Plan{op_type: {:command, cmd}} = plan
      {:sql_command, sql_cmd} = cmd.command_type
      assert sql_cmd.pos_arguments != []
      assert sql_cmd.pos_args == []
      [expr] = sql_cmd.pos_arguments
      assert %Spark.Connect.Expression{expr_type: {:literal, _}} = expr
    end
  end

  describe "Column.substr mixed types (REV_OPUS #33)" do
    test "accepts Column pos with integer len" do
      c = SparkEx.Functions.col("s")
      result = SparkEx.Column.substr(c, SparkEx.Functions.lit(1), 5)
      assert %SparkEx.Column{expr: {:fn, "substr", [_, _, {:lit, 5}], false}} = result
    end

    test "accepts integer pos with Column len" do
      c = SparkEx.Functions.col("s")
      result = SparkEx.Column.substr(c, 1, SparkEx.Functions.lit(5))
      assert %SparkEx.Column{expr: {:fn, "substr", [_, {:lit, 1}, _], false}} = result
    end

    test "rejects invalid types" do
      c = SparkEx.Functions.col("s")

      assert_raise ArgumentError, ~r/Column or integer/, fn ->
        SparkEx.Column.substr(c, "1", "5")
      end
    end
  end

  describe "StreamWriter.output_mode catch-all (REV_OPUS #57)" do
    test "rejects invalid output mode string" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      writer = %SparkEx.StreamWriter{df: df}

      assert_raise ArgumentError, ~r/must be one of/, fn ->
        SparkEx.StreamWriter.output_mode(writer, "invalid")
      end
    end
  end

  describe "Writer.mode catch-all (REV_OPUS #60)" do
    test "rejects invalid atom mode" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      writer = %SparkEx.Writer{df: df}

      assert_raise ArgumentError, ~r/unknown save mode/, fn ->
        SparkEx.Writer.mode(writer, :invalid_mode)
      end
    end

    test "rejects invalid string mode" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      writer = %SparkEx.Writer{df: df}

      assert_raise ArgumentError, ~r/unknown save mode/, fn ->
        SparkEx.Writer.mode(writer, "nope")
      end
    end
  end

  describe "Observation UUID dedup (REV_OPUS #14)" do
    test "new/0 generates valid UUID using Internal.UUID" do
      obs = SparkEx.Observation.new()
      assert obs.name =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
    end
  end

  describe "Functions.lit/1 Column pass-through (REV_OPUS #15)" do
    test "returns Column unchanged" do
      c = SparkEx.Functions.col("x")
      assert SparkEx.Functions.lit(c) == c
    end
  end

  describe "PlanEncoder sort ordering consistency (REV_OPUS #58)" do
    test "encode_sort_order uses SORT_NULLS_FIRST for bare col expressions" do
      sort = {:sort_order, {:col, "name"}, :asc, nil}
      expr = SparkEx.Connect.PlanEncoder.encode_expression(sort)
      assert %Spark.Connect.Expression{expr_type: {:sort_order, so}} = expr
      assert so.null_ordering == :SORT_NULLS_FIRST
    end
  end

  # ── decode_stream_arrow memory limits (REV_OPUS #55) ──

  describe "decode_stream_arrow max_bytes enforcement" do
    test "enforces max_bytes limit" do
      alias Spark.Connect.ExecutePlanResponse
      alias Spark.Connect.ExecutePlanResponse.ArrowBatch

      big_data = :binary.copy(<<0>>, 1000)

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch, %ArrowBatch{data: big_data, row_count: 10, start_offset: 0}},
           schema: nil,
           observed_metrics: [],
           metrics: nil
         }}
      ]

      assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :bytes}} =
               SparkEx.Connect.ResultDecoder.decode_stream_arrow(stream, nil, max_bytes: 500)
    end

    test "enforces max_rows limit" do
      alias Spark.Connect.ExecutePlanResponse
      alias Spark.Connect.ExecutePlanResponse.ArrowBatch

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch, %ArrowBatch{data: <<1, 2, 3>>, row_count: 100, start_offset: 0}},
           schema: nil,
           observed_metrics: [],
           metrics: nil
         }}
      ]

      assert {:error, %SparkEx.Error.LimitExceeded{limit_type: :rows}} =
               SparkEx.Connect.ResultDecoder.decode_stream_arrow(stream, nil, max_rows: 50)
    end

    test "passes with limits not exceeded" do
      alias Spark.Connect.ExecutePlanResponse
      alias Spark.Connect.ExecutePlanResponse.ArrowBatch

      stream = [
        {:ok,
         %ExecutePlanResponse{
           response_type:
             {:arrow_batch, %ArrowBatch{data: <<1, 2, 3>>, row_count: 5, start_offset: 0}},
           schema: nil,
           observed_metrics: [],
           metrics: nil
         }},
        {:ok,
         %ExecutePlanResponse{
           response_type: {:result_complete, %ExecutePlanResponse.ResultComplete{}},
           schema: nil,
           observed_metrics: [],
           metrics: nil
         }}
      ]

      assert {:ok, result} =
               SparkEx.Connect.ResultDecoder.decode_stream_arrow(stream, nil,
                 max_rows: 100,
                 max_bytes: 10_000
               )

      assert is_binary(result.arrow)
    end
  end

  # ── Writer partition_by empty list (REV_OPUS #37) ──

  describe "Writer.partition_by empty list validation" do
    test "rejects empty column list" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      writer = %SparkEx.Writer{df: df}

      assert_raise ArgumentError, ~r/partition_by columns should not be empty/, fn ->
        SparkEx.Writer.partition_by(writer, [])
      end
    end
  end

  # ── DataFrame.drop type validation (REV_OPUS #46) ──

  describe "DataFrame.drop type validation" do
    test "rejects non-string/atom/Column values" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}

      assert_raise ArgumentError, ~r/drop expects column names/, fn ->
        SparkEx.DataFrame.drop(df, [123])
      end
    end

    test "accepts string column names" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      result = SparkEx.DataFrame.drop(df, ["col1", "col2"])
      assert %SparkEx.DataFrame{plan: {:drop, _, ["col1", "col2"], []}} = result
    end

    test "accepts atom column names" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      result = SparkEx.DataFrame.drop(df, [:col1])
      assert %SparkEx.DataFrame{plan: {:drop, _, ["col1"], []}} = result
    end
  end

  # ── Column.otherwise improved error message (REV_OPUS #48) ──

  describe "Column.otherwise error message (REV_OPUS #48)" do
    test "error says 'already been called' not 'can only be called once'" do
      col =
        SparkEx.Functions.when_(
          SparkEx.Functions.col("x") |> SparkEx.Column.gt(0),
          SparkEx.Functions.lit("pos")
        )
        |> SparkEx.Column.otherwise("zero")

      assert_raise ArgumentError, ~r/already been called/, fn ->
        SparkEx.Column.otherwise(col, "neg")
      end
    end
  end

  # ── DataFrame.normalize_column_expr negative integer guard (REV_OPUS #47) ──

  describe "DataFrame.normalize_column_expr negative integer (REV_OPUS #47)" do
    test "rejects negative integer column index" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}

      assert_raise FunctionClauseError, fn ->
        SparkEx.DataFrame.select(df, [-1])
      end
    end

    test "accepts non-negative integer column index" do
      df = %SparkEx.DataFrame{session: self(), plan: {:sql, "SELECT 1", nil}}
      result = SparkEx.DataFrame.select(df, [0])
      assert %SparkEx.DataFrame{} = result
    end
  end

  # ── ProgressHandlerRegistry dedup (REV_OPUS #20) ──

  describe "ProgressHandlerRegistry dedup (REV_OPUS #20)" do
    test "duplicate registration is idempotent" do
      session_id = "test-dedup-#{System.unique_integer([:positive])}"
      handler = fn _payload -> :ok end

      :ok = SparkEx.ProgressHandlerRegistry.register(session_id, handler)
      :ok = SparkEx.ProgressHandlerRegistry.register(session_id, handler)

      # Should have only one entry
      entries = :ets.lookup(:spark_ex_progress_handlers, session_id)
      assert length(entries) == 1

      # Cleanup
      SparkEx.ProgressHandlerRegistry.clear(session_id)
    end
  end

  # ── TypeMapper.data_type_to_ddl preserves decimal precision (REV_OPUS #29) ──

  describe "TypeMapper.data_type_to_ddl decimal precision (REV_OPUS #29)" do
    alias Spark.Connect.DataType

    test "preserves DECIMAL precision and scale" do
      dt = %DataType{kind: {:decimal, %DataType.Decimal{precision: 10, scale: 2}}}
      assert SparkEx.Connect.TypeMapper.data_type_to_ddl(dt) == "DECIMAL(10, 2)"
    end

    test "defaults DECIMAL scale to 0 when only precision given" do
      dt = %DataType{kind: {:decimal, %DataType.Decimal{precision: 18, scale: nil}}}
      assert SparkEx.Connect.TypeMapper.data_type_to_ddl(dt) == "DECIMAL(18, 0)"
    end

    test "defaults DECIMAL to (10, 0) when no precision" do
      dt = %DataType{kind: {:decimal, %DataType.Decimal{}}}
      assert SparkEx.Connect.TypeMapper.data_type_to_ddl(dt) == "DECIMAL(10, 0)"
    end

    test "direct DDL for timestamp preserves type name" do
      dt = %DataType{kind: {:timestamp, %DataType.Timestamp{}}}
      assert SparkEx.Connect.TypeMapper.data_type_to_ddl(dt) == "TIMESTAMP"
    end

    test "direct DDL for null returns VOID" do
      dt = %DataType{kind: nil}
      assert SparkEx.Connect.TypeMapper.data_type_to_ddl(dt) == "VOID"
    end
  end

  # ── PlanEncoder multi-name alias with metadata (REV_OPUS #52) ──

  describe "PlanEncoder multi-name alias with metadata (REV_OPUS #52)" do
    test "raises for multi-name alias with metadata" do
      assert_raise ArgumentError, ~r/cannot provide metadata for multi-name alias/, fn ->
        SparkEx.Connect.PlanEncoder.encode_expression(
          {:alias, {:col, "x"}, ["a", "b"], "{\"key\": \"val\"}"}
        )
      end
    end

    test "multi-name alias without metadata works" do
      result = SparkEx.Connect.PlanEncoder.encode_expression({:alias, {:col, "x"}, ["a", "b"]})
      assert %Spark.Connect.Expression{expr_type: {:alias, alias}} = result
      assert alias.name == ["a", "b"]
    end
  end
end
