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
end
