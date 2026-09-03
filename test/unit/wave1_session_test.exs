defmodule SparkEx.Unit.Wave1SessionTest do
  @moduledoc """
  Regression tests for the wave-1 triaged session/types fixes:
  T-01, T-02, T-04 (session half), T-10, T-12, T-27.
  """
  use ExUnit.Case, async: false

  alias SparkEx.Session
  alias SparkEx.Types

  # Starts a Session GenServer connected to a throwaway local TCP listener.
  # The gRPC connection is lazy, so no real Spark server is needed for the
  # local validation paths exercised here.
  defp start_fake_session(opts \\ []) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, {:active, false}, {:reuseaddr, true}])
    {:ok, port} = :inet.port(listener)

    acceptor =
      spawn_link(fn ->
        case :gen_tcp.accept(listener) do
          {:ok, _socket} -> Process.sleep(:infinity)
          _ -> :ok
        end
      end)

    {:ok, session} =
      SparkEx.connect(Keyword.put_new(opts, :url, "sc://localhost:#{port}"))

    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session) do
        try do
          GenServer.stop(session, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
      end

      Process.exit(acceptor, :kill)
      :gen_tcp.close(listener)
    end)

    session
  end

  describe "T-01: artifact_status/2 validates names before entering the GenServer" do
    test "a bare binary is rejected without touching the session" do
      session = start_fake_session()

      assert {:error, {:invalid_artifact_names, "x"}} = Session.artifact_status(session, "x")
      assert Process.alive?(session)
    end

    test "a list with non-binary elements is rejected" do
      session = start_fake_session()

      assert {:error, {:invalid_artifact_names, [:a]}} = Session.artifact_status(session, [:a])
      assert {:error, {:invalid_artifact_names, nil}} = Session.artifact_status(session, nil)
      assert Process.alive?(session)
    end
  end

  describe "T-02: execute_explorer max_rows handling" do
    test "invalid max_rows returns {:error, {:invalid_option, _}} and keeps the session alive" do
      session = start_fake_session()

      for bad <- [-1, :unbounded, "10", 1.5] do
        assert {:error, {:invalid_option, {:max_rows, ^bad}}} =
                 Session.execute_explorer(session, {:range, 0, 10, 1, nil}, max_rows: bad)
      end

      assert Process.alive?(session)
    end

    test ":infinity is not injected as a remote LIMIT (encode does not crash the session)" do
      session = start_fake_session()

      # The plan encodes fine (no int32 LIMIT with :infinity); the call then
      # fails at the transport layer against the fake listener, but the
      # session must survive rather than dying on protobuf encode.
      result =
        Session.execute_explorer(session, {:range, 0, 10, 1, nil},
          max_rows: :infinity,
          timeout: 500
        )

      assert match?({:error, _}, result)
      refute match?({:error, {:invalid_option, _}}, result)
      assert Process.alive?(session)
    end

    test "unsafe: true does not bypass max_rows validation" do
      session = start_fake_session()

      assert {:error, {:invalid_option, {:max_rows, "10"}}} =
               Session.execute_explorer(session, {:range, 0, 10, 1, nil},
                 unsafe: true,
                 max_rows: "10",
                 timeout: 500
               )
    end
  end

  describe "T-10: backslash escapes" do
    test "a backslash-escaped quote inside a comment does not end the quoted run" do
      assert Session.__split_top_level_schema_fields__("a STRING COMMENT 'it\\'s, x', b INT") == [
               "a STRING COMMENT 'it\\'s, x'",
               "b INT"
             ]
    end
  end

  describe "T-04: Explorer non-finite float sentinels infer as DOUBLE" do
    test "nan / infinity / neg_infinity infer as :double and merge with floats" do
      assert Session.__infer_value_type__([:nan]) == :double
      assert Session.__infer_value_type__([:infinity]) == :double
      assert Session.__infer_value_type__([:neg_infinity]) == :double
      assert Session.__infer_value_type__([1.5, :nan, nil]) == :double
      assert Session.__infer_value_type__([1, :infinity]) == :double
    end

    test "JSON row normalization emits the literals Spark's from_json accepts for DOUBLE" do
      assert {:ok, [row]} =
               Session.__normalize_rows_for_schema__([
                 %{"a" => :nan, "b" => :infinity, "c" => :neg_infinity, "d" => [:nan]}
               ])

      assert row == %{"a" => "NaN", "b" => "Infinity", "c" => "-Infinity", "d" => ["NaN"]}
      assert {:ok, _} = Jason.encode(row)
    end
  end

  describe "T-10: DDL top-level field splitter" do
    test "commas inside parentheses and single-quoted comments do not split" do
      assert Session.__split_top_level_schema_fields__(
               "a DECIMAL(10, 2) COMMENT 'money, honey', b STRING"
             ) == ["a DECIMAL(10, 2) COMMENT 'money, honey'", "b STRING"]
    end

    test "commas inside backtick-quoted identifiers do not split" do
      assert Session.__split_top_level_schema_fields__("`a,b` INT, c STRING") ==
               ["`a,b` INT", "c STRING"]
    end

    test "doubled-quote escapes keep the quoted run open" do
      assert Session.__split_top_level_schema_fields__(
               "a STRING COMMENT 'it''s, quoted', `we``ird,` INT, b STRING"
             ) == ["a STRING COMMENT 'it''s, quoted'", "`we``ird,` INT", "b STRING"]

      assert Session.__split_top_level_schema_fields__(~s(a STRING COMMENT "x, ""y"", z", b INT)) ==
               [~s(a STRING COMMENT "x, ""y"", z"), "b INT"]
    end

    test "angle-bracket nesting still works" do
      assert Session.__split_top_level_schema_fields__(
               "s STRUCT<x: INT, y: MAP<STRING, ARRAY<DECIMAL(5, 1)>>>, t STRING"
             ) == ["s STRUCT<x: INT, y: MAP<STRING, ARRAY<DECIMAL(5, 1)>>>", "t STRING"]
    end
  end

  describe "T-12: parse fallback rewrites every changed sibling" do
    test "both branches of a union of parse relations are rewritten" do
      parse_a = {:parse, {:range, 0, 1, 1, nil}, :json, "a INT", []}
      parse_b = {:parse, {:range, 0, 2, 1, nil}, :csv, "b INT", []}

      plan =
        {:plan_id, 3, {:union, {:plan_id, 1, parse_a}, {:plan_id, 2, parse_b}, [is_all: true]}}

      rewriter = fn {:parse, child, format, _schema, _opts} ->
        {{:rewritten, format, child}, true}
      end

      assert {rewritten, true} = Session.__rewrite_parse_walk__(plan, rewriter)

      assert rewritten ==
               {:plan_id, 3,
                {:union, {:plan_id, 1, {:rewritten, :json, {:range, 0, 1, 1, nil}}},
                 {:plan_id, 2, {:rewritten, :csv, {:range, 0, 2, 1, nil}}}, [is_all: true]}}
    end

    test "siblings in a list are all visited" do
      parse = {:parse, {:range, 0, 1, 1, nil}, :json, "a INT", []}
      rewriter = fn {:parse, _, _, _, _} -> {:done, true} end

      assert {[:done, :x, :done], true} =
               Session.__rewrite_parse_walk__([parse, :x, parse], rewriter)
    end

    test "unchanged trees report false" do
      rewriter = fn node -> {node, false} end

      assert {{:range, 0, 1, 1, nil}, false} =
               Session.__rewrite_parse_walk__({:range, 0, 1, 1, nil}, rewriter)
    end
  end

  describe "T-27: unset decimal precision/scale in proto -> JSON" do
    test "defaults to decimal(10,0)" do
      proto = %Spark.Connect.DataType{
        kind: {:decimal, %Spark.Connect.DataType.Decimal{precision: nil, scale: nil}}
      }

      assert Jason.decode!(Types.data_type_to_json(proto)) == "decimal(10,0)"
    end

    test "precision-only defaults scale to 0; explicit values are preserved" do
      precision_only = %Spark.Connect.DataType{
        kind: {:decimal, %Spark.Connect.DataType.Decimal{precision: 12, scale: nil}}
      }

      assert Jason.decode!(Types.data_type_to_json(precision_only)) == "decimal(12,0)"

      explicit = %Spark.Connect.DataType{
        kind: {:decimal, %Spark.Connect.DataType.Decimal{precision: 7, scale: 3}}
      }

      assert Jason.decode!(Types.data_type_to_json(explicit)) == "decimal(7,3)"
    end
  end
end
