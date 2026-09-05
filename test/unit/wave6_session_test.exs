defmodule SparkEx.Unit.Wave6SessionTest do
  use ExUnit.Case, async: true

  alias SparkEx.Session

  @left {:plan_id, 0, {:sql, "SELECT * FROM VALUES (1), (2) AS t(id)", nil}}
  @right {:plan_id, 1, {:sql, "SELECT * FROM VALUES (1) AS t(rid)", nil}}
  @cond {:fn, "==", [col: "id", col: "rid"], false}

  describe "T-34: empty-relation rewrite walks the whole plan" do
    test "lateral join under a project inside plan_id envelopes becomes a join" do
      lateral = {:plan_id, 2, {:lateral_join, @left, @right, @cond, :inner}}
      plan = {:plan_id, 3, {:project, lateral, [col: "id"]}}

      assert {rewritten, true} = Session.__rewrite_empty_relation_deep__(plan)

      assert rewritten ==
               {:plan_id, 3,
                {:project, {:plan_id, 2, {:join, @left, @right, @cond, :inner, []}}, [col: "id"]}}
    end

    test "lateral join under a filter is rewritten" do
      lateral = {:plan_id, 2, {:lateral_join, @left, @right, @cond, :left}}
      plan = {:plan_id, 3, {:filter, lateral, {:fn, ">", [{:col, "id"}, {:lit, 1}], false}}}

      assert {{:plan_id, 3,
               {:filter, {:plan_id, 2, {:join, @left, @right, @cond, :left, []}}, _}}, true} =
               Session.__rewrite_empty_relation_deep__(plan)
    end

    test "as-of join is never downgraded to a plain join (semantics would be lost)" do
      as_of =
        {:plan_id, 2,
         {:as_of_join, @left, @right, {:col, "t1"}, {:col, "t2"}, @cond, [], "left", {:lit, nil},
          true, "backward"}}

      plan = {:plan_id, 3, {:filter, as_of, {:lit, true}}}

      assert {^plan, false} = Session.__rewrite_empty_relation_deep__(plan)

      assert {:error, {:unsupported_on_server, :as_of_join, message}} =
               Session.__rewrite_empty_relation_collect_plan__(plan)

      assert message =~ "Spark 4.0+"
    end

    test "transpose under sort and project is emulated with unpivot + pivot" do
      transpose = {:plan_id, 2, {:transpose, @left, [col: "id"]}}

      plan =
        {:plan_id, 4,
         {:project,
          {:plan_id, 3, {:sort, transpose, [{:sort_order, {:col, "id"}, :asc, :nulls_first}]}},
          [col: "id"]}}

      assert {{:plan_id, 4, {:project, {:plan_id, 3, {:sort, {:plan_id, 2, emulated}, _}}, _}},
              true} = Session.__rewrite_empty_relation_deep__(plan)

      assert {:sort, {:aggregate, {:unpivot, @left, [col: "id"], nil, _, _}, :pivot, _, _, _, _},
              _} =
               emulated
    end

    test "table-valued function nested under a limit becomes SQL" do
      tvf = {:plan_id, 0, {:table_valued_function, "range", [{:lit, 3}]}}
      plan = {:plan_id, 1, {:limit, tvf, 2}}

      assert {{:plan_id, 1, {:limit, {:plan_id, 0, {:sql, sql, []}}, 2}}, true} =
               Session.__rewrite_empty_relation_deep__(plan)

      assert sql =~ ~r/^SELECT \* FROM range\(/
    end

    test "nested unsupported relations are rewritten in one pass" do
      transpose = {:plan_id, 2, {:transpose, @right, [col: "rid"]}}
      lateral = {:plan_id, 3, {:lateral_join, @left, transpose, @cond, :inner}}
      plan = {:plan_id, 4, {:project, lateral, [col: "id"]}}

      assert {{:plan_id, 4,
               {:project,
                {:plan_id, 3, {:join, @left, {:plan_id, 2, emulated}, @cond, :inner, []}}, _}},
              true} = Session.__rewrite_empty_relation_deep__(plan)

      assert {:sort, {:aggregate, _, :pivot, _, _, _, _}, _} = emulated
    end

    test "both branches of a union are visited" do
      lateral_a = {:plan_id, 2, {:lateral_join, @left, @right, @cond, :inner}}
      lateral_b = {:plan_id, 3, {:lateral_join, @right, @left, @cond, :inner}}
      plan = {:plan_id, 4, {:set_operation, lateral_a, lateral_b, :union, true}}

      assert {{:plan_id, 4,
               {:set_operation, {:plan_id, 2, {:join, _, _, _, _, _}},
                {:plan_id, 3, {:join, _, _, _, _, _}}, :union, true}}, true} =
               Session.__rewrite_empty_relation_deep__(plan)
    end

    test "plans without unsupported relations are returned unchanged" do
      plan =
        {:plan_id, 2,
         {:project, {:plan_id, 1, {:join, @left, @right, @cond, :inner, []}}, [col: "id"]}}

      assert {^plan, false} = Session.__rewrite_empty_relation_deep__(plan)
      assert {[], false} = Session.__rewrite_empty_relation_deep__([])
      assert {"x", false} = Session.__rewrite_empty_relation_deep__("x")
    end

    test "a transpose the emulation cannot express is left alone" do
      plan = {:plan_id, 2, {:transpose, @left, [col: "a", col: "b"]}}
      assert {^plan, false} = Session.__rewrite_empty_relation_deep__(plan)
    end
  end

  describe "T-64: local relation configs" do
    @keys [
      "spark.sql.session.localRelationCacheThreshold",
      "spark.sql.session.localRelationChunkSizeRows",
      "spark.sql.session.localRelationChunkSizeBytes",
      "spark.sql.session.localRelationBatchOfChunksSizeBytes"
    ]

    test "parses every known config from the server response" do
      pairs = Enum.zip(@keys, ["1048576", "10000", "16777216", "1073741824"])

      assert Session.__parse_local_relation_configs__(pairs) == %{
               cache_threshold: 1_048_576,
               chunk_size_rows: 10_000,
               chunk_size_bytes: 16_777_216,
               batch_of_chunks_size_bytes: 1_073_741_824
             }
    end

    test "missing chunking keys (Spark 3.5) keep every default, including the threshold" do
      pairs = Enum.zip(@keys, ["67108864", nil, nil, nil])

      # Spark 3.5 reports a threshold but cannot accept chunked cached
      # relations, so the client-side default is kept and payloads stay inlined.
      assert Session.__parse_local_relation_configs__(pairs) == %{
               cache_threshold: 4 * 1024 * 1024,
               chunk_size_rows: 10_000,
               chunk_size_bytes: 16 * 1024 * 1024,
               batch_of_chunks_size_bytes: 1024 * 1024 * 1024
             }
    end

    test "unparsable or negative values keep the defaults" do
      pairs = Enum.zip(@keys, ["abc", "-1", "12abc", ""])
      defaults = Session.__parse_local_relation_configs__([])
      assert Session.__parse_local_relation_configs__(pairs) == defaults
      assert defaults.cache_threshold == 4 * 1024 * 1024
      assert Session.__parse_local_relation_configs__(nil) == defaults
    end

    test "chunk params: explicit options override server configs" do
      configs = Session.__parse_local_relation_configs__([])

      assert {:ok,
              %{
                cache_threshold: 10,
                chunk_size_bytes: 7,
                chunk_size_rows: 3,
                batch_of_chunks_size_bytes: 1_073_741_824
              }} =
               Session.__local_relation_chunk_params__(
                 [cache_threshold: 10, cache_chunk_size: 7, cache_chunk_rows: 3],
                 configs
               )
    end

    test "chunk params: default byte cap is min(chunkSizeBytes, batchOfChunksSizeBytes)" do
      configs = %{
        cache_threshold: 100,
        chunk_size_rows: 50,
        chunk_size_bytes: 4000,
        batch_of_chunks_size_bytes: 1000
      }

      assert {:ok,
              %{chunk_size_bytes: 1000, chunk_size_rows: 50, batch_of_chunks_size_bytes: 1000}} =
               Session.__local_relation_chunk_params__([], configs)
    end

    test "chunk params: invalid options are rejected" do
      configs = Session.__parse_local_relation_configs__([])

      assert {:error, {:invalid_option, {:cache_threshold, -1}}} =
               Session.__local_relation_chunk_params__([cache_threshold: -1], configs)

      assert {:error, {:invalid_option, {:cache_chunk_size, 0}}} =
               Session.__local_relation_chunk_params__([cache_chunk_size: 0], configs)

      assert {:error, {:invalid_option, {:cache_chunk_rows, :nope}}} =
               Session.__local_relation_chunk_params__([cache_chunk_rows: :nope], configs)
    end

    test "rows per chunk is bounded by bytes and by rows" do
      # 256 sample rows at 4 bytes each; 100-byte chunks hold 25 rows
      assert Session.__local_relation_rows_per_chunk__(1024, 256, 100, nil) == 25
      assert Session.__local_relation_rows_per_chunk__(1024, 256, 100, 10) == 10
      assert Session.__local_relation_rows_per_chunk__(1024, 256, 100, 1000) == 25
      # a chunk smaller than one row still ships one row
      assert Session.__local_relation_rows_per_chunk__(1024, 256, 1, nil) == 1
      # degenerate sample sizes never divide by zero
      assert Session.__local_relation_rows_per_chunk__(0, 0, 100, nil) == 100
    end

    test "artifacts are grouped into upload batches by summed size" do
      artifacts = [{"a", "12345"}, {"b", "12345"}, {"c", "123456789012"}, {"d", "1"}]

      assert Session.__batch_cache_artifacts__(artifacts, 10) == [
               [{"a", "12345"}, {"b", "12345"}],
               [{"c", "123456789012"}],
               [{"d", "1"}]
             ]

      assert Session.__batch_cache_artifacts__(artifacts, 1_000) == [artifacts]
      assert Session.__batch_cache_artifacts__([], 10) == []
    end

    test "split_explorer_dataframe_for_cache caps chunks by rows" do
      df = Explorer.DataFrame.new(%{"id" => Enum.to_list(1..10)})

      assert {:ok, chunks} = Session.split_explorer_dataframe_for_cache(df, 10_000_000, 3)
      assert length(chunks) == 4

      row_counts =
        Enum.map(chunks, fn ipc ->
          ipc |> Explorer.DataFrame.load_ipc_stream!() |> Explorer.DataFrame.n_rows()
        end)

      assert row_counts == [3, 3, 3, 1]

      # without a row cap the payload fits in one chunk
      assert {:ok, [_single]} = Session.split_explorer_dataframe_for_cache(df, 10_000_000)
    end
  end

  describe "T-64: threshold adoption is gated on the 4.1 chunking configs" do
    test "server threshold ignored when localRelationChunkSizeRows is absent (Spark 3.5)" do
      configs =
        SparkEx.Session.__parse_local_relation_configs__([
          {"spark.sql.session.localRelationCacheThreshold", "1048576"},
          {"spark.sql.session.localRelationChunkSizeRows", nil}
        ])

      assert configs.cache_threshold == 4 * 1024 * 1024
    end

    test "server threshold adopted when the chunking configs are reported (Spark 4.1)" do
      configs =
        SparkEx.Session.__parse_local_relation_configs__([
          {"spark.sql.session.localRelationCacheThreshold", "1048576"},
          {"spark.sql.session.localRelationChunkSizeRows", "10000"}
        ])

      assert configs.cache_threshold == 1_048_576
      assert configs.chunk_size_rows == 10_000
    end
  end
end
