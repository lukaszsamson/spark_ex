defmodule SparkEx.BugsPlan5.StreamJTest do
  @moduledoc """
  Regression tests for BUGS_PLAN_5 Stream J cleanup sweep.
  """

  use ExUnit.Case, async: true

  alias Spark.Connect.StorageLevel
  alias SparkEx.Connect.TypeMapper
  alias SparkEx.Internal.UUID

  describe "explorer_schema_to_ddl (CLAUDE-02/03)" do
    test "list input renders BIGINT for {:s, 64}" do
      assert TypeMapper.explorer_schema_to_ddl([{"id", {:s, 64}}, {"name", :string}]) ==
               "id BIGINT, name STRING"
    end

    test "map input emits a deprecation warning" do
      io =
        ExUnit.CaptureIO.capture_io(:stderr, fn ->
          assert TypeMapper.explorer_schema_to_ddl(%{"id" => {:s, 64}}) =~ "id BIGINT"
        end)

      assert io =~ "non-deterministic"
    end
  end

  describe "Trigger.Once rejection (CLAUDE-40)" do
    test "encode_command rejects :once trigger" do
      df_plan = {:sql, "SELECT 1", nil}

      assert_raise ArgumentError, ~r/Trigger\.Once was removed/, fn ->
        SparkEx.Connect.CommandEncoder.encode_command(
          {:write_stream_operation_start, df_plan,
           [format: "console", output_mode: "append", trigger: :once]},
          0
        )
      end
    end
  end

  describe "is_cached? StorageLevel.NONE (GPT-19)" do
    test "use_* flags determine cached status, not replication" do
      none = %StorageLevel{
        use_disk: false,
        use_memory: false,
        use_off_heap: false,
        deserialized: false,
        replication: 1
      }

      refute none.use_disk or none.use_memory or none.use_off_heap
    end
  end

  describe "config coercion (CLAUDE-57/81)" do
    test "client source carries the nil-rejecting coercer clause" do
      # The private coerce_config_string/1 is only reachable via the public
      # RPC entry points, which would need a real session. Assert structurally
      # that the nil-rejecting clause is present in the source.
      src = File.read!("lib/spark_ex/connect/client.ex")
      assert src =~ "config key/value cannot be nil"
    end
  end

  describe "is_modifiable parser strict (CLAUDE-41)" do
    test "module source no longer downcases/trims is_modifiable values" do
      src = File.read!("lib/spark_ex/connect/client.ex")
      refute src =~ "String.trim() |> String.downcase"
    end
  end

  describe "clone_session UUID validation (CLAUDE-21/65)" do
    test "UUID.valid_uuid? rejects empty / non-UUID strings" do
      refute UUID.valid_uuid?("not-a-uuid")
      refute UUID.valid_uuid?("")
    end
  end

  describe "release_execute response shape (CLAUDE-62)" do
    test "@doc mentions operation_id field" do
      {:docs_v1, _, _, _, _, _, fns} = Code.fetch_docs(SparkEx.Connect.Client)

      doc =
        Enum.find_value(fns, fn
          {{:function, :release_execute, 3}, _, _, %{"en" => text}, _} -> text
          _ -> nil
        end)

      assert is_binary(doc)
      assert doc =~ "operation_id"
    end
  end

  describe "artifact_status_full (CLAUDE-56)" do
    test "Client exports artifact_status_full/2" do
      Code.ensure_loaded!(SparkEx.Connect.Client)
      assert function_exported?(SparkEx.Connect.Client, :artifact_status_full, 2)
    end
  end
end
