defmodule SparkEx.Integration.ErrorHandlingTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "server error classes" do
    @describetag min_spark: "4.0"
    test "analysis exception includes class and parameters", %{session: session} do
      df = SparkEx.sql(session, "SELECT missing_column FROM range(1)")

      assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)
      assert error.error_class == "UNRESOLVED_COLUMN.WITH_SUGGESTION"
      assert error.sql_state == "42703"
      assert is_map(error.message_parameters)
      assert Map.has_key?(error.message_parameters, "objectName")

      assert Map.has_key?(error.message_parameters, "objectType") or
               Map.has_key?(error.message_parameters, "proposal")

      assert is_list(error.query_contexts)

      if error.query_contexts != [] do
        context = hd(error.query_contexts)
        assert is_binary(context.summary)
        assert String.contains?(context.summary, "missing_column")
        assert context.context_type in [:SQL, :DATAFRAME]
        assert context.object_type in [nil, "", "column"]
        assert context.object_name in [nil, "", "missing_column"]
        assert is_binary(context.fragment)
        assert String.contains?(context.fragment, "missing_column")

        if context.start_index != 0 or context.stop_index != 0 do
          assert context.start_index <= context.stop_index
        end

        if context.call_site != "" do
          assert is_binary(context.call_site)
        end
      end
    end

    test "parse exception returns structured error", %{session: session} do
      df = SparkEx.sql(session, "SELEC 1")

      assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)
      assert error.error_class == "PARSE_SYNTAX_ERROR"
      assert error.sql_state == "42601"
      assert is_list(error.query_contexts)

      if error.query_contexts != [] do
        context = hd(error.query_contexts)
        assert is_binary(context.summary)
      end
    end

    test "invalid star reports object metadata", %{session: session} do
      df = SparkEx.sql(session, "SELECT missing_alias.* FROM range(1)")

      assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)

      assert error.error_class in [
               "UNRESOLVED_COLUMN.WITH_SUGGESTION",
               "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
               "UNRESOLVED_STAR",
               "CANNOT_RESOLVE_STAR_EXPAND"
             ]

      assert is_map(error.message_parameters)

      assert Map.has_key?(error.message_parameters, "objectName") or
               Map.has_key?(error.message_parameters, "targetString")

      assert Map.has_key?(error.message_parameters, "objectType") or
               Map.has_key?(error.message_parameters, "columns")

      if error.query_contexts != [] do
        context = hd(error.query_contexts)
        assert context.object_type in [nil, "", "column", "*", "ambiguous"]
        assert is_binary(context.fragment)
      end
    end

    test "isin error includes message parameters", %{session: session} do
      import SparkEx.Functions

      df =
        SparkEx.range(session, 1)
        |> DataFrame.filter(SparkEx.Column.isin(col("missing"), [lit(1)]))

      assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)
      assert is_map(error.message_parameters)
      assert Map.has_key?(error.message_parameters, "objectName")

      assert Map.has_key?(error.message_parameters, "objectType") or
               Map.has_key?(error.message_parameters, "proposal")
    end

    test "between error includes message parameters", %{session: session} do
      import SparkEx.Functions

      df =
        SparkEx.range(session, 1)
        |> DataFrame.filter(SparkEx.Column.between(col("missing"), lit(1), lit(2)))

      assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)
      assert is_map(error.message_parameters)
      assert Map.has_key?(error.message_parameters, "objectName")

      assert Map.has_key?(error.message_parameters, "objectType") or
               Map.has_key?(error.message_parameters, "proposal")
    end

    test "illegal argument returns error class", %{session: session} do
      df = SparkEx.sql(session, "SELECT * FROM range(0, 5, 0)")

      assert {:error, %SparkEx.Error.Remote{} = error} = DataFrame.collect(df)

      assert error.error_class in [
               "FAILED_FUNCTION_CALL",
               "INVALID_PARAMETER_VALUE",
               "ILLEGAL_ARGUMENT_EXCEPTION"
             ]

      assert is_binary(error.sql_state)
    end
  end
end
