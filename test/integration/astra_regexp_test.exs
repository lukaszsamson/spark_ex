defmodule SparkEx.Integration.AstraRegexpTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.DataFrame
  alias SparkEx.Column
  alias SparkEx.Functions

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  describe "regexp pattern arguments name columns" do
    test "regexp_instr / regexp_substr / regexp_extract_all read the pattern column", %{
      session: session
    } do
      df = SparkEx.sql(session, "SELECT 'abc123' AS s, '[0-9]+' AS p")

      selected =
        DataFrame.select(df, [
          Column.alias_(Functions.regexp_instr("s", "p"), "instr"),
          Column.alias_(Functions.regexp_substr("s", "p"), "substr"),
          Column.alias_(Functions.regexp_extract_all("s", "p", 0), "all")
        ])

      assert {:ok, [row]} = DataFrame.collect(selected)
      assert row["instr"] == 4
      assert row["substr"] == "123"
      assert row["all"] == ["123"]
    end

    test "lit/1 still gives a literal pattern", %{session: session} do
      df = SparkEx.sql(session, "SELECT 'abc123' AS s")

      selected =
        DataFrame.select(df, [
          Column.alias_(Functions.regexp_substr("s", Functions.lit("[0-9]+")), "substr")
        ])

      assert {:ok, [%{"substr" => "123"}]} = DataFrame.collect(selected)
    end
  end
end
