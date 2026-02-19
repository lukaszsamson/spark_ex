defmodule SparkEx.Integration.FunctionParityTest do
  use ExUnit.Case

  @moduletag :integration

  alias SparkEx.{DataFrame, Functions}

  @spark_remote System.get_env("SPARK_REMOTE", "sc://localhost:15002")

  setup do
    {:ok, session} = SparkEx.connect(url: @spark_remote)
    Process.unlink(session)

    on_exit(fn ->
      if Process.alive?(session), do: SparkEx.Session.stop(session)
    end)

    %{session: session}
  end

  test "sample of registered functions executes successfully", %{session: session} do
    registry = SparkEx.Macros.FunctionRegistry.registry()

    candidates =
      registry
      |> Enum.filter(fn {_name, _spark, arity, _opts} -> arity == 1 end)
      |> Enum.reject(fn {name, _spark, _arity, _opts} -> skip_function?(name) end)
      |> Enum.take(50)

    df = SparkEx.sql(session, "SELECT 1 AS id")

    Enum.each(candidates, fn {name, _spark, _arity, _opts} ->
      column = apply(Functions, name, [Functions.col("id")])
      projected = DataFrame.select(df, [column])

      assert {:ok, [row]} = DataFrame.collect(projected)
      assert map_size(row) == 1
      assert row |> Map.values() |> hd() != nil
    end)
  end

  defp skip_function?(name) do
    name in [
      :try_add,
      :try_subtract,
      :try_multiply,
      :try_divide,
      :try_reflect,
      :try_parse_url
    ]
  end
end
