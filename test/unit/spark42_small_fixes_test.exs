defmodule SparkEx.Unit.Spark42SmallFixesTest do
  use ExUnit.Case, async: true
  alias SparkEx.{Artifacts, DataFrame, Writer}
  alias SparkEx.Connect.PlanEncoder

  test "lambda references and declarations reject empty names" do
    for expression <- [{:lambda_var, ""}, {:lambda, {:lit, 1}, [{:lambda_var, ""}]}] do
      assert_raise ArgumentError, ~r/lambda variable name must be a nonempty string/, fn ->
        PlanEncoder.encode_expression(expression)
      end
    end

    assert %{expr_type: {:unresolved_named_lambda_variable, %{name_parts: ["x"]}}} =
             PlanEncoder.encode_expression({:lambda_var, "x"})
  end

  test "bucket errors identify the offending number or column argument" do
    writer = DataFrame.new(self(), {:range, 0, 1, 1, nil}) |> DataFrame.write()

    for count <- [0, -1, 1.5, "2"] do
      assert_raise ArgumentError, ~r/positive number of buckets/, fn ->
        Writer.bucket_by(writer, count, ["id"])
      end
    end

    assert_raise ArgumentError, ~r/list of column names/, fn ->
      Writer.bucket_by(writer, 2, "id")
    end
  end

  test "artifact paths retain literal spaces and percent escapes rather than URI decoding" do
    path =
      Path.join(
        System.tmp_dir!(),
        "spark-ex-artifact-#{System.unique_integer([:positive])} %20.txt"
      )

    File.write!(path, "contents")
    on_exit(fn -> File.rm(path) end)
    assert {:ok, [{name, {:file, ^path, 8}}]} = Artifacts.prepare(path, "files")
    assert name == "files/" <> Path.basename(path)

    uri = "file://" <> URI.encode(path)
    assert {:error, {:file_stat_error, ^uri, _}} = Artifacts.prepare(uri, "files")
  end
end
