defmodule SparkEx.Error.LimitExceededTest do
  use ExUnit.Case, async: true

  alias SparkEx.Error.LimitExceeded

  describe "message/1" do
    test "formats rows limit message" do
      error = %LimitExceeded{
        limit_type: :rows,
        limit_value: 10_000,
        actual_value: 15_000,
        remediation: "Use DataFrame.limit/2 to reduce result size"
      }

      msg = Exception.message(error)
      assert msg =~ "maximum 10000 rows"
      assert msg =~ "(got 15000)"
      assert msg =~ "Use DataFrame.limit/2"
    end

    test "formats bytes limit message with MB" do
      error = %LimitExceeded{
        limit_type: :bytes,
        limit_value: 67_108_864,
        actual_value: nil,
        remediation: "Pass max_bytes: <value> to increase the limit"
      }

      msg = Exception.message(error)
      assert msg =~ "maximum 64 MB"
      refute msg =~ "(got"
      assert msg =~ "max_bytes"
    end

    test "formats bytes limit message with KB" do
      error = %LimitExceeded{
        limit_type: :bytes,
        limit_value: 1024,
        actual_value: 2048,
        remediation: nil
      }

      msg = Exception.message(error)
      assert msg =~ "maximum 1 KB"
      assert msg =~ "(got 2048)"
    end

    test "formats small bytes limit" do
      error = %LimitExceeded{
        limit_type: :bytes,
        limit_value: 500,
        actual_value: nil,
        remediation: nil
      }

      msg = Exception.message(error)
      assert msg =~ "maximum 500 bytes"
    end

    test "is an exception" do
      assert is_exception(%LimitExceeded{
               limit_type: :rows,
               limit_value: 100,
               actual_value: nil,
               remediation: nil
             })
    end
  end
end
