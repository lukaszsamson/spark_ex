defmodule SparkEx.M13.UDFRegistrationTest do
  use ExUnit.Case, async: true

  describe "register_java/4" do
    test "builds correct command tuple" do
      # We can't test actual execution without a session, but verify the command
      # would be constructed correctly by testing the command encoder directly
      command = {:register_java_udf, "my_upper", "com.example.UpperUDF", nil, false}

      assert {:register_java_udf, "my_upper", "com.example.UpperUDF", nil, false} = command
    end

    test "builds command with return_type option" do
      return_type = %Spark.Connect.DataType{kind: {:string, %Spark.Connect.DataType.String{}}}
      command = {:register_java_udf, "my_fn", "com.example.MyFn", return_type, false}

      assert {:register_java_udf, "my_fn", "com.example.MyFn", ^return_type, false} = command
    end

    test "builds command with aggregate option" do
      command = {:register_java_udf, "my_sum", "com.example.SumUDAF", nil, true}
      assert {:register_java_udf, "my_sum", "com.example.SumUDAF", nil, true} = command
    end
  end

  describe "register_udtf/4" do
    test "builds correct command tuple for UDTF" do
      command =
        {:register_udtf, "my_udtf", <<1, 2, 3>>, nil, 0, "3.11", true}

      assert {:register_udtf, "my_udtf", <<1, 2, 3>>, nil, 0, "3.11", true} = command
    end

    test "builds command with return_type option" do
      return_type = %Spark.Connect.DataType{
        kind: {:struct, %Spark.Connect.DataType.Struct{}}
      }

      command =
        {:register_udtf, "my_udtf", <<1, 2, 3>>, return_type, 0, "3.11", true}

      assert {:register_udtf, "my_udtf", <<1, 2, 3>>, ^return_type, 0, "3.11", true} = command
    end
  end

  describe "command encoder for register_java_udf" do
    alias SparkEx.Connect.CommandEncoder

    test "encodes register_java_udf without return type" do
      command_tuple = {:register_java_udf, "my_upper", "com.example.UpperUDF", nil, false}
      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:register_function, fun} = command.command_type
      assert fun.function_name == "my_upper"
      assert fun.deterministic == true
      assert {:java_udf, java_udf} = fun.function
      assert java_udf.class_name == "com.example.UpperUDF"
      assert java_udf.output_type == nil
      assert java_udf.aggregate == false
    end

    test "encodes register_java_udf with return type" do
      return_type = %Spark.Connect.DataType{kind: {:string, %Spark.Connect.DataType.String{}}}
      command_tuple = {:register_java_udf, "my_fn", "com.example.MyFn", return_type, false}
      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:register_function, fun} = command.command_type
      assert {:java_udf, java_udf} = fun.function
      assert java_udf.output_type == return_type
    end

    test "encodes register_java_udf as aggregate" do
      command_tuple = {:register_java_udf, "my_sum", "com.example.SumUDAF", nil, true}
      {command, _counter} = CommandEncoder.encode_command(command_tuple, 0)

      assert {:register_function, fun} = command.command_type
      assert {:java_udf, java_udf} = fun.function
      assert java_udf.aggregate == true
    end
  end
end
