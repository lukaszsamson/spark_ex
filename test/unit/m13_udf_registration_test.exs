defmodule SparkEx.M13.UDFRegistrationTest do
  use ExUnit.Case, async: true

  defmodule FakeUDFSession do
    use GenServer

    def start_link(test_pid) do
      GenServer.start_link(__MODULE__, test_pid, [])
    end

    @impl true
    def init(test_pid), do: {:ok, test_pid}

    @impl true
    def handle_call({:analyze_ddl_parse, ddl}, _from, test_pid) do
      send(test_pid, {:analyze_ddl_parse_called, ddl})

      parsed = %Spark.Connect.DataType{kind: {:double, %Spark.Connect.DataType.Double{}}}
      {:reply, {:ok, parsed}, test_pid}
    end

    @impl true
    def handle_call({:execute_command, command, _opts}, _from, test_pid) do
      send(test_pid, {:execute_command_called, command})
      {:reply, :ok, test_pid}
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

  describe "register_java/4 return_type normalization" do
    test "normalizes return_type DDL string into DataType via analyze_ddl_parse" do
      {:ok, session} = FakeUDFSession.start_link(self())

      assert :ok =
               SparkEx.UDFRegistration.register_java(
                 session,
                 "my_udf",
                 "java.lang.Math",
                 return_type: "DOUBLE"
               )

      assert_receive {:analyze_ddl_parse_called, "DOUBLE"}

      assert_receive {:execute_command_called,
                      {:register_java_udf, "my_udf", "java.lang.Math", %Spark.Connect.DataType{}, false}}
    end

    test "returns error for invalid return_type value" do
      {:ok, session} = FakeUDFSession.start_link(self())

      assert {:error, {:invalid_return_type, 123}} =
               SparkEx.UDFRegistration.register_java(
                 session,
                 "my_udf",
                 "java.lang.Math",
                 return_type: 123
               )
    end
  end

  describe "register_udtf/4 and register_data_source/4 validation" do
    test "register_udtf normalizes return_type DDL string" do
      {:ok, session} = FakeUDFSession.start_link(self())

      assert :ok =
               SparkEx.UDFRegistration.register_udtf(
                 session,
                 "my_udtf",
                 <<1, 2, 3>>,
                 return_type: "id INT"
               )

      assert_receive {:analyze_ddl_parse_called, "id INT"}

      assert_receive {:execute_command_called,
                      {:register_udtf, "my_udtf", <<1, 2, 3>>, %Spark.Connect.DataType{}, 0, "3.11",
                       true}}
    end

    test "register_udtf returns error for invalid deterministic value" do
      {:ok, session} = FakeUDFSession.start_link(self())

      assert {:error, {:invalid_deterministic, "yes"}} =
               SparkEx.UDFRegistration.register_udtf(
                 session,
                 "my_udtf",
                 <<1, 2, 3>>,
                 deterministic: "yes"
               )
    end

    test "register_data_source returns error for non-string python_ver" do
      {:ok, session} = FakeUDFSession.start_link(self())

      assert {:error, {:invalid_python_ver, 311}} =
               SparkEx.UDFRegistration.register_data_source(
                 session,
                 "my_source",
                 <<1, 2, 3>>,
                 python_ver: 311
               )
    end
  end
end
