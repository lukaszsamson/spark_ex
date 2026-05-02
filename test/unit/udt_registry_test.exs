defmodule SparkEx.Unit.UDTRegistryTest do
  # Process-global ETS table — keep async: false so concurrent tests do not
  # collide with each other's registrations.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SparkEx.Connect.UDTRegistry

  setup do
    name = "test.UDT.#{System.unique_integer([:positive])}"
    on_exit(fn -> UDTRegistry.unregister(name) end)
    %{name: name}
  end

  test "register/2 stores a deserializer that lookup_deserializer can find", %{name: name} do
    fun = fn x -> {:wrapped, x} end
    assert :ok = UDTRegistry.register(name, fun)

    udt = %Spark.Connect.DataType.UDT{jvm_class: name}
    assert UDTRegistry.lookup_deserializer(udt) == fun
  end

  test "registering the same {name, fun} twice is silent", %{name: name} do
    fun = fn x -> x end
    assert :ok = UDTRegistry.register(name, fun)

    log = capture_log(fn -> assert :ok = UDTRegistry.register(name, fun) end)
    refute log =~ "replacing existing deserializer"
  end

  test "registering a different fun under the same name warns about clobber", %{name: name} do
    assert :ok = UDTRegistry.register(name, fn x -> x end)

    log =
      capture_log(fn ->
        assert :ok = UDTRegistry.register(name, fn _ -> :replaced end)
      end)

    assert log =~ "replacing existing deserializer"
    assert log =~ inspect(name)
  end

  test "replace?: true silences the clobber warning", %{name: name} do
    assert :ok = UDTRegistry.register(name, fn x -> x end)

    log =
      capture_log(fn ->
        assert :ok = UDTRegistry.register(name, fn _ -> :replaced end, replace?: true)
      end)

    refute log =~ "replacing existing deserializer"
  end

  test "unregister/1 removes the entry", %{name: name} do
    UDTRegistry.register(name, fn x -> x end)
    assert :ok = UDTRegistry.unregister(name)

    udt = %Spark.Connect.DataType.UDT{jvm_class: name}
    assert UDTRegistry.lookup_deserializer(udt) == nil
  end
end
