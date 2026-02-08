defmodule SparkExTest do
  use ExUnit.Case
  doctest SparkEx

  test "greets the world" do
    assert SparkEx.hello() == :world
  end
end
