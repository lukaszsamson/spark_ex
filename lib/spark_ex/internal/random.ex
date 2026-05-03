defmodule SparkEx.Internal.Random do
  @moduledoc false

  @two_pow_63 0x8000000000000000
  @two_pow_64 0x10000000000000000

  @doc """
  Returns a uniformly distributed integer in the full signed 64-bit range
  `[-2^63, 2^63 - 1]`, matching the seed range Spark uses for
  `rand`/`randn`/`shuffle`/`sample`/etc.
  """
  @spec long_seed() :: integer()
  def long_seed do
    :rand.uniform(@two_pow_64) - 1 - @two_pow_63
  end
end
