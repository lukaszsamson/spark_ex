defmodule SparkEx.Internal.UUID do
  @moduledoc false

  @uuid_v4_regex ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\z/

  @spec generate_v4() :: String.t()
  def generate_v4 do
    <<a::48, _::4, b::12, _::2, c::62>> = :crypto.strong_rand_bytes(16)

    <<a::48, 4::4, b::12, 2::2, c::62>>
    |> encode_uuid()
  end

  defp encode_uuid(<<a::32, b::16, c::16, d::16, e::48>>) do
    hex = &Base.encode16(&1, case: :lower)

    [
      hex.(<<a::32>>),
      "-",
      hex.(<<b::16>>),
      "-",
      hex.(<<c::16>>),
      "-",
      hex.(<<d::16>>),
      "-",
      hex.(<<e::48>>)
    ]
    |> IO.iodata_to_binary()
  end

  @spec valid_v4?(term()) :: boolean()
  def valid_v4?(value) when is_binary(value), do: String.match?(value, @uuid_v4_regex)
  def valid_v4?(_value), do: false
end
