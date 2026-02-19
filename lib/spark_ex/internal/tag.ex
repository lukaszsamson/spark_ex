defmodule SparkEx.Internal.Tag do
  @moduledoc false

  @spec validate!(String.t()) :: :ok
  def validate!(""), do: raise(ArgumentError, "Spark Connect tag must be a non-empty string")

  def validate!(tag) when is_binary(tag) do
    if String.contains?(tag, ",") do
      raise ArgumentError, "Spark Connect tag cannot contain ','"
    end

    :ok
  end
end
