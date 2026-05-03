defmodule SparkEx.BugsPlan4.StreamHTest do
  use ExUnit.Case, async: false

  alias SparkEx.Internal.Random

  @two_pow_63 0x8000000000000000
  @long_min -@two_pow_63
  @long_max @two_pow_63 - 1

  describe "H1 — Random.long_seed/0 covers full signed Long range" do
    test "samples stay within [Long.MIN_VALUE, Long.MAX_VALUE]" do
      for _ <- 1..2_000 do
        seed = Random.long_seed()
        assert is_integer(seed)
        assert seed >= @long_min
        assert seed <= @long_max
      end
    end

    test "produces values in both halves of the signed range" do
      seeds = for _ <- 1..2_000, do: Random.long_seed()

      assert Enum.any?(seeds, &(&1 < 0)),
             "expected some negative seeds across 2000 samples"

      assert Enum.any?(seeds, &(&1 > 0)),
             "expected some positive seeds across 2000 samples"
    end
  end

  describe "H2 — Session.safe_disconnect/1 emits telemetry on adapter failure" do
    test "telemetry exception event fires when disconnect crashes" do
      ref = make_ref()
      handler_id = {__MODULE__, ref}
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:spark_ex, :session, :disconnect, :exception],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:telemetry, ref, measurements, metadata})
          end,
          nil
        )

      try do
        # An invalid channel triggers the catch/rescue path in safe_disconnect.
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = SparkEx.Session.safe_disconnect(:invalid_channel)
        end)

        assert_receive {:telemetry, ^ref, _measurements, metadata}, 500
        assert metadata.kind in [:error, :exit, :throw]
        assert Map.has_key?(metadata, :reason)
        assert is_list(metadata.stacktrace)
      after
        :telemetry.detach(handler_id)
      end
    end
  end
end
