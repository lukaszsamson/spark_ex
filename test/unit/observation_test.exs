defmodule SparkEx.ObservationTest do
  use ExUnit.Case, async: false

  alias Spark.Connect.{DataType, Expression}
  alias SparkEx.Observation

  setup do
    SparkEx.EtsTableOwner.ensure_table!(:spark_ex_observations, :set)
    :ets.delete_all_objects(:spark_ex_observations)
    :ok
  end

  describe "new/1" do
    test "auto-generates a unique id and a UUID name when no name is provided" do
      a = Observation.new()
      b = Observation.new()
      refute a.id == b.id
      refute a.name == b.name
    end

    test "preserves the provided name and gives each instance a unique id" do
      a = Observation.new("metrics")
      b = Observation.new("metrics")
      assert a.name == "metrics"
      assert b.name == "metrics"
      refute a.id == b.id
    end
  end

  describe "decode_literal/1" do
    test "decodes date as Date" do
      lit = %Expression.Literal{literal_type: {:date, 19_724}}
      assert Observation.decode_literal(lit) == ~D[2024-01-02]
    end

    test "decodes timestamp as DateTime" do
      lit = %Expression.Literal{literal_type: {:timestamp, 1_700_000_000_000_000}}
      decoded = Observation.decode_literal(lit)
      assert %DateTime{} = decoded
      assert decoded.time_zone == "Etc/UTC"
    end

    test "decodes timestamp_ntz as NaiveDateTime" do
      lit = %Expression.Literal{literal_type: {:timestamp_ntz, 1_700_000_000_000_000}}
      assert %NaiveDateTime{} = Observation.decode_literal(lit)
    end

    test "decodes time as Time with microsecond precision" do
      # 12:34:56 in nanoseconds since midnight: 12*3600 + 34*60 + 56 = 45_296 sec
      lit = %Expression.Literal{
        literal_type: {:time, %Expression.Literal.Time{nano: 45_296_000_000_000}}
      }

      assert Observation.decode_literal(lit) == ~T[12:34:56.000000]
    end

    test "decodes decimal as Decimal" do
      lit = %Expression.Literal{
        literal_type: {:decimal, %Expression.Literal.Decimal{value: "123.45"}}
      }

      assert Observation.decode_literal(lit) == Decimal.new("123.45")
    end

    test "decodes calendar_interval as map" do
      lit = %Expression.Literal{
        literal_type:
          {:calendar_interval,
           %Expression.Literal.CalendarInterval{months: 1, days: 2, microseconds: 3}}
      }

      assert Observation.decode_literal(lit) == %{months: 1, days: 2, microseconds: 3}
    end

    test "decodes year_month_interval as map of months" do
      lit = %Expression.Literal{literal_type: {:year_month_interval, 14}}
      assert Observation.decode_literal(lit) == %{months: 14}
    end

    test "decodes day_time_interval as map of microseconds" do
      lit = %Expression.Literal{literal_type: {:day_time_interval, 86_400_000_000}}
      assert Observation.decode_literal(lit) == %{microseconds: 86_400_000_000}
    end

    test "decodes struct elements with field names from struct_type" do
      st = %DataType{
        kind:
          {:struct,
           %DataType.Struct{
             fields: [
               %DataType.StructField{
                 name: "n",
                 data_type: %DataType{kind: {:long, %DataType.Long{}}}
               },
               %DataType.StructField{
                 name: "s",
                 data_type: %DataType{kind: {:string, %DataType.String{}}}
               }
             ]
           }}
      }

      lit = %Expression.Literal{
        literal_type:
          {:struct,
           %Expression.Literal.Struct{
             struct_type: st,
             elements: [
               %Expression.Literal{literal_type: {:long, 5}},
               %Expression.Literal{literal_type: {:string, "x"}}
             ]
           }}
      }

      assert Observation.decode_literal(lit) == %{"n" => 5, "s" => "x"}
    end

    test "decodes specialized_array as a flat list" do
      lit = %Expression.Literal{
        literal_type:
          {:specialized_array,
           %Expression.Literal.SpecializedArray{
             value_type: {:longs, %Spark.Connect.Longs{values: [1, 2, 3]}}
           }}
      }

      assert Observation.decode_literal(lit) == [1, 2, 3]
    end
  end

  describe "per-instance ETS keying" do
    test "rejects a second live observation that shares a name in the same session (FABLE-28)" do
      a = Observation.new("dup")
      b = Observation.new("dup")

      Observation.register_observation(a, [{:alias, :_, "total"}])

      # The single per-(session, name) routing slot cannot disambiguate two
      # live same-named observations at metric-arrival time, so a second
      # attach is refused rather than silently misrouting metrics.
      assert_raise ArgumentError, ~r/AMBIGUOUS_OBSERVATION/, fn ->
        Observation.register_observation(b, [{:alias, :_, "total"}])
      end

      # The first observation still owns the route.
      Observation.store_observed_metrics(%{"dup" => %{"total" => 99}})
      assert Observation.get(a) == %{"total" => 99}
    end

    test "a name may be reused after the prior observation is cleared" do
      a = Observation.new("dup2")
      Observation.register_observation(a, [{:alias, :_, "total"}])
      Observation.clear(a)

      b = Observation.new("dup2")
      assert :ok = Observation.register_observation(b, [{:alias, :_, "total"}])

      Observation.store_observed_metrics(%{"dup2" => %{"total" => 7}})
      assert Observation.get(b) == %{"total" => 7}
    end

    test "last execution wins for a given Observation id (dict.update semantics, FABLE-51)" do
      obs = Observation.new("reuse")
      Observation.register_observation(obs, [{:alias, :_, "total"}])

      Observation.store_observed_metrics(%{"reuse" => %{"total" => 1}})
      Observation.store_observed_metrics(%{"reuse" => %{"total" => 2}})

      assert Observation.get(obs) == %{"total" => 2}
    end

    test "get/1 returns an empty map after attach but before any action (FABLE-51)" do
      obs = Observation.new("pending")
      Observation.register_observation(obs, [{:alias, :_, "total"}])

      assert Observation.get(obs) == %{}
    end

    test "raises when reading an observation that was never attached" do
      obs = Observation.new("ghost")

      assert_raise ArgumentError, ~r/NO_OBSERVE_BEFORE_GET/, fn ->
        Observation.get(obs)
      end
    end
  end
end
