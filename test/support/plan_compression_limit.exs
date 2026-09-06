# Run with a dedicated Spark Connect server configured with
# --conf spark.connect.maxPlanSize=1024:
# SPARK_REMOTE=sc://localhost:15004 MIX_ENV=test mix run test/support/plan_compression_limit.exs
{:ok, session} =
  SparkEx.connect(url: System.fetch_env!("SPARK_REMOTE"), plan_compression: true)

try do
  :ok =
    SparkEx.Session.config_set(session, [
      {"spark.connect.session.planCompression.threshold", "0"}
    ])

  query = "SELECT '" <> String.duplicate("bounded compression check", 1000) <> "' AS value"

  {:error,
   %SparkEx.Error.Remote{
     error_class: "CONNECT_INVALID_PLAN.PLAN_SIZE_LARGER_THAN_MAX",
     message_parameters: %{"maxPlanSize" => "1024"}
   }} = SparkEx.Session.execute_collect(session, {:sql, query, nil})

  IO.puts("Valid compressed plan rejected by the server's 1024-byte maxPlanSize limit")
after
  SparkEx.Session.stop(session)
end
