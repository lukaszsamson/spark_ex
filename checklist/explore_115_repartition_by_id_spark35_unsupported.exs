# Repro: repartition_by_id emits unsupported expression error on Spark 3.5 cluster
import SparkEx.Functions
alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

df = SparkEx.sql(s, "SELECT * FROM VALUES (1,'a'), (2,'b'), (3,'c') AS t(pid, val)")

r1 = DataFrame.repartition_by_id(df, 2, col("pid"))
r2 = DataFrame.repartition_by_id(df, 2, "pid")

IO.inspect(DataFrame.count(r1), label: "count_r1")
IO.inspect(DataFrame.rdd_num_partitions_approx(r1), label: "parts_r1_approx")
IO.inspect(DataFrame.count(r2), label: "count_r2")
IO.inspect(DataFrame.rdd_num_partitions_approx(r2), label: "parts_r2_approx")
IO.puts("session_alive?: #{Process.alive?(s)}")
