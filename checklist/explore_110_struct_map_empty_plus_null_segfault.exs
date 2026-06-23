# Repro: native segfault in collect/1 for struct<map> when empty-map row and null-struct row coexist.
# This script is expected to crash the BEAM process (exit 139) in affected builds.
alias SparkEx.DataFrame

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

df =
  SparkEx.sql(
    s,
    """
    SELECT * FROM VALUES
      (1, named_struct('a', 10, 'b', array(1,2), 'm', map('x',1,'y',2))),
      (2, named_struct('a', NULL, 'b', array(), 'm', map())),
      (3, CAST(NULL AS STRUCT<a:INT,b:ARRAY<INT>,m:MAP<STRING,INT>>))
    AS t(id, payload)
    """
  )

IO.puts("about_to_collect")
IO.inspect(DataFrame.collect(df), label: "collect_result")
IO.puts("session_alive?: #{Process.alive?(s)}")
