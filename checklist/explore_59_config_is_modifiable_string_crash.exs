# Repro: EX-107 — config_is_modifiable with bare string crashes session
# The function expects keys to be a list of strings but doesn't validate.
# Passing a single string sends it to protobuf `keys` field which expects Enumerable,
# causing ProtobufEncodeError that kills the GenServer session process.

{:ok, s} = SparkEx.connect(url: System.get_env("SPARK_REMOTE", "sc://localhost:15002"))

# This crashes the session:
SparkEx.Session.config_is_modifiable(s, "spark.sql.shuffle.partitions")
# => ** (Protobuf.EncodeError) protocol Enumerable not implemented for BitString

# Correct usage would be:
# SparkEx.Session.config_is_modifiable(s, ["spark.sql.shuffle.partitions"])
