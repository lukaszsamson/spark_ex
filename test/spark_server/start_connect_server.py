"""Start a local Spark Connect server on port 15002.

Usage: cd test/spark_server && uv run python start_connect_server.py
"""

import os
import sys
import subprocess
import signal

def find_spark_home():
    import pyspark
    return os.path.dirname(pyspark.__file__)

def main():
    spark_home = find_spark_home()
    print(f"SPARK_HOME: {spark_home}")

    env = os.environ.copy()
    env["SPARK_HOME"] = spark_home
    # Homebrew OpenJDK - adjust if your install differs
    java_home = env.get("JAVA_HOME") or "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"
    env["JAVA_HOME"] = java_home
    print(f"JAVA_HOME: {java_home}")

    spark_submit = os.path.join(spark_home, "bin", "spark-submit")
    connect_jar = os.path.join(spark_home, "jars", "spark-connect_2.13-4.1.1.jar")

    cmd = [
        "bash", spark_submit,
        "--class", "org.apache.spark.sql.connect.SimpleSparkConnectService",
        "--conf", "spark.connect.grpc.binding.port=15002",
        connect_jar,
    ]

    print(f"Starting Spark Connect server on port 15002...")

    proc = subprocess.Popen(cmd, env=env)

    def shutdown(sig, frame):
        print("\nShutting down Spark Connect server...")
        proc.terminate()
        proc.wait(timeout=10)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    proc.wait()

if __name__ == "__main__":
    main()
