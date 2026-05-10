"""Generate PySpark `Spark.Connect.Plan` wire-byte fixtures for parity tests.

This script builds a small, fixed set of DataFrame chains via PySpark Connect
and dumps each chain's `Plan` proto bytes to
``test/fixtures/parity_smoke/<name>.bin`` so that SparkEx can build the same
chain locally and structurally diff against the upstream encoding.

Usage
-----

    cd test/spark_server
    SPARK_REMOTE=sc://localhost:15002 uv run python parity_smoke.py

Plan IDs differ between PySpark and SparkEx (PySpark generates them at
relation-construction time; SparkEx generates them at encode time), so the
Elixir-side comparison strips ``plan_id`` from every ``RelationCommon`` and
``UnresolvedAttribute``/etc. before diffing. The wire bytes themselves are
*not* expected to match; the goal is structural parity of the relations
and expressions emitted.

To add a new chain, append an entry to ``CHAINS`` with a unique key and a
zero-arg lambda returning a DataFrame. Keep chains deterministic (no
``rand``, no current-time literals).
"""
from __future__ import annotations

import os
import pathlib
import sys
from typing import Callable, Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO_ROOT / "test" / "fixtures" / "parity_smoke"


def _chain_sql_simple(spark: SparkSession):
    return spark.sql("SELECT 1 AS x")


def _chain_select_filter(spark: SparkSession):
    return (
        spark.sql("SELECT id, name FROM tbl")
        .filter(F.col("id") > F.lit(10))
        .select(F.col("name"), (F.col("id") + F.lit(1)).alias("id1"))
    )


def _chain_join_on_columns(spark: SparkSession):
    left = spark.sql("SELECT id, dept_id FROM emp")
    right = spark.sql("SELECT dept_id, name FROM dept")
    return left.join(right, left["dept_id"] == right["dept_id"], "inner").select(
        left["id"], right["name"]
    )


CHAINS: Dict[str, Callable[[SparkSession], "DataFrame"]] = {
    "sql_simple": _chain_sql_simple,
    "select_filter": _chain_select_filter,
    "join_on_columns": _chain_join_on_columns,
}


def main() -> int:
    remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
    spark = SparkSession.builder.remote(remote).getOrCreate()

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    for name, build in CHAINS.items():
        df = build(spark)
        plan = df._plan.to_proto(spark.client)
        out = FIXTURE_DIR / f"{name}.bin"
        out.write_bytes(plan.SerializeToString())
        print(f"wrote {out.relative_to(REPO_ROOT)} ({out.stat().st_size} bytes)")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
