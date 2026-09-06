#!/usr/bin/env bash
set -euo pipefail

: "${SPARK_HOME:?set SPARK_HOME to a Spark 4.2.0 distribution}"
: "${JAVA_HOME:?set JAVA_HOME to a Java 17+ JDK}"

root_dir=$(cd "$(dirname "$0")/.." && pwd)
out_dir=${1:-/tmp/spark-ex-spark42-providers}
mkdir -p "$out_dir/classes"

catalyst_jar="$out_dir/spark-catalyst_2.13-4.2.0-tests.jar"
sql_jar="$out_dir/spark-sql_2.13-4.2.0-tests.jar"
seeder_jar="$out_dir/spark42-cdc-seeder.jar"

curl -fL "https://repo1.maven.org/maven2/org/apache/spark/spark-catalyst_2.13/4.2.0/spark-catalyst_2.13-4.2.0-tests.jar" -o "$catalyst_jar"
curl -fL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.13/4.2.0/spark-sql_2.13-4.2.0-tests.jar" -o "$sql_jar"

verify_sha256() {
  local expected=$1
  local file=$2
  local actual
  if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$file" | awk '{print $1}')
  else
    actual=$(shasum -a 256 "$file" | awk '{print $1}')
  fi
  test "$actual" = "$expected"
}

verify_sha256 9f46cfb6d4cb5dff696b76f8af3db1ecfc56cbc43ffb0fa785ef947ac4ad2620 "$catalyst_jar"
verify_sha256 810bef6fd568e1626aad7354a1161e9e5ac629fbc55ba0a9a4e3cec8c06376eb "$sql_jar"

"$JAVA_HOME/bin/javac" -proc:none \
  -cp "$SPARK_HOME/jars/*:$catalyst_jar" \
  -d "$out_dir/classes" \
  "$root_dir/test/support/spark42/Spark42SeededChangelogCatalog.java"
"$JAVA_HOME/bin/jar" cf "$seeder_jar" -C "$out_dir/classes" .

printf '%s\n' "$catalyst_jar,$sql_jar,$seeder_jar"
