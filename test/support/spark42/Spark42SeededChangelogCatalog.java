package org.apache.spark.sql.connector.catalog;

import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

/** Spark 4.2 test catalog with deterministic insert/update/delete CDC history. */
public final class Spark42SeededChangelogCatalog extends InMemoryChangelogCatalog {
  @Override
  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties) {
    Table table = super.createTable(ident, columns, partitions, properties);
    seed(ident);
    return table;
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException,
          org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException {
    Table table = super.createTable(ident, schema, partitions, properties);
    seed(ident);
    return table;
  }

  @Override
  public Table createTable(Identifier ident, TableInfo info) {
    Table table = super.createTable(ident, info);
    seed(ident);
    return table;
  }

  private void seed(Identifier ident) {
    if (!"p1_cdc".equals(ident.name())) {
      return;
    }
    InternalRow insert = row(1L, "old", "insert", 1L, 1_000_000L);
    InternalRow updateBefore = row(1L, "old", "update_before", 2L, 2_000_000L);
    InternalRow updateAfter = row(1L, "new", "update_after", 2L, 2_000_000L);
    InternalRow delete = row(1L, "new", "delete", 3L, 3_000_000L);
    Seq<InternalRow> rows =
        CollectionConverters.asScala(
                Arrays.asList(insert, updateBefore, updateAfter, delete))
            .toSeq();
    clearChangeRows(ident);
    addChangeRows(ident, rows);
  }

  private static InternalRow row(
      long id, String data, String type, long version, long timestampMicros) {
    return new GenericInternalRow(
        new Object[] {
          id,
          UTF8String.fromString(data),
          UTF8String.fromString(type),
          version,
          timestampMicros
        });
  }
}
