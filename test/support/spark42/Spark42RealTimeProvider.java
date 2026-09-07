package org.apache.spark.sql.connector.read;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.streaming.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Test-only finite source exercising the actual Spark 4.2 real-time read contract. */
public final class Spark42RealTimeProvider implements TableProvider {
  private static final StructType SCHEMA = new StructType()
      .add("id", DataTypes.LongType, false);

  @Override public StructType inferSchema(CaseInsensitiveStringMap options) { return SCHEMA; }
  @Override public Table getTable(StructType schema, Transform[] partitions,
      Map<String, String> properties) { return new SourceTable(); }

  private static final class SourceTable implements SupportsRead {
    @Override public String name() { return "Spark42RealTimeFixture"; }
    @Override public StructType schema() { return SCHEMA; }
    @Override public Set<TableCapability> capabilities() {
      return EnumSet.of(TableCapability.MICRO_BATCH_READ);
    }
    @Override public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return () -> new Scan() {
        @Override public StructType readSchema() { return SCHEMA; }
        @Override public MicroBatchStream toMicroBatchStream(String checkpoint) {
          return new SourceStream();
        }
      };
    }
  }

  private static final class Position extends Offset implements PartitionOffset {
    final long value;
    Position(long value) { this.value = value; }
    @Override public String json() { return Long.toString(value); }
  }

  private static final class Partition implements InputPartition {
    final long start;
    Partition(long start) { this.start = start; }
  }

  private static final class SourceStream implements MicroBatchStream, SupportsRealTimeMode {
    @Override public Offset initialOffset() { return new Position(0); }
    @Override public Offset latestOffset() { return new Position(3); }
    @Override public Offset deserializeOffset(String json) {
      return new Position(Long.parseLong(json));
    }
    @Override public InputPartition[] planInputPartitions(Offset start, Offset end) {
      throw new IllegalStateException("fixture must execute using the real-time scan path");
    }
    @Override public InputPartition[] planInputPartitions(Offset start) {
      return new InputPartition[] {new Partition(((Position) start).value)};
    }
    @Override public Offset mergeOffsets(PartitionOffset[] offsets) {
      if (offsets.length != 1) throw new IllegalStateException("expected one partition");
      return (Position) offsets[0];
    }
    @Override public PartitionReaderFactory createReaderFactory() {
      return new ReaderFactory();
    }
    @Override public void commit(Offset offset) {}
    @Override public void stop() {}
  }

  private static final class ReaderFactory implements PartitionReaderFactory {
    @Override public PartitionReader<InternalRow> createReader(InputPartition partition) {
      return new Reader(((Partition) partition).start);
    }
  }

  private static final class Reader implements SupportsRealTimeRead<InternalRow> {
    private long next;
    private boolean closed;
    Reader(long start) { next = start; }
    @Override public boolean next() {
      throw new IllegalStateException("fixture requires nextWithTimeout");
    }
    @Override public RecordStatus nextWithTimeout(Long startTimeMs, Long timeoutMs)
        throws IOException {
      if (next < 3) {
        next++;
        return RecordStatus.newStatusWithoutArrivalTime(true);
      }
      long remaining;
      while (!closed && (remaining = startTimeMs + timeoutMs - System.currentTimeMillis()) > 0) {
        try { Thread.sleep(Math.min(remaining, 100)); }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("real-time fixture interrupted", e);
        }
      }
      return RecordStatus.newStatusWithoutArrivalTime(false);
    }
    @Override public InternalRow get() { return new GenericInternalRow(new Object[] {next}); }
    @Override public PartitionOffset getOffset() { return new Position(next); }
    @Override public void close() { closed = true; }
  }
}
