package org.apache.spark.sql.connect.service;

import org.sparkproject.connect.protobuf.Any;
import org.apache.spark.connect.proto.ExecutePlanRequest;
import org.apache.spark.sql.connect.planner.SparkConnectPlanner;
import org.apache.spark.sql.connect.plugin.CommandPlugin;

/** Test-only barrier: register a real execution without starting its runner. */
public final class Spark42PendingOperationPlugin implements CommandPlugin {
  private static final String TYPE = "spark-ex.test/PendingOperation";

  @Override
  public boolean process(byte[] command, SparkConnectPlanner planner) {
    final Any payload;
    try {
      payload = Any.parseFrom(command);
    } catch (org.sparkproject.connect.protobuf.InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
    if (!TYPE.equals(payload.getTypeUrl())) return false;
    String[] parts = payload.getValue().toStringUtf8().split("\\|", -1);
    if (parts.length != 3) throw new IllegalArgumentException("action|operation_id|tag required");
    SparkConnectExecutionManager manager = SparkConnectService$.MODULE$.executionManager();
    SessionHolder session = planner.sessionHolder();
    ExecuteKey key = new ExecuteKey(session.userId(), session.sessionId(), parts[1]);
    if ("create".equals(parts[0])) {
      ExecutePlanRequest request = planner.executeHolderOpt().get().request().toBuilder()
          .setOperationId(parts[1]).clearTags().addTags(parts[2]).build();
      ExecuteHolder holder = manager.createExecuteHolder(key, request, session);
      if (!"Pending".equals(holder.eventsManager().status().toString())
          || holder.isExecuteThreadRunnerAlive()) {
        throw new IllegalStateException("fixture execution must remain Pending without a runner");
      }
    } else if ("assert_removed".equals(parts[0])) {
      if (manager.getExecuteHolder(key).isDefined()) {
        throw new IllegalStateException("released execution still registered");
      }
    } else {
      throw new IllegalArgumentException("unknown fixture action");
    }
    return true;
  }
}
