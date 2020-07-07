package org.hypertrace.gateway.service.trace;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.trace.Trace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TraceComparatorTest {
  private static final String PROTOCOL_NAME_KEY = "protocol_name";
  private static final String NAME_KEY = "name";
  private static final String TIME_KEY = "time";
  private static final String HTTP = "HTTP";
  private static final String GRPC = "GRPC";
  private static final String AD_SERVICE_NAME = "adservice";
  private static final String DATA_SERVICE_NAME = "dataservice";
  private static final Trace.Builder GRPC_DATA_SERVICE =
      Trace.newBuilder()
          .putAttributes(PROTOCOL_NAME_KEY, createStringValue(GRPC))
          .putAttributes(NAME_KEY, createStringValue(DATA_SERVICE_NAME))
          .putAttributes(TIME_KEY, createTimestampValue(100));
  private static final Trace.Builder HTTP_AD_SERVICE =
      Trace.newBuilder()
          .putAttributes(PROTOCOL_NAME_KEY, createStringValue(HTTP))
          .putAttributes(NAME_KEY, createStringValue(AD_SERVICE_NAME))
          .putAttributes(TIME_KEY, createTimestampValue(500));
  private static final Trace.Builder HTTP_DATA_SERVICE =
      Trace.newBuilder()
          .putAttributes(PROTOCOL_NAME_KEY, createStringValue(HTTP))
          .putAttributes(NAME_KEY, createStringValue(DATA_SERVICE_NAME))
          .putAttributes(TIME_KEY, createTimestampValue(300));

  private List<Trace.Builder> unsortedTrace;
  private OrderByExpression protocolNameAsc;
  private OrderByExpression nameAsc;
  private OrderByExpression nameDesc;
  private OrderByExpression timeAsc;

  private static Value createStringValue(String value) {
    return Value.newBuilder().setString(value).setValueType(ValueType.STRING).build();
  }

  private static Value createTimestampValue(long value) {
    return Value.newBuilder().setTimestamp(value).setValueType(ValueType.TIMESTAMP).build();
  }

  @BeforeEach
  public void setup() {
    unsortedTrace = new ArrayList<>();
    unsortedTrace.add(HTTP_DATA_SERVICE);
    unsortedTrace.add(GRPC_DATA_SERVICE);
    unsortedTrace.add(HTTP_AD_SERVICE);

    protocolNameAsc =
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(createExpression(PROTOCOL_NAME_KEY))
            .build();

    nameAsc =
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(createExpression(NAME_KEY))
            .build();

    nameDesc =
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.DESC)
            .setExpression(createExpression(NAME_KEY))
            .build();

    timeAsc =
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(createExpression(TIME_KEY))
            .build();
  }

  @Test
  public void test_compare_ascMultiValue_shouldReturnInAsc() {
    {
      List<OrderByExpression> orderByExpressions = List.of(protocolNameAsc, nameAsc);

      List<Trace.Builder> sortedTrace =
          unsortedTrace.stream()
              .sorted(new TraceComparator(orderByExpressions))
              .collect(Collectors.toList());

      assertEquals(GRPC_DATA_SERVICE, sortedTrace.get(0));
      assertEquals(HTTP_AD_SERVICE, sortedTrace.get(1));
      assertEquals(HTTP_DATA_SERVICE, sortedTrace.get(2));
    }

    {
      List<OrderByExpression> orderByExpressions = List.of(protocolNameAsc, timeAsc);

      List<Trace.Builder> sortedTrace =
          unsortedTrace.stream()
              .sorted(new TraceComparator(orderByExpressions))
              .collect(Collectors.toList());

      assertEquals(GRPC_DATA_SERVICE, sortedTrace.get(0));
      assertEquals(HTTP_AD_SERVICE, sortedTrace.get(2));
      assertEquals(HTTP_DATA_SERVICE, sortedTrace.get(1));
    }
  }

  @Test
  public void test_compare_descMultiValue_shouldReturnInDesc() {
    List<OrderByExpression> orderByExpressions = List.of(protocolNameAsc, nameDesc);

    List<Trace.Builder> sortedTrace =
        unsortedTrace.stream()
            .sorted(new TraceComparator(orderByExpressions))
            .collect(Collectors.toList());

    assertEquals(GRPC_DATA_SERVICE, sortedTrace.get(0));
    assertEquals(HTTP_DATA_SERVICE, sortedTrace.get(1));
    assertEquals(HTTP_AD_SERVICE, sortedTrace.get(2));
  }

  @Test
  public void test_compare_noRoot_noRootShouldBeLast() {
    Trace.Builder emptyAttributes = Trace.newBuilder();
    unsortedTrace.clear();
    unsortedTrace.add(HTTP_AD_SERVICE);
    unsortedTrace.add(GRPC_DATA_SERVICE);
    unsortedTrace.add(emptyAttributes);

    List<OrderByExpression> orderByExpressions = Collections.singletonList(protocolNameAsc);

    List<Trace.Builder> sortedTrace =
        unsortedTrace.stream()
            .sorted(new TraceComparator(orderByExpressions))
            .collect(Collectors.toList());

    assertEquals(GRPC_DATA_SERVICE, sortedTrace.get(0));
    assertEquals(HTTP_AD_SERVICE, sortedTrace.get(1));
    assertEquals(emptyAttributes, sortedTrace.get(2));
  }

  private Expression createExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }
}
