package org.hypertrace.gateway.service.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link EntityKeyEntityBuilderEntryComparator} class. */
public class EntityKeyEntityBuilderEntryComparatorTest {
  private static final int[] VALUES = new int[] {234, 456, 123};
  private static final long[] TIMESTAMPS = new long[] {1000, 5000, 3000};
  private static final String ID_ATTRIBUTE = "id";
  private static final String VARIABLE_ATTRIBUTE_NAME = "attribute.foo";
  private static final String CONSTANT_ATTRIBUTE_NAME = "attribute.constant";
  private static final String METRIC_NAME = "metric.foo";
  private static final String TIMESTAMP_ATTR_NAME = "attribute.startTime";

  private List<Map.Entry<EntityKey, Builder>> getEntities() {
    Map<EntityKey, Builder> builders = new LinkedHashMap<>();

    for (int i = 0; i < VALUES.length; i++) {
      builders.put(
          EntityKey.of("id" + i),
          Entity.newBuilder()
              .putAttribute(
                  ID_ATTRIBUTE,
                  Value.newBuilder().setLong(VALUES[i]).setValueType(ValueType.LONG).build())
              .putAttribute(
                  VARIABLE_ATTRIBUTE_NAME,
                  Value.newBuilder()
                      .setString("bar" + VALUES[i])
                      .setValueType(ValueType.STRING)
                      .build())
              .putAttribute(
                  CONSTANT_ATTRIBUTE_NAME,
                  Value.newBuilder().setString("constant").setValueType(ValueType.STRING).build())
              .putAttribute(
                  TIMESTAMP_ATTR_NAME,
                  Value.newBuilder()
                      .setTimestamp(TIMESTAMPS[i])
                      .setValueType(ValueType.TIMESTAMP)
                      .build())
              .putMetric(
                  METRIC_NAME,
                  AggregatedMetricValue.newBuilder()
                      .setValue(
                          Value.newBuilder()
                              .setDouble((double) 1000 - VALUES[i])
                              .setValueType(ValueType.DOUBLE))
                      .setFunction(FunctionType.SUM)
                      .build()));
    }

    return new ArrayList<>(builders.entrySet());
  }

  @Test
  public void testSortingBySingleOrderBy() {
    // Sort the entities based on descending order of the attribute.
    List<OrderByExpression> orderBys =
        Collections.singletonList(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.DESC)
                .setExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(VARIABLE_ATTRIBUTE_NAME)))
                .build());

    List<Map.Entry<EntityKey, Entity.Builder>> builders =
        getEntities().stream()
            .sorted(new EntityKeyEntityBuilderEntryComparator(orderBys))
            .collect(Collectors.toList());

    // Now the entities should be in descending order of ids.
    assertEquals(
        VALUES[2], builders.get(2).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());
    assertEquals(
        VALUES[0], builders.get(1).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());
    assertEquals(
        VALUES[1], builders.get(0).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());

    // Sort the entities based on descending order of the metric values.
    orderBys =
        Collections.singletonList(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.DESC)
                .setExpression(
                    Expression.newBuilder()
                        .setFunction(
                            FunctionExpression.newBuilder()
                                .setAlias(METRIC_NAME)
                                .setFunction(FunctionType.SUM)
                                .addArguments(
                                    Expression.newBuilder()
                                        .setColumnIdentifier(
                                            ColumnIdentifier.newBuilder()
                                                .setColumnName(METRIC_NAME)
                                                .build())
                                        .build())
                                .build())
                        .build())
                .build());

    builders =
        getEntities().stream()
            .sorted(new EntityKeyEntityBuilderEntryComparator(orderBys))
            .collect(Collectors.toList());

    // Now the entities should be in ascending order of ids.
    assertEquals(
        VALUES[2], builders.get(0).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());
    assertEquals(
        VALUES[0], builders.get(1).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());
    assertEquals(
        VALUES[1], builders.get(2).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());

    // Sort the entities based on ascending order of timestamp attribute
    orderBys =
        Collections.singletonList(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.ASC)
                .setExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(TIMESTAMP_ATTR_NAME)))
                .build());

    builders =
        getEntities().stream()
            .sorted(new EntityKeyEntityBuilderEntryComparator(orderBys))
            .collect(Collectors.toList());

    // Now the entities should be in ascending order of timestamp attr.
    assertEquals(
        TIMESTAMPS[0],
        builders.get(0).getValue().getAttributeMap().get(TIMESTAMP_ATTR_NAME).getTimestamp());
    assertEquals(
        TIMESTAMPS[2],
        builders.get(1).getValue().getAttributeMap().get(TIMESTAMP_ATTR_NAME).getTimestamp());
    assertEquals(
        TIMESTAMPS[1],
        builders.get(2).getValue().getAttributeMap().get(TIMESTAMP_ATTR_NAME).getTimestamp());
  }

  @Test
  public void testSortingByMultipleOrderBys() {
    // Sort the entities based on multiple OrderBys. By keeping the first orderBy on an attribute
    // that's same across all entities, we can effectively test the working of second orderBy.
    List<OrderByExpression> orderBys =
        List.of(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.DESC)
                .setExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(CONSTANT_ATTRIBUTE_NAME)))
                .build(),
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.ASC)
                .setExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(VARIABLE_ATTRIBUTE_NAME)))
                .build());

    List<Map.Entry<EntityKey, Entity.Builder>> builders =
        getEntities().stream()
            .sorted(new EntityKeyEntityBuilderEntryComparator(orderBys))
            .collect(Collectors.toList());

    // Now the entities should be in descending order of ids.
    assertEquals(
        VALUES[2], builders.get(0).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());
    assertEquals(
        VALUES[0], builders.get(1).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());
    assertEquals(
        VALUES[1], builders.get(2).getValue().getAttributeMap().get(ID_ATTRIBUTE).getLong());
  }
}
