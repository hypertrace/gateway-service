package org.hypertrace.gateway.service.common.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.EntityKeyEntityBuilderEntryComparator;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for DataCollectionUtil */
public class DataCollectionUtilTest {

  @Test
  public void testLimitAndSort() {
    List<OrderByExpression> orderByExpressionList =
        Collections.singletonList(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.ASC)
                .setExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName("API.name")))
                .build());
    Entity.Builder abcBuilder =
        Entity.newBuilder()
            .putAttribute(
                "API.name",
                Value.newBuilder().setValueType(ValueType.STRING).setString("ABC").build());
    Entity.Builder pqrBuilder =
        Entity.newBuilder()
            .putAttribute(
                "API.name",
                Value.newBuilder().setValueType(ValueType.STRING).setString("PQR").build());
    Entity.Builder xyzBuilder =
        Entity.newBuilder()
            .putAttribute(
                "API.name",
                Value.newBuilder().setValueType(ValueType.STRING).setString("XYZ").build());
    Map<EntityKey, Entity.Builder> entityKeyBuilderMap =
        Map.of(
            EntityKey.of("id1"), pqrBuilder,
            EntityKey.of("id2"), abcBuilder,
            EntityKey.of("id3"), xyzBuilder);
    List<Map.Entry<EntityKey, Entity.Builder>> sortedBuilders =
        DataCollectionUtil.limitAndSort(
            entityKeyBuilderMap.entrySet().stream(),
            2,
            1,
            orderByExpressionList.size(),
            new EntityKeyEntityBuilderEntryComparator(orderByExpressionList));
    Assertions.assertEquals(2, sortedBuilders.size());
    Assertions.assertEquals(pqrBuilder, sortedBuilders.get(0).getValue());
    Assertions.assertEquals(xyzBuilder, sortedBuilders.get(1).getValue());
  }
}
