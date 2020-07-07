package org.hypertrace.gateway.service.explore;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RowComparatorTest {
  private final Row.Builder builder1 = createRowBuilder("dataService", 12.5, 10L, 398, "Great");
  private final Row.Builder builder2 = createRowBuilder("foodService", 0.5, 15L, 76, "Meh");
  private final Row.Builder builder3 = createRowBuilder("adService", 120.5, 67L, 45, "Great");
  private final Row.Builder builder4 = createRowBuilder("zzzAccounting", 1.5, 109L, 897, "Meh");
  private final Row.Builder builder5 = createRowBuilder("auditService", 50.9, 10L, 65, "Great");

  @Test
  public void testSortingSingleOrderByAsc() {
    List<Row.Builder> rowBuilders = getUnsortedRowBuilders();

    List<OrderByExpression> orderByExpressions = new ArrayList<>();
    orderByExpressions.add(createOrderByExpresssion("serviceName", SortOrder.ASC, false));

    RowComparator rowComparator = new RowComparator(orderByExpressions);
    rowBuilders.sort(rowComparator);

    Assertions.assertEquals(builder3, rowBuilders.get(0));
    Assertions.assertEquals(builder5, rowBuilders.get(1));
    Assertions.assertEquals(builder1, rowBuilders.get(2));
    Assertions.assertEquals(builder2, rowBuilders.get(3));
    Assertions.assertEquals(builder4, rowBuilders.get(4));
  }

  @Test
  public void testSortingSingleOrderByDesc() {
    List<Row.Builder> rowBuilders = getUnsortedRowBuilders();

    List<OrderByExpression> orderByExpressions = new ArrayList<>();
    orderByExpressions.add(createOrderByExpresssion("serviceName", SortOrder.DESC, false));

    RowComparator rowComparator = new RowComparator(orderByExpressions);
    rowBuilders.sort(rowComparator);

    Assertions.assertEquals(builder4, rowBuilders.get(0));
    Assertions.assertEquals(builder2, rowBuilders.get(1));
    Assertions.assertEquals(builder1, rowBuilders.get(2));
    Assertions.assertEquals(builder5, rowBuilders.get(3));
    Assertions.assertEquals(builder3, rowBuilders.get(4));
  }

  @Test
  public void testSortingMultipleOrderByAllAsc() {
    List<Row.Builder> rowBuilders = getUnsortedRowBuilders();

    List<OrderByExpression> orderByExpressions = new ArrayList<>();
    orderByExpressions.add(createOrderByExpresssion("v4", SortOrder.ASC, false));
    orderByExpressions.add(createOrderByExpresssion("v1", SortOrder.ASC, true));

    RowComparator rowComparator = new RowComparator(orderByExpressions);
    rowBuilders.sort(rowComparator);

    Assertions.assertEquals(builder1, rowBuilders.get(0));
    Assertions.assertEquals(builder5, rowBuilders.get(1));
    Assertions.assertEquals(builder3, rowBuilders.get(2));
    Assertions.assertEquals(builder2, rowBuilders.get(3));
    Assertions.assertEquals(builder4, rowBuilders.get(4));
  }

  @Test
  public void testSortingMultipleOrderByAllDesc() {
    List<Row.Builder> rowBuilders = getUnsortedRowBuilders();

    List<OrderByExpression> orderByExpressions = new ArrayList<>();
    orderByExpressions.add(createOrderByExpresssion("v4", SortOrder.DESC, false));
    orderByExpressions.add(createOrderByExpresssion("v1", SortOrder.DESC, true));

    RowComparator rowComparator = new RowComparator(orderByExpressions);
    rowBuilders.sort(rowComparator);

    Assertions.assertEquals(builder4, rowBuilders.get(0));
    Assertions.assertEquals(builder2, rowBuilders.get(1));
    Assertions.assertEquals(builder3, rowBuilders.get(2));
    Assertions.assertEquals(builder5, rowBuilders.get(3));
    Assertions.assertEquals(builder1, rowBuilders.get(4));
  }

  @Test
  public void testSortingMultipleOrderByAllMixedAscDesc() {
    List<Row.Builder> rowBuilders = getUnsortedRowBuilders();

    List<OrderByExpression> orderByExpressions = new ArrayList<>();
    orderByExpressions.add(createOrderByExpresssion("v4", SortOrder.DESC, false));
    orderByExpressions.add(createOrderByExpresssion("v2", SortOrder.ASC, true));
    orderByExpressions.add(createOrderByExpresssion("v1", SortOrder.DESC, true));

    RowComparator rowComparator = new RowComparator(orderByExpressions);
    rowBuilders.sort(rowComparator);

    Assertions.assertEquals(builder2, rowBuilders.get(0));
    Assertions.assertEquals(builder4, rowBuilders.get(1));
    Assertions.assertEquals(builder5, rowBuilders.get(2));
    Assertions.assertEquals(builder1, rowBuilders.get(3));
    Assertions.assertEquals(builder3, rowBuilders.get(4));
  }

  private List<Row.Builder> getUnsortedRowBuilders() {
    List<Row.Builder> rowBuilders = new ArrayList<>();

    rowBuilders.add(builder1);
    rowBuilders.add(builder2);
    rowBuilders.add(builder3);
    rowBuilders.add(builder4);
    rowBuilders.add(builder5);

    return rowBuilders;
  }

  private OrderByExpression createOrderByExpresssion(
      String columnName, SortOrder sortOrder, boolean isFunctionType) {
    Expression.Builder expressionBuilder;
    if (isFunctionType) {
      expressionBuilder =
          Expression.newBuilder().setFunction(FunctionExpression.newBuilder().setAlias(columnName));
    } else {
      expressionBuilder =
          Expression.newBuilder()
              .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName));
    }

    return OrderByExpression.newBuilder()
        .setOrder(sortOrder)
        .setExpression(expressionBuilder)
        .build();
  }

  private Row.Builder createRowBuilder(String serviceName, double v1, long v2, int v3, String v4) {
    return Row.newBuilder()
        .putColumns(
            "serviceName",
            Value.newBuilder().setValueType(ValueType.STRING).setString(serviceName).build())
        .putColumns("v1", Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(v1).build())
        .putColumns("v2", Value.newBuilder().setValueType(ValueType.LONG).setLong(v2).build())
        .putColumns("v3", Value.newBuilder().setValueType(ValueType.LONG).setLong(v3).build())
        .putColumns("v4", Value.newBuilder().setValueType(ValueType.STRING).setString(v4).build());
  }
}
