package org.hypertrace.gateway.service.explore;

import java.util.List;
import org.hypertrace.gateway.service.common.comparators.OrderByComparator;
import org.hypertrace.gateway.service.common.comparators.ValueComparator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Row;

public class RowComparator extends OrderByComparator<Row.Builder> {

  public RowComparator(List<OrderByExpression> orderByList) {
    super(orderByList);
  }

  @Override
  protected int compareFunctionExpressionValues(Row.Builder left, Row.Builder right, String alias) {
    return compareRowBuilders(left, right, alias);
  }

  @Override
  protected int compareColumnExpressionValues(
      Row.Builder left, Row.Builder right, String columnName) {
    return compareRowBuilders(left, right, columnName);
  }

  private int compareRowBuilders(Row.Builder left, Row.Builder right, String alias) {
    return ValueComparator.compare(
        left.getColumnsOrDefault(alias, null), right.getColumnsOrDefault(alias, null));
  }
}
