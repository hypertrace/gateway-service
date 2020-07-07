package org.hypertrace.gateway.service.common.comparators;

import java.util.Comparator;
import java.util.List;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;

/**
 * This will be used to compare two values, denoted by the generic type T, based on the OrderByExpressions list. It
 * switches on the OrderByExpression type, whether it's a Function or a Column. The extensions of this class implement:
 * 1. compareFunctionExpressionValues(T o1, T o2, String alias) which compares two objects given an alias of the function.
 *    It is expected that T has a way to determine the order given the alias and that this function will execute on a
 *    value that is a result of some aggregation.
 * 2. compareColummnExpressionValues(T o1, T o2, String columnName) which compares two objects based on simple column
 *    names. This is expected to be applied to a simple column read.
 *
 * <p>This class is used to fix Pinot's limitation of not being able to do Order By when Group By is
 * specified in the query. It will be useful for Entities and Explore Requests.
 *
 * <p>See EntityComparator for an example.
 *
 * @param <T>
 */
public abstract class OrderByComparator<T> implements Comparator<T> {
  private final List<OrderByExpression> orderByList;

  public OrderByComparator(List<OrderByExpression> orderByList) {
    this.orderByList = orderByList;
  }

  @Override
  public int compare(T o1, T o2) {
    for (OrderByExpression orderBy : orderByList) {

      // Check if we need to order by an aggregated metric.
      switch (orderBy.getExpression().getValueCase()) {
        case FUNCTION:
          org.hypertrace.gateway.service.v1.common.FunctionExpression function =
              orderBy.getExpression().getFunction();
          String alias = MetricAggregationFunctionUtil.getAggregationFunctionAlias(function);
          int value = compareFunctionExpressionValues(o1, o2, alias);
          if (value == 0) {
            continue;
          }
          return orderBy.getOrder() == SortOrder.ASC ? value : Math.negateExact(value);

        case COLUMNIDENTIFIER:
          // Otherwise, this is either a simple attribute/metric or entity name itself.
          String attr = orderBy.getExpression().getColumnIdentifier().getColumnName();
          value = compareColumnExpressionValues(o1, o2, attr);
          if (value == 0) {
            continue;
          }
          return orderBy.getOrder() == SortOrder.ASC ? value : Math.negateExact(value);

        default:
          throw new IllegalArgumentException(
              "Invalid orderBy: " + orderBy.getExpression().getValueCase());
      }
    }

    // If all fields are matching, both the entities are same.
    return 0;
  }

  protected abstract int compareFunctionExpressionValues(T o1, T o2, String alias);

  protected abstract int compareColumnExpressionValues(T o1, T o2, String columnName);
}
