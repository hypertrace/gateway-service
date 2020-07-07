package org.hypertrace.gateway.service.common.util;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;

/**
 * For manual sorting and limiting that's done when Group By is specified, the OrderByExpressions in
 * the request may have a different aliases from the one in the Selection or Time Aggregation
 * Expression. This is important for function expression OrderBys since the OrderByComparator uses
 * the alias in the OrderBy expression as the column name of value to read when doing the sorting.
 * So we need to make sure that the OrderBy expressions that are functions have aliases that match
 * the ones in the Selections or Time Aggregations. We make a match based on the function type and
 * ColumnIdentifier argument in the OrderBy function expression and Selection/Time Aggregation
 * function expression
 *
 * <p>For ColumnExpression type of OrderBys, we match based on the column name which is expected to
 * be the same in the Selection Column Expression.
 */
public class OrderByUtil {
  public static List<OrderByExpression> matchOrderByExpressionsAliasToSelectionAlias(
      List<OrderByExpression> orderByExpressions,
      List<Expression> selectionList,
      List<TimeAggregation> timeAggregationList) {
    return orderByExpressions.stream()
        .map(
            orderByExpression -> {
              if (!orderByExpression.getExpression().hasFunction()) {
                return orderByExpression;
              } else {
                Expression matchingSelectionExpression =
                    getMatchingFunctionExpressionInSelections(
                        orderByExpression, selectionList, timeAggregationList);
                if (matchingSelectionExpression
                    == null) { // Didn't find a matching expression. Just return the order by
                               // expression
                  return orderByExpression;
                }

                // Create an OrderBy Expression with an alias that matches the one in the matching
                // selection.
                OrderByExpression.Builder orderByBuilder =
                    OrderByExpression.newBuilder(orderByExpression);
                orderByBuilder
                    .getExpressionBuilder()
                    .getFunctionBuilder()
                    .setAlias(matchingSelectionExpression.getFunction().getAlias());

                return orderByBuilder.build();
              }
            })
        .collect(Collectors.toList());
  }

  private static Expression getMatchingFunctionExpressionInSelections(
      OrderByExpression orderByExpression,
      List<Expression> selectionList,
      List<TimeAggregation> timeAggregationList) {
    // Look for matching expression in Selections list.
    Optional<Expression> matchingSelection =
        selectionList.stream()
            .filter(
                expression ->
                    expression.hasFunction()
                        && orderByFunctionMatchesSelectionFunction(
                            orderByExpression.getExpression().getFunction(),
                            expression.getFunction()))
            .findFirst();

    if (matchingSelection.isPresent()) {
      return matchingSelection.get();
    }

    // Not found in Selections list. Look for matching selection in Time Aggregations
    Optional<TimeAggregation> matchingTimeAggregation =
        timeAggregationList.stream()
            .filter(
                timeAggregation ->
                    timeAggregation.getAggregation().hasFunction()
                        && orderByFunctionMatchesSelectionFunction(
                            orderByExpression.getExpression().getFunction(),
                            timeAggregation.getAggregation().getFunction()))
            .findFirst();

    return matchingTimeAggregation.map(TimeAggregation::getAggregation).orElse(null);
  }

  private static boolean orderByFunctionMatchesSelectionFunction(
      FunctionExpression orderByExpression, FunctionExpression selectionExpression) {
    Optional<Expression> orderByColumnIdExpression =
        orderByExpression.getArgumentsList().stream()
            .filter(Expression::hasColumnIdentifier)
            .findFirst();
    if (orderByColumnIdExpression.isEmpty()) {
      return false;
    }

    // Match the ColumnId Expression
    return selectionExpression.getFunction() == orderByExpression.getFunction()
        && selectionExpression.getArgumentsList().stream()
            .filter(Expression::hasColumnIdentifier)
            .findFirst()
            .equals(orderByColumnIdExpression);
  }
}
