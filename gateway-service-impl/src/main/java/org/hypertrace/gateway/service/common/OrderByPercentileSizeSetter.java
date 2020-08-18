package org.hypertrace.gateway.service.common;

import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;

/**
 * Set the size for percentiles in order by if it is not set. This is to give UI the time to fix
 * the bug which does not set the size when they have order by in the request.
 * Will delete this once the bug has been fixed in the UI.
 */
public class OrderByPercentileSizeSetter {
  private static final long DEFAULT_PERCENTILE_SIZE = 99;
  public static EntitiesRequest setPercentileSize(EntitiesRequest originalRequest) {
    if (originalRequest.getOrderByCount() == 0) {
      return originalRequest;
    }

    boolean hasOrderByPercentileMissingArgument = originalRequest.getOrderByList().stream().anyMatch(
        (orderByExpression) -> {
          Expression expression = orderByExpression.getExpression();
          if (expression.hasFunction() && expression.getFunction().getFunction() == FunctionType.PERCENTILE) {
            return expression.getFunction().getArgumentsList().stream().filter(Expression::hasLiteral).findFirst().isEmpty();
          }
          return false;
        }
    );

    if (!hasOrderByPercentileMissingArgument) {
      return originalRequest;
    }

    return EntitiesRequest.newBuilder(originalRequest)
        .clearOrderBy()
        .addAllOrderBy(addArgumentsToOrderByPercentile(originalRequest))
        .build();
  }

  private static List<OrderByExpression> addArgumentsToOrderByPercentile(EntitiesRequest originalRequest) {
    Expression percentileArgument = originalRequest.getSelectionList().stream()
        .filter(selection -> selection.hasFunction() && selection.getFunction().getFunction() == FunctionType.PERCENTILE)
        .flatMap(selection -> selection.getFunction().getArgumentsList().stream())
        .filter(Expression::hasLiteral)
        .findFirst()
        .orElse(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(Value.newBuilder()
                            .setLong(DEFAULT_PERCENTILE_SIZE)
                            .setValueType(ValueType.LONG)
                        )
                )
                .build()
        );
    return originalRequest.getOrderByList().stream()
        .map(orderByExpression -> {
          Expression expression = orderByExpression.getExpression();
          if (expression.hasFunction() && expression.getFunction().getFunction() == FunctionType.PERCENTILE &&
              expression.getFunction().getArgumentsList().stream().filter(Expression::hasLiteral).findFirst().isEmpty()) {
            return OrderByExpression.newBuilder(orderByExpression)
                .setExpression(
                    Expression.newBuilder(expression).
                        setFunction(
                            FunctionExpression.newBuilder(expression.getFunction())
                                .addArguments(percentileArgument)
                        )
                )
                .build();
          } else {
            return orderByExpression;
          }
        })
        .collect(Collectors.toList());
  }
}
