package org.hypertrace.gateway.service.common.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.hypertrace.gateway.service.v1.common.AttributeExpression;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.Builder;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.HealthExpression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class QueryExpressionUtil {

  public static Builder buildAttributeExpression(String attributeId) {
    return Expression.newBuilder()
        .setAttributeExpression(AttributeExpression.newBuilder().setAttributeId(attributeId));
  }

  public static Builder buildAttributeExpression(String attributeId, String alias) {
    return Expression.newBuilder()
        .setAttributeExpression(
            AttributeExpression.newBuilder().setAttributeId(attributeId).setAlias(alias));
  }

  public static Expression.Builder getLiteralExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(value).setValueType(ValueType.STRING)));
  }

  public static Expression.Builder getLiteralExpression(List<String> values) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .addAllStringArray(values)
                        .setValueType(ValueType.STRING_ARRAY)));
  }

  public static Expression.Builder getLiteralExpression(Long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setLong(value).setValueType(ValueType.LONG)));
  }

  public static Expression.Builder getAggregateFunctionExpression(
      String columnName, FunctionType function, String alias, boolean includeHealth) {
    return getAggregateFunctionExpression(
        columnName, function, alias, Collections.emptyList(), includeHealth);
  }

  public static Expression.Builder getAggregateFunctionExpression(
      String columnName, FunctionType function, String alias) {
    return getAggregateFunctionExpression(
        columnName, function, alias, Collections.emptyList(), false);
  }

  public static Expression.Builder getAggregateFunctionExpression(
      String attributeId,
      FunctionType function,
      String alias,
      List<Expression> additionalArguments,
      boolean includeHealth) {
    FunctionExpression.Builder functionBuilder =
        FunctionExpression.newBuilder()
            .setFunction(function)
            .setAlias(alias)
            .addArguments(buildAttributeExpression(attributeId));
    if (!additionalArguments.isEmpty()) {
      additionalArguments.forEach(functionBuilder::addArguments);
    }
    if (includeHealth) {
      functionBuilder.addArguments(
          Expression.newBuilder().setHealth(HealthExpression.newBuilder()));
    }

    return Expression.newBuilder().setFunction(functionBuilder.build());
  }

  public static Filter.Builder getBooleanFilter(String attributeId, boolean value) {
    return Filter.newBuilder()
        .setLhs(buildAttributeExpression(attributeId))
        .setOperator(Operator.EQ)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setBoolean(value)
                                .setValueType(ValueType.BOOL)
                                .build())
                        .build())
                .build());
  }

  public static Filter buildStringFilter(String attributeId, Operator operator, String value) {
    return Filter.newBuilder()
        .setLhs(buildAttributeExpression(attributeId))
        .setOperator(operator)
        .setRhs(getLiteralExpression(value))
        .build();
  }

  public static OrderByExpression.Builder getOrderBy(
      String columnName, FunctionType function, String alias, SortOrder order) {
    return OrderByExpression.newBuilder()
        .setOrder(order)
        .setExpression(getAggregateFunctionExpression(columnName, function, alias, false));
  }

  public static OrderByExpression.Builder getOrderBy(String columnName, SortOrder order) {
    return OrderByExpression.newBuilder()
        .setOrder(order)
        .setExpression(buildAttributeExpression(columnName));
  }

  public static long alignToPeriodBoundary(long timeMillis, long periodSecs, boolean alignToNext) {
    long periodMillis = TimeUnit.SECONDS.toMillis(periodSecs);
    long delta = timeMillis % periodMillis;
    return delta == 0
        ? timeMillis
        : alignToNext ? timeMillis + (periodMillis - delta) : timeMillis - delta;
  }
}
