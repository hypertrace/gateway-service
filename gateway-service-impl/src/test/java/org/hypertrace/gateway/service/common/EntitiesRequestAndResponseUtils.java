package org.hypertrace.gateway.service.common;

import static org.hypertrace.gateway.service.v1.common.Operator.AND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.EntityResponse;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;

public class EntitiesRequestAndResponseUtils {
  public static Filter generateAndOrNotFilter(Operator operator, Filter... filters) {
    return Filter.newBuilder()
        .setOperator(operator)
        .addAllChildFilter(Arrays.asList(filters))
        .build();
  }

  public static Filter generateFilter(Operator operator, String columnName, Value columnValue) {
    return Filter.newBuilder()
        .setOperator(operator)
        .setLhs(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(columnName).build())
                .build())
        .setRhs(
            Expression.newBuilder()
                .setLiteral(LiteralConstant.newBuilder().setValue(columnValue).build())
                .build())
        .build();
  }

  public static Filter generateEQFilter(String columnName, String columnValue) {
    return generateFilter(
        Operator.EQ,
        columnName,
        Value.newBuilder().setString(columnValue).setValueType(ValueType.STRING).build());
  }

  public static OrderByExpression buildOrderByExpression(String columnName) {
    return OrderByExpression.newBuilder().setExpression(buildExpression(columnName)).build();
  }

  public static OrderByExpression buildOrderByExpression(SortOrder sortOrder, Expression expression) {
    return OrderByExpression.newBuilder().setExpression(expression).setOrder(sortOrder).build();
  }

  public static Expression buildExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }

  public static Expression buildAggregateExpression(
      String columnName,
      FunctionType function,
      String alias,
      List<Expression> additionalArguments) {
    FunctionExpression.Builder functionBuilder =
        FunctionExpression.newBuilder()
            .setFunction(function)
            .setAlias(alias)
            .addArguments(buildExpression(columnName));
    if (!additionalArguments.isEmpty()) {
      additionalArguments.forEach(functionBuilder::addArguments);
    }
    return Expression.newBuilder().setFunction(functionBuilder).build();
  }

  public static TimeAggregation buildTimeAggregation(int period,
                                              String columnName,
                                              FunctionType function,
                                              String alias,
                                              List<Expression> additionalArguments) {
    return TimeAggregation.newBuilder()
        .setPeriod(Period.newBuilder()
            .setValue(period)
            .setUnit(ChronoUnit.SECONDS.name())
        )
        .setAggregation(buildAggregateExpression(columnName, function, alias, additionalArguments))
        .build();
  }

  public static Value getStringValue(String value) {
    return Value.newBuilder().setString(value).setValueType(ValueType.STRING).build();
  }

  public static AggregatedMetricValue getAggregatedMetricValue(FunctionType functionType, double value) {
    return AggregatedMetricValue.newBuilder()
        .setFunction(functionType)
        .setValue(Value.newBuilder().setDouble(value).setValueType(ValueType.DOUBLE))
        .build();
  }

  public static AggregatedMetricValue getAggregatedMetricValue(FunctionType functionType, long value) {
    return AggregatedMetricValue.newBuilder()
        .setFunction(functionType)
        .setValue(Value.newBuilder().setLong(value).setValueType(ValueType.LONG))
        .build();
  }

  public static Expression getLiteralExpression(long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.LONG)
                        .setLong(value)
                )
        )
        .build();
  }

  /**
   * This is because we cannot compare Entity.Builders. We need to build them and compare the built
   * Entity projects.
   * @param expectedEntityFetcherResponse
   * @param actualEntityFetcherResponse
   */
  public static void compareEntityFetcherResponses(EntityFetcherResponse expectedEntityFetcherResponse,
                                                   EntityFetcherResponse actualEntityFetcherResponse) {
    assertEquals(expectedEntityFetcherResponse.size(), actualEntityFetcherResponse.size());
    Map<EntityKey, Builder> expectedEntityFetcherResponseMap = expectedEntityFetcherResponse.getEntityKeyBuilderMap();
    Map<EntityKey, Entity.Builder> actualEntityFetcherResponseMap = actualEntityFetcherResponse.getEntityKeyBuilderMap();
    expectedEntityFetcherResponseMap.forEach((k, v) -> {
      assertTrue(actualEntityFetcherResponseMap.containsKey(k), "Missing key: " + k);
      assertEquals(expectedEntityFetcherResponseMap.get(k).build(), actualEntityFetcherResponseMap.get(k).build());
    });
  }

  public static void compareEntityResponses(EntityResponse expectedEntityResponse, EntityResponse actualEntityResponse) {
    compareEntityFetcherResponses(
        expectedEntityResponse.getEntityFetcherResponse(),
        actualEntityResponse.getEntityFetcherResponse());

    assertEquals(expectedEntityResponse.getEntityKeys(), actualEntityResponse.getEntityKeys());
  }

  public static Filter getTimeRangeFilter(String colName, long startTime, long endTime) {
    return Filter.newBuilder()
        .setOperator(AND)
        .addChildFilter(getTimestampFilter(colName, Operator.GE, startTime))
        .addChildFilter(getTimestampFilter(colName, Operator.LT, endTime))
        .build();
  }

  public static Filter getTimestampFilter(String colName, Operator operator, long timestamp) {
    return Filter.newBuilder()
        .setOperator(operator)
        .setLhs(buildExpression(colName))
        .setRhs(getLiteralExpression(timestamp)).build();
  }
}
