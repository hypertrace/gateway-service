package org.hypertrace.gateway.service.common.converter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GatewayValueToQueryValueConverterTest {

  @Test
  public void testLiteralConstantConversion() {
    getLiteralConstantExpressionMap()
        .forEach(
            (ge, qe) ->
                Assertions.assertEquals(
                    qe, QueryAndGatewayDtoConverter.convertToQueryExpression(ge).build()));
  }

  @Test
  public void testFunctionExpressionConversion() {
    getFunctionExpressionMap()
        .forEach(
            (ge, qe) ->
                Assertions.assertEquals(
                    qe, QueryAndGatewayDtoConverter.convertToQueryExpression(ge).build()));
  }

  @Test
  public void testExpressionWithOrderBy() {
    getOrderByExpressionMap()
        .forEach(
            (ge, qe) ->
                Assertions.assertEquals(
                    qe, QueryAndGatewayDtoConverter.convertToQueryExpression(ge).build()));
  }

  @Test
  public void testFilter() {
    getFilterMap()
        .forEach(
            (gf, qf) ->
                Assertions.assertEquals(qf, QueryAndGatewayDtoConverter.convertToQueryFilter(gf)));
  }

  private Map<Expression, org.hypertrace.core.query.service.api.Expression>
      getLiteralConstantExpressionMap() {
    Map<Expression, org.hypertrace.core.query.service.api.Expression> expressionMap =
        new HashMap<>();
    getLiteralMap()
        .forEach(
            (gl, ql) ->
                expressionMap.put(
                    Expression.newBuilder().setLiteral(gl).build(),
                    org.hypertrace.core.query.service.api.Expression.newBuilder()
                        .setLiteral(ql)
                        .build()));
    return expressionMap;
  }

  private Map<Expression, org.hypertrace.core.query.service.api.Expression>
      getFunctionExpressionMap() {
    Map<Expression, org.hypertrace.core.query.service.api.Expression> expressionMap =
        new HashMap<>();
    getFunctionMap()
        .forEach(
            (gf, qf) ->
                expressionMap.put(
                    Expression.newBuilder().setFunction(gf).build(),
                    org.hypertrace.core.query.service.api.Expression.newBuilder()
                        .setFunction(qf)
                        .build()));
    return expressionMap;
  }

  private Map<Expression, org.hypertrace.core.query.service.api.Expression>
      getOrderByExpressionMap() {
    org.hypertrace.core.query.service.api.Expression queryExpression =
        org.hypertrace.core.query.service.api.Expression.newBuilder()
            .setColumnIdentifier(
                org.hypertrace.core.query.service.api.ColumnIdentifier.newBuilder()
                    .setColumnName("API.apiId")
                    .build())
            .build();
    Map<Expression, org.hypertrace.core.query.service.api.Expression> expressionMap =
        new HashMap<>();
    expressionMap.put(
        Expression.newBuilder()
            .setOrderBy(QueryExpressionUtil.getOrderBy("API.apiId", SortOrder.ASC))
            .build(),
        org.hypertrace.core.query.service.api.Expression.newBuilder()
            .setOrderBy(
                org.hypertrace.core.query.service.api.OrderByExpression.newBuilder()
                    .setExpression(queryExpression)
                    .setOrder(org.hypertrace.core.query.service.api.SortOrder.ASC))
            .build());
    expressionMap.put(
        Expression.newBuilder()
            .setOrderBy(QueryExpressionUtil.getOrderBy("API.apiId", SortOrder.DESC))
            .build(),
        org.hypertrace.core.query.service.api.Expression.newBuilder()
            .setOrderBy(
                org.hypertrace.core.query.service.api.OrderByExpression.newBuilder()
                    .setExpression(queryExpression)
                    .setOrder(org.hypertrace.core.query.service.api.SortOrder.DESC))
            .build());
    return expressionMap;
  }

  private Map<Filter, org.hypertrace.core.query.service.api.Filter> getFilterMap() {
    Map<Filter, org.hypertrace.core.query.service.api.Filter> filterMap = new HashMap<>();
    Filter.Builder gatewayFilterBuilder =
        Filter.newBuilder()
            .setLhs(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("API.apiId")))
            .setRhs(Expression.newBuilder().setLiteral(getStringGatewayLiteralConstant("abc")));
    org.hypertrace.core.query.service.api.Filter.Builder queryFilterBuilder =
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setLhs(
                org.hypertrace.core.query.service.api.Expression.newBuilder()
                    .setColumnIdentifier(
                        org.hypertrace.core.query.service.api.ColumnIdentifier.newBuilder()
                            .setColumnName("API.apiId")))
            .setRhs(
                org.hypertrace.core.query.service.api.Expression.newBuilder()
                    .setLiteral(getStringQueryLiteralConstant("abc")));
    getLeafOperatorMap()
        .forEach(
            (go, qo) ->
                filterMap.put(
                    Filter.newBuilder(gatewayFilterBuilder.build()).setOperator(go).build(),
                    org.hypertrace.core.query.service.api.Filter.newBuilder(
                            queryFilterBuilder.build())
                        .setOperator(qo)
                        .build()));

    // Construct boolean filters too
    filterMap.put(
        Filter.newBuilder().setOperator(Operator.AND).addAllChildFilter(filterMap.keySet()).build(),
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(org.hypertrace.core.query.service.api.Operator.AND)
            .addAllChildFilter(filterMap.values())
            .build());

    filterMap.put(
        Filter.newBuilder().setOperator(Operator.OR).addAllChildFilter(filterMap.keySet()).build(),
        org.hypertrace.core.query.service.api.Filter.newBuilder()
            .setOperator(org.hypertrace.core.query.service.api.Operator.OR)
            .addAllChildFilter(filterMap.values())
            .build());

    return filterMap;
  }

  private Map<Operator, org.hypertrace.core.query.service.api.Operator> getLeafOperatorMap() {
    return Map.of(
        Operator.EQ, org.hypertrace.core.query.service.api.Operator.EQ,
        Operator.NEQ, org.hypertrace.core.query.service.api.Operator.NEQ,
        Operator.GT, org.hypertrace.core.query.service.api.Operator.GT,
        Operator.LT, org.hypertrace.core.query.service.api.Operator.LT,
        Operator.LE, org.hypertrace.core.query.service.api.Operator.LE,
        Operator.GE, org.hypertrace.core.query.service.api.Operator.GE,
        Operator.IN, org.hypertrace.core.query.service.api.Operator.IN);
  }

  private Map<FunctionExpression, Function> getFunctionMap() {
    Expression gatewayExprArgument =
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("API.apiId").build())
            .build();
    org.hypertrace.core.query.service.api.Expression queryExprArgument =
        org.hypertrace.core.query.service.api.Expression.newBuilder()
            .setColumnIdentifier(
                org.hypertrace.core.query.service.api.ColumnIdentifier.newBuilder()
                    .setColumnName("API.apiId")
                    .build())
            .build();
    return Map.of(
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.SUM)
            .addArguments(gatewayExprArgument)
            .build(),
        Function.newBuilder().setFunctionName("SUM").addArguments(queryExprArgument).build(),
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVG)
            .addArguments(gatewayExprArgument)
            .build(),
        Function.newBuilder().setFunctionName("AVG").addArguments(queryExprArgument).build(),
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.COUNT)
            .addArguments(gatewayExprArgument)
            .build(),
        Function.newBuilder().setFunctionName("COUNT").addArguments(queryExprArgument).build(),
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.DISTINCTCOUNT)
            .addArguments(gatewayExprArgument)
            .build(),
        Function.newBuilder()
            .setFunctionName("DISTINCTCOUNT")
            .addArguments(queryExprArgument)
            .build(),
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.MAX)
            .addArguments(gatewayExprArgument)
            .build(),
        Function.newBuilder().setFunctionName("MAX").addArguments(queryExprArgument).build(),
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.MIN)
            .addArguments(gatewayExprArgument)
            .build(),
        Function.newBuilder().setFunctionName("MIN").addArguments(queryExprArgument).build(),
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.LATEST)
            .addArguments(gatewayExprArgument)
            .build(),
        Function.newBuilder().setFunctionName("LATEST").addArguments(queryExprArgument).build());
  }

  private Map<LiteralConstant, org.hypertrace.core.query.service.api.LiteralConstant>
      getLiteralMap() {
    return Map.of(
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder().setValueType(ValueType.STRING).setString("abc").build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
                .setString("abc")
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder().setValueType(ValueType.LONG).setLong(1L).build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
                .setLong(1L)
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(1.0).build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE)
                .setDouble(1.0)
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder().setValueType(ValueType.TIMESTAMP).setTimestamp(1000).build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.TIMESTAMP)
                .setTimestamp(1000)
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder().setValueType(ValueType.BOOL).setBoolean(true).build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.BOOL)
                .setBoolean(true)
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder()
                .setValueType(ValueType.STRING_MAP)
                .putStringMap("key", "val")
                .build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_MAP)
                .putStringMap("key", "val")
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder()
                .setValueType(ValueType.STRING_ARRAY)
                .addAllStringArray(Arrays.asList("abc", "xyz"))
                .build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_ARRAY)
                .addAllStringArray(Arrays.asList("abc", "xyz"))
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder()
                .setValueType(ValueType.LONG_ARRAY)
                .addAllLongArray(Arrays.asList(1L, 2L, 3L))
                .build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG_ARRAY)
                .addAllLongArray(Arrays.asList(1L, 2L, 3L))
                .build()),
        buildLiteralConstantFromGatewayValue(
            Value.newBuilder()
                .setValueType(ValueType.DOUBLE_ARRAY)
                .addAllDoubleArray(Arrays.asList(1.0, 2.0))
                .build()),
        buildLiteralConstantFromQueryValue(
            org.hypertrace.core.query.service.api.Value.newBuilder()
                .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE_ARRAY)
                .addAllDoubleArray(Arrays.asList(1.0, 2.0))
                .build()));
  }

  private LiteralConstant getStringGatewayLiteralConstant(String string) {
    return buildLiteralConstantFromGatewayValue(
        Value.newBuilder().setString(string).setValueType(ValueType.STRING).build());
  }

  private org.hypertrace.core.query.service.api.LiteralConstant getStringQueryLiteralConstant(
      String string) {
    return buildLiteralConstantFromQueryValue(
        org.hypertrace.core.query.service.api.Value.newBuilder()
            .setString(string)
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .build());
  }

  private LiteralConstant buildLiteralConstantFromGatewayValue(Value value) {
    return LiteralConstant.newBuilder().setValue(value).build();
  }

  private org.hypertrace.core.query.service.api.LiteralConstant buildLiteralConstantFromQueryValue(
      org.hypertrace.core.query.service.api.Value value) {
    return org.hypertrace.core.query.service.api.LiteralConstant.newBuilder()
        .setValue(value)
        .build();
  }
}
