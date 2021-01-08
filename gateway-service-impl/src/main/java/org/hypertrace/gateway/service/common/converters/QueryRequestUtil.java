package org.hypertrace.gateway.service.common.converters;

import java.util.List;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

/**
 * Utility methods to easily create {@link org.hypertrace.core.query.service.api.QueryRequest} its
 * selections and filters.
 */
public class QueryRequestUtil {

  public static final String DATE_TIME_CONVERTER = "dateTimeConvert";

  public static Filter createBetweenTimesFilter(String columnName, long lower, long higher) {
    return Filter.newBuilder()
        .setOperator(Operator.AND)
        .addChildFilter(createLongFilter(columnName, Operator.GE, lower))
        .addChildFilter(createLongFilter(columnName, Operator.LT, higher))
        .build();
  }

  public static Expression createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName))
        .build();
  }

  public static Expression createColumnExpression(String columnName, String alias) {
    return Expression.newBuilder()
        .setColumnIdentifier(
            ColumnIdentifier.newBuilder().setColumnName(columnName).setAlias(alias))
        .build();
  }

  public static Filter createStringFilter(String columnName, Operator op, String value) {
    return createFilter(columnName, op, createStringLiteralExpression(value));
  }

  public static Filter createLongFilter(String columnName, Operator op, long value) {
    return createFilter(columnName, op, createLongLiteralExpression(value));
  }

  public static Filter createFilter(String columnName, Operator op, Expression value) {
    return createFilter(createColumnExpression(columnName), op, value);
  }

  public static Filter createFilter(Expression columnExpression, Operator op, Expression value) {
    return Filter.newBuilder()
                 .setLhs(columnExpression)
                 .setOperator(op)
                 .setRhs(value)
                 .build();
  }

  public static Filter createCompositeFilter(Operator operator, List<Filter> childFilters) {
    return Filter.newBuilder().setOperator(operator).addAllChildFilter(childFilters).build();
  }

  public static Expression createStringLiteralExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString(value)))
        .build();
  }

  public static Expression createLongLiteralExpression(long value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(value)))
        .build();
  }

  public static Expression createStringNullLiteralExpression() {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.NULL_STRING)))
        .build();
  }

  public static Expression createStringArrayLiteralExpression(List<String> strings) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setValueType(ValueType.STRING_ARRAY)
                        .addAllStringArray(strings)))
        .build();
  }

  public static Expression createCountByColumnSelection(String columnName) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName("COUNT")
                .addArguments(createColumnExpression(columnName)))
        .build();
  }

  public static Expression createTimeColumnGroupByExpression(String timeColumn, long periodSecs) {
    return Expression.newBuilder()
        .setFunction(
            Function.newBuilder()
                .setFunctionName(DATE_TIME_CONVERTER)
                .addArguments(createColumnExpression(timeColumn))
                .addArguments(createStringLiteralExpression("1:MILLISECONDS:EPOCH"))
                .addArguments(createStringLiteralExpression("1:MILLISECONDS:EPOCH"))
                .addArguments(createStringLiteralExpression(periodSecs + ":SECONDS")))
        .build();
  }
}
