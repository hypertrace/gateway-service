package org.hypertrace.gateway.service.testutils;

import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class GatewayExpressionCreator {
  public static Filter.Builder createFilter(
      Expression.Builder lhs, Operator op, Expression.Builder rhs) {
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs);
  }

  public static Expression.Builder createLiteralExpression(Boolean value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setBoolean(value).setValueType(ValueType.BOOL)));
  }

  public static Expression.Builder createLiteralExpression(String value) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setString(value).setValueType(ValueType.STRING)));
  }
}
