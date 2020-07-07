package org.hypertrace.gateway.service.common.validators.function;

import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AggregationValidatorTest {
  @Test
  public void aggregationIsValid_shouldNotThrowIllegalArgException() {
    Expression avgExpression =
        Expression.newBuilder()
            .setFunction(
                FunctionExpression.newBuilder()
                    .setFunction(FunctionType.AVG)
                    .setAlias("avg-alias")
                    .addArguments(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder().setColumnName("test-column-name"))))
            .build();

    AggregationValidator aggregationValidator = new AggregationValidator();
    aggregationValidator.validate(avgExpression);
  }

  @Test
  public void aggregationWithNoneFunctionType_shouldThrowIllegalArgException() {
    Expression avgExpression =
        Expression.newBuilder()
            .setFunction(
                FunctionExpression.newBuilder()
                    .setFunction(FunctionType.NONE)
                    .setAlias("avg-alias")
                    .addArguments(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder().setColumnName("test-column-name"))))
            .build();

    AggregationValidator aggregationValidator = new AggregationValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          aggregationValidator.validate(avgExpression);
        });
  }

  @Test
  public void pretendAggregation_shouldThrowIllegalArgException() {
    Expression avgExpression =
        Expression.newBuilder()
            .setLiteral(
                LiteralConstant.newBuilder()
                    .setValue(Value.newBuilder().setLong(20L).setValueType(ValueType.LONG)))
            .build();

    AggregationValidator aggregationValidator = new AggregationValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          aggregationValidator.validate(avgExpression);
        });
  }

  @Test
  public void aggregationWithInvalidFunction_shouldThrowIllegalArgException() {
    Expression avgExpression =
        Expression.newBuilder()
            .setFunction(
                FunctionExpression.newBuilder()
                    .setFunction(FunctionType.AVG)
                    .addArguments(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder().setColumnName("test-column-name"))))
            .build();

    AggregationValidator aggregationValidator = new AggregationValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          aggregationValidator.validate(avgExpression);
        });
  }
}
