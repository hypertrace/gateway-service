package org.hypertrace.gateway.service.common.validators.function;

import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.HealthExpression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OneArgAggregationValidatorTest {
  @Test
  public void validAvgFunction_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVG)
            .setAlias("avg-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .build();
    OneArgAggregationValidator avgValidator = new OneArgAggregationValidator();
    avgValidator.validate(functionExpression);
  }

  @Test
  public void validAvgFunctionWithHealth_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVG)
            .setAlias("avg-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(Expression.newBuilder().setHealth(HealthExpression.newBuilder()))
            .build();
    OneArgAggregationValidator avgValidator = new OneArgAggregationValidator();
    avgValidator.validate(functionExpression);
  }

  @Test
  public void validCountFunction_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.COUNT)
            .setAlias("count-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .build();
    OneArgAggregationValidator countValidator = new OneArgAggregationValidator();
    countValidator.validate(functionExpression);
  }

  @Test
  public void validDistinctCountFunction_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.DISTINCTCOUNT)
            .setAlias("distinctcount-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .build();
    OneArgAggregationValidator countValidator = new OneArgAggregationValidator();
    countValidator.validate(functionExpression);
  }

  @Test
  public void avgFunctionWithEmptyAlias_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVG)
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .build();
    OneArgAggregationValidator avgValidator = new OneArgAggregationValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgFunctionWithEmptyColumnName_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVG)
            .setAlias("avg-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("")))
            .build();
    OneArgAggregationValidator avgValidator = new OneArgAggregationValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgFunctionWithNoColumn_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder().setFunction(FunctionType.AVG).setAlias("avg-alias").build();
    OneArgAggregationValidator avgValidator = new OneArgAggregationValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgFunctionWithIllegalArg_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVG)
            .setAlias("avg-alias")
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    OneArgAggregationValidator avgValidator = new OneArgAggregationValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgValidator.validate(functionExpression);
        });
  }
}
