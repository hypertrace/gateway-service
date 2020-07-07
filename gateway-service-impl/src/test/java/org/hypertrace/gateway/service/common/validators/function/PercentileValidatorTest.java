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

public class PercentileValidatorTest {

  @Test
  public void validPercentileFunction_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();
    percentileValidator.validate(functionExpression);
  }

  @Test
  public void validPercentileFunctionWithDoublePercentile_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setDouble(80.2).setValueType(ValueType.DOUBLE))))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();
    percentileValidator.validate(functionExpression);
  }

  @Test
  public void validPercentileFunctionForZeroPercentile_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(Value.newBuilder().setValueType(ValueType.LONG))))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();
    percentileValidator.validate(functionExpression);
  }

  @Test
  public void validPercentileFunctionWithHealth_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .addArguments(Expression.newBuilder().setHealth(HealthExpression.newBuilder()))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();
    percentileValidator.validate(functionExpression);
  }

  @Test
  public void percentileFunctionWithoutPercentileArg_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          percentileValidator.validate(functionExpression);
        });
  }

  @Test
  public void percentileFunctionWithoutColumnnId_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          percentileValidator.validate(functionExpression);
        });
  }

  @Test
  public void percentileFunctionWithEmptyColumnId_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          percentileValidator.validate(functionExpression);
        });
  }

  @Test
  public void percentileFunctionWithFunctionExpression_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .addArguments(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          percentileValidator.validate(functionExpression);
        });
  }

  @Test
  public void percentileFunctionWithNonLongLiteral_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .setAlias("percentile-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setString("not-long")
                                    .setValueType(ValueType.STRING))))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          percentileValidator.validate(functionExpression);
        });
  }

  @Test
  public void percentileFunctionWithEmptyAlias_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.PERCENTILE)
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    PercentileValidator percentileValidator = new PercentileValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          percentileValidator.validate(functionExpression);
        });
  }
}
