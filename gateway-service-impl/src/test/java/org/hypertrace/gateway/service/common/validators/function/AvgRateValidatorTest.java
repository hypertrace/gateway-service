package org.hypertrace.gateway.service.common.validators.function;

import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.HealthExpression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AvgRateValidatorTest {

  @Test
  public void validAvgRateFunction_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
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
    var avgRateFunctionValidator = new AvgRateValidator();
    avgRateFunctionValidator.validate(functionExpression);
  }

  @Test
  public void validAvgRateFunctionWithHealth_shouldNotThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
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
    var avgRateFunctionValidator = new AvgRateValidator();
    avgRateFunctionValidator.validate(functionExpression);
  }

  @Test
  public void validAvgRateFunctionWithUnrecognizedArgType_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
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
            .addArguments(Expression.newBuilder().setOrderBy(OrderByExpression.newBuilder()))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void nonAvgRateFunction_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVG)
            .setAlias("avg-alias")
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
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithoutColumnIdentifier_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithoutPeriodAsLiteral_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column-name")))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithEmptyColumnName_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
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
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithUnsetColumnName_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
            .addArguments(
                Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder()))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithUnsetPeriod_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column")))
            .addArguments(Expression.newBuilder().setLiteral(LiteralConstant.newBuilder()))
            .build();

    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithNonLongPeriod_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder()
                                    .setString("not long")
                                    .setValueType(ValueType.STRING))))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithZeroPeriod_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("avg-rate-alias")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(Value.newBuilder().setLong(0L).setValueType(ValueType.LONG))))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithNoAlias_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }

  @Test
  public void avgRateFunctionWithEmptyAlias_shouldThrowIllegalArgException() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .setAlias("")
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("test-column")))
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(
                                Value.newBuilder().setLong(20L).setValueType(ValueType.LONG))))
            .build();
    var avgRateFunctionValidator = new AvgRateValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          avgRateFunctionValidator.validate(functionExpression);
        });
  }
}
