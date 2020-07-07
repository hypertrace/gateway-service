package org.hypertrace.gateway.service.common.util;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ArithmeticValueUtilTest {

  @Test
  public void testDivideValid() {
    Value expectedResult =
        Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(10.0).build();
    {
      Value value =
          ArithmeticValueUtil.divide(
              Value.newBuilder().setLong(100).setValueType(ValueType.LONG).build(), 10.0);
      Assertions.assertEquals(expectedResult, value);
    }

    {
      Value value =
          ArithmeticValueUtil.divide(
              Value.newBuilder().setDouble(100.0).setValueType(ValueType.DOUBLE).build(), 10.0);
      Assertions.assertEquals(expectedResult, value);
    }

    {
      Value value =
          ArithmeticValueUtil.divide(
              Value.newBuilder().setString("100").setValueType(ValueType.STRING).build(), 10.0);
      Assertions.assertEquals(expectedResult, value);
    }
  }

  @Test
  public void testDivideInvalid1() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          ArithmeticValueUtil.divide(
              Value.newBuilder().setValueType(ValueType.LONG).setLong(100).build(), 0.0);
        });
  }

  @Test
  public void testDivideInvalid2() {
    for (ValueType valueType :
        List.of(
            ValueType.BOOL,
            ValueType.STRING_ARRAY,
            ValueType.LONG_ARRAY,
            ValueType.BOOLEAN_ARRAY,
            ValueType.DOUBLE_ARRAY)) {
      try {
        ArithmeticValueUtil.divide(Value.newBuilder().setValueType(valueType).build(), 10.0);
      } catch (IllegalArgumentException ex) {
        // Expected
        continue;
      }
      Assertions.fail("Expecting IllegalArgumentException");
    }
  }

  @Test
  public void testComputeAvgRateSixtySecondPeriod() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(Value.newBuilder().setLong(60).setValueType(ValueType.LONG))))
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("API.bytes_received")))
            .build();

    org.hypertrace.core.query.service.api.Value originalValue =
        org.hypertrace.core.query.service.api.Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
            .setLong(120)
            .build();
    Value value =
        ArithmeticValueUtil.computeAvgRate(
            functionExpression,
            originalValue,
            TimeUnit.SECONDS.toMillis(0),
            TimeUnit.SECONDS.toMillis(120));
    Assertions.assertEquals(ValueType.DOUBLE, value.getValueType());
    Assertions.assertEquals(60.0, value.getDouble(), 0.001);
  }

  @Test
  public void testComputeAvgRateOneSecondPeriod() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .addArguments(
                Expression.newBuilder()
                    .setLiteral(
                        LiteralConstant.newBuilder()
                            .setValue(Value.newBuilder().setLong(1).setValueType(ValueType.LONG))))
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("API.bytes_received")))
            .build();

    org.hypertrace.core.query.service.api.Value originalValue =
        org.hypertrace.core.query.service.api.Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
            .setLong(120)
            .build();
    Value value =
        ArithmeticValueUtil.computeAvgRate(
            functionExpression,
            originalValue,
            TimeUnit.SECONDS.toMillis(0),
            TimeUnit.SECONDS.toMillis(120));
    Assertions.assertEquals(ValueType.DOUBLE, value.getValueType());
    Assertions.assertEquals(1.0, value.getDouble(), 0.001);
  }

  @Test
  public void testComputeAvgRateNoPeriod() {
    FunctionExpression functionExpression =
        FunctionExpression.newBuilder()
            .setFunction(FunctionType.AVGRATE)
            .addArguments(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("API.bytes_received")))
            .build();

    org.hypertrace.core.query.service.api.Value originalValue =
        org.hypertrace.core.query.service.api.Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
            .setLong(120)
            .build();
    Assertions.assertThrows(
        NoSuchElementException.class,
        () -> {
          ArithmeticValueUtil.computeAvgRate(
              functionExpression,
              originalValue,
              TimeUnit.SECONDS.toMillis(0),
              TimeUnit.SECONDS.toMillis(120));
        });
  }
}
