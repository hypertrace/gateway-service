package org.hypertrace.gateway.service.common.validators.aggregation;

import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeAggregationValidatorTest {

  @Test
  public void validTimeAggregation_shouldNotThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(1).setUnit("SECONDS"))
            .setAggregation(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    timeAggregationValidator.validate(timeAggregation);
  }

  @Test
  public void timeAggregationWithNoPeriod_shouldThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setAggregation(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          timeAggregationValidator.validate(timeAggregation);
        });
  }

  @Test
  public void timeAggregationWithNoExpression_shouldThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(1).setUnit("SECONDS"))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          timeAggregationValidator.validate(timeAggregation);
        });
  }

  @Test
  public void timeAggregationWithNonFunctionExpression_shouldThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(1).setUnit("SECONDS"))
            .setAggregation(Expression.newBuilder().setLiteral(LiteralConstant.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          timeAggregationValidator.validate(timeAggregation);
        });
  }

  @Test
  public void timeAggregationWithZeroPeriod_shouldThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(0).setUnit("SECONDS"))
            .setAggregation(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          timeAggregationValidator.validate(timeAggregation);
        });
  }

  @Test
  public void timeAggregationInvalidPeriodUnit_shouldThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(1).setUnit("UNKNOWN"))
            .setAggregation(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          timeAggregationValidator.validate(timeAggregation);
        });
  }

  @Test
  public void timeAggregationExpectedResultsCountWithinLimit_shouldNotThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(1).setUnit("SECONDS"))
            .setAggregation(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    timeAggregationValidator.validateWithinTimeBounds(timeAggregation, 0, 1000000);
  }

  @Test
  public void timeAggregationExpectedResultsCountWithinLimit2_shouldNotThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(1).setUnit("SECONDS"))
            .setAggregation(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    timeAggregationValidator.validateWithinTimeBounds(timeAggregation, 0, 9000000);
  }

  @Test
  public void timeAggregationExpectedResultsCountNotWithiLimit_shouldThrowException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(1).setUnit("SECONDS"))
            .setAggregation(Expression.newBuilder().setFunction(FunctionExpression.newBuilder()))
            .build();
    var timeAggregationValidator = new TimeAggregationValidator();
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> {
          timeAggregationValidator.validateWithinTimeBounds(timeAggregation, 0, 11000000);
        });
  }
}
