package org.hypertrace.gateway.service.common.validators.aggregation;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.hypertrace.gateway.service.common.validators.Validator;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeAggregationValidator extends Validator {
  private static final Logger LOG = LoggerFactory.getLogger(TimeAggregationValidator.class);
  // Limit set to 10K for now
  private static final long MAX_RESULTS = 10000L;

  public void validate(TimeAggregation timeAggregation) {
    checkArgument(
        timeAggregation.getAggregation().getValueCase() == Expression.ValueCase.FUNCTION,
        "TimeAggregation Expression should be a Function Expression");
    checkArgument(timeAggregation.hasPeriod(), "TimeAggregation should have period");
    checkArgument(
        timeAggregation.getPeriod().getValue() > 0, "TimeAggregation period should be > 0");
    // Validate that ChronoUnit can convert the period unit.
    ChronoUnit.valueOf(timeAggregation.getPeriod().getUnit());
  }

  public void validateWithinTimeBounds(
      TimeAggregation timeAggregation, long startTimeMillis, long endTimeMillis) {
    validate(timeAggregation);
    var timeRangeSeconds = (endTimeMillis - startTimeMillis) / 1000;
    ChronoUnit unit = ChronoUnit.valueOf(timeAggregation.getPeriod().getUnit());
    var periodSeconds =
        Duration.of(timeAggregation.getPeriod().getValue(), unit).get(ChronoUnit.SECONDS);
    checkState(
        (timeRangeSeconds / periodSeconds) < MAX_RESULTS,
        "TimeAggregation should return less than %s results",
        MAX_RESULTS);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
