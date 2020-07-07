package org.hypertrace.gateway.service.common.validators.function;

import org.hypertrace.gateway.service.common.validators.Validator;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregationValidator extends Validator {
  private static final Logger LOG = LoggerFactory.getLogger(AggregationValidator.class);

  private final AvgRateValidator avgRateValidator = new AvgRateValidator();
  private final PercentileValidator percentileValidator = new PercentileValidator();
  private final OneArgAggregationValidator oneArgAggregationValidator =
      new OneArgAggregationValidator();

  public void validate(Expression expression) {
    checkArgument(
        expression.hasFunction(),
        "Expression is not a FunctionExpression. Are you sure you are passing the correct Expression?");
    FunctionExpression functionExpression = expression.getFunction();

    checkArgument(
        functionExpression.getFunction() != FunctionType.NONE, "FunctionType should be specified");
    validateFunctionExpression(functionExpression);
  }

  private void validateFunctionExpression(FunctionExpression functionExpression) {
    switch (functionExpression.getFunction()) {
      case AVGRATE:
        avgRateValidator.validate(functionExpression);
        break;
      case PERCENTILE:
        percentileValidator.validate(functionExpression);
        break;
      default:
        oneArgAggregationValidator.validate(functionExpression);
    }
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
