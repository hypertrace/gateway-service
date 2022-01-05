package org.hypertrace.gateway.service.common.validators.function;

import static java.util.function.Predicate.not;
import static org.hypertrace.gateway.service.v1.common.Expression.ValueCase.HEALTH;

import java.util.Optional;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneArgAggregationValidator extends FunctionExpressionValidator {
  private static final Logger LOG = LoggerFactory.getLogger(OneArgAggregationValidator.class);

  @Override
  protected void validateArguments(FunctionExpression functionExpression) {
    var argumentsList = functionExpression.getArgumentsList();
    boolean attributeIdArgSet = false;
    for (var argument : argumentsList) {
      switch (argument.getValueCase()) {
        case COLUMNIDENTIFIER:
        case ATTRIBUTE_EXPRESSION:
          // Need a non empty attribute ID
          Optional<String> attributeId =
              ExpressionReader.getAttributeIdFromAttributeSelection(argument);
          checkArgument(
              attributeId.filter(not(String::isEmpty)).isPresent(), "attributeId is missing");
          attributeIdArgSet = true;
          break;
        default:
          // Only other argument allowed is Health. All others are illegal.
          checkArgument(argument.getValueCase() == HEALTH, "Illegal argument");
      }
    }

    checkArgument(attributeIdArgSet, "AttributeId arg not set");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
