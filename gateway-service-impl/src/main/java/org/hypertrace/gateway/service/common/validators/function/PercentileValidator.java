package org.hypertrace.gateway.service.common.validators.function;

import static java.util.function.Predicate.not;
import static org.hypertrace.gateway.service.v1.common.Expression.ValueCase.HEALTH;

import java.util.Optional;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PercentileValidator extends FunctionExpressionValidator {
  private static final Logger LOG = LoggerFactory.getLogger(PercentileValidator.class);

  @Override
  protected void validateArguments(FunctionExpression functionExpression) {
    checkArgument(
        functionExpression.getFunction() == FunctionType.PERCENTILE,
        "Incorrect function type: %s. You might be executing the incorrect validator",
        functionExpression.getFunction());

    var argumentsList = functionExpression.getArgumentsList();
    boolean attributeIdArgSet = false;
    boolean percentileArgSet = false;
    for (var argument : argumentsList) {
      switch (argument.getValueCase()) {
        case COLUMNIDENTIFIER:
        case ATTRIBUTE_EXPRESSION:
          // Need a non empty attribute ID
          Optional<String> attributeId = ExpressionReader.getSelectionAttributeId(argument);
          checkArgument(
              attributeId.filter(not(String::isEmpty)).isPresent(), "attributeId is missing");
          attributeIdArgSet = true;
          break;
        case LITERAL:
          // Need the percentile to be set
          checkArgument(
              argument.getLiteral().hasValue() && literalValueIsValid(argument.getLiteral()),
              "Percentile arg not set as a Long or Double Type");
          percentileArgSet = true;
          break;
        default:
          // Only other argument allowed is Health. All others are illegal.
          checkArgument(argument.getValueCase() == HEALTH, "Illegal argument");
      }
    }

    checkArgument(attributeIdArgSet, "ColumnIdentifier arg not set");
    checkArgument(percentileArgSet, "Percentile arg not set");
  }

  private boolean literalValueIsValid(LiteralConstant literalConstant) {
    return literalConstant.getValue().getValueType() == ValueType.LONG
        || literalConstant.getValue().getValueType() == ValueType.DOUBLE;
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
