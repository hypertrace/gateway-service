package org.hypertrace.gateway.service.common.validators.function;

import org.hypertrace.gateway.service.common.validators.Validator;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;

public abstract class FunctionExpressionValidator extends Validator {

  public void validate(FunctionExpression functionExpression) {
    // Require alias to be set
    var functionAlias = functionExpression.getAlias();
    checkArgument(
        functionAlias != null && !functionAlias.isEmpty(),
        "Function alias is null or empty for %s",
        functionExpression.getFunction());

    validateArguments(functionExpression);
  }

  protected abstract void validateArguments(FunctionExpression functionExpression);
}
