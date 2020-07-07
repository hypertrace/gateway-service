package org.hypertrace.gateway.service.common.validators.function;

import static org.hypertrace.gateway.service.v1.common.Expression.ValueCase.HEALTH;

import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneArgAggregationValidator extends FunctionExpressionValidator {
  private static final Logger LOG = LoggerFactory.getLogger(OneArgAggregationValidator.class);

  @Override
  protected void validateArguments(FunctionExpression functionExpression) {
    var argumentsList = functionExpression.getArgumentsList();
    boolean columnIdentifierArgSet = false;
    for (var argument : argumentsList) {
      switch (argument.getValueCase()) {
        case COLUMNIDENTIFIER:
          // Need a non empty column name
          String columnName = argument.getColumnIdentifier().getColumnName();
          checkArgument(columnName != null && !columnName.isEmpty(), "columnName is null or empty");
          columnIdentifierArgSet = true;
          break;
        default:
          // Only other argument allowed is Health. All others are illegal.
          checkArgument(argument.getValueCase() == HEALTH, "Illegal argument");
      }
    }

    checkArgument(columnIdentifierArgSet, "ColumnIdentifier arg not set");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
