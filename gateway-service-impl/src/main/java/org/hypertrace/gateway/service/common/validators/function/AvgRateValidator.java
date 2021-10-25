package org.hypertrace.gateway.service.common.validators.function;

import static org.hypertrace.gateway.service.v1.common.Expression.ValueCase.HEALTH;

import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvgRateValidator extends FunctionExpressionValidator {
  private static final Logger LOG = LoggerFactory.getLogger(AvgRateValidator.class);

  @Override
  protected void validateArguments(FunctionExpression functionExpression) {
    checkArgument(
        functionExpression.getFunction() == FunctionType.AVGRATE,
        "Incorrect function type: %s. You might be executing the incorrect validator",
        functionExpression.getFunction());

    var argumentsList = functionExpression.getArgumentsList();
    boolean columnIdentifierArgSet = false;
    boolean periodArgSet = false;
    for (var argument : argumentsList) {
      switch (argument.getValueCase()) {
        case COLUMNIDENTIFIER:
          // Need a non empty column name
          String columnName = argument.getColumnIdentifier().getColumnName();
          checkArgument(columnName != null && !columnName.isEmpty(), "columnName is null or empty");
          columnIdentifierArgSet = true;
          break;
        case LITERAL:
          // Need the Period to be set
          checkArgument(
              argument.getLiteral().hasValue()
                  && argument.getLiteral().getValue().getValueType() == ValueType.LONG,
              "Period not set as a Long Type");
          Long period = argument.getLiteral().getValue().getLong();
          checkArgument(period > 0L, "Period should be > 0");
          periodArgSet = true;
          break;
        default:
          // Only other argument allowed is Health. All others are illegal.
          checkArgument(argument.getValueCase() == HEALTH, "Illegal argument");
      }
    }

    checkArgument(columnIdentifierArgSet, "ColumnIdentifier arg not set");
    checkArgument(periodArgSet, "Period arg not set");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
