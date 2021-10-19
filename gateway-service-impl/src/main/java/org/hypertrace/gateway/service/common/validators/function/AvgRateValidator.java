package org.hypertrace.gateway.service.common.validators.function;

import static org.hypertrace.gateway.service.v1.common.Expression.ValueCase.HEALTH;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
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
          // Need the Period to be set in ISO format
          // Added support for backward compatibility so can be long as well (do not use)

          checkArgument(
              argument.getLiteral().hasValue()
                  && (argument.getLiteral().getValue().getValueType() == ValueType.STRING
                      || argument.getLiteral().getValue().getValueType() == ValueType.LONG),
              "Period not set as a STRING/LONG Type");

          long period;
          if (argument.getLiteral().getValue().getValueType() == ValueType.STRING) {
            String periodInIso = argument.getLiteral().getValue().getString();
            period = isoDurationToSeconds(periodInIso);
          } else {
            period = argument.getLiteral().getValue().getLong();
          }

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

  private static long isoDurationToSeconds(String duration) {
    try {
      Duration d = java.time.Duration.parse(duration);
      return d.get(ChronoUnit.SECONDS);
    } catch (DateTimeParseException ex) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported string format for duration: %s, expects iso string format", duration));
    }
  }
}
