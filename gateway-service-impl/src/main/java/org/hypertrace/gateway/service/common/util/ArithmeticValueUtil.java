package org.hypertrace.gateway.service.common.util;

import java.util.concurrent.TimeUnit;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArithmeticValueUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ArithmeticValueUtil.class);

  public static Value divide(Value dividend, Double divisor) {
    if (divisor == null || Double.max(divisor, 0) <= 0) {
      LOG.error("Attempted division by null or 0");
      throw new IllegalArgumentException("Attempted to divide by 0");
    }
    double result = 0.0;
    switch (dividend.getValueType()) {
      case LONG:
        result = ((double) dividend.getLong() / divisor);
        break;
      case DOUBLE:
        result = dividend.getDouble() / divisor;
        break;
      case STRING:
        result = Double.parseDouble(dividend.getString()) / divisor;
        break;
      case BOOL:
      case LONG_ARRAY:
      case DOUBLE_ARRAY:
      case STRING_ARRAY:
      case BOOLEAN_ARRAY:
      case TIMESTAMP:
      case STRING_MAP:
      case UNRECOGNIZED:
        LOG.error("Unsupported format for division, requested format: {}", dividend.getValueType());
        throw new IllegalArgumentException(
            String.format("Invalid value for divide operation:%s", dividend));
    }

    return Value.newBuilder().setDouble(result).setValueType(ValueType.DOUBLE).build();
  }

  public static Value computeAvgRate(
      FunctionExpression originalFunctionExpression,
      org.hypertrace.core.query.service.api.Value originalValue,
      long startTime,
      long endTime) {
    Value sumValue = QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(originalValue);

    // Read the Period from the original expression. If not found throw an exception. This should
    // have been validated by the AvgRateValidator. The period should always be set.
    Expression periodExpression =
        originalFunctionExpression.getArgumentsList().stream()
            .filter(org.hypertrace.gateway.service.v1.common.Expression::hasLiteral)
            .findFirst()
            .orElseThrow();

    long period = periodExpression.getLiteral().getValue().getLong();

    Double divisor = ((double) endTime - startTime) / TimeUnit.SECONDS.toMillis(period);
    return ArithmeticValueUtil.divide(sumValue, divisor);
  }
}
