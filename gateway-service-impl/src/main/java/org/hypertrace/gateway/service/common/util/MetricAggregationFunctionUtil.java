package org.hypertrace.gateway.service.common.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Value;

/**
 * Class with some utility methods around Aggregated metrics, alias in the entity requests.
 */
public class MetricAggregationFunctionUtil {

  private static final String FUNCTION_NAME_SEPARATOR = "_";

  public static ImmutablePair<String, FunctionType> getMetricNameAggregation(String alias) {
    Preconditions.checkArgument(alias.contains(FUNCTION_NAME_SEPARATOR));

    int index = alias.indexOf(FUNCTION_NAME_SEPARATOR);
    return new ImmutablePair<>(
        alias.substring(index + 1), FunctionType.valueOf(alias.substring(0, index)));
  }

  /**
   * Given the selection expressions, returns a map from the metric name/alias to the Aggregation
   * function that was requested. This will be useful to parse the results.
   */
  public static Map<String, FunctionExpression> getAggMetricToFunction(
      List<Expression> selections) {
    Map<String, FunctionExpression> result = new HashMap<>();
    for (Expression expression : selections) {
      if (expression.getValueCase() == ValueCase.FUNCTION) {
        result.put(getAggregationFunctionAlias(expression.getFunction()), expression.getFunction());
      }
    }
    return result;
  }

  public static String getAggregationFunctionAlias(FunctionExpression functionExpression) {
    if (StringUtils.isNotEmpty(functionExpression.getAlias())) {
      return functionExpression.getAlias();
    } else {
      return functionExpression.getFunction()
          + FUNCTION_NAME_SEPARATOR
          + functionExpression.getArguments(0).getColumnIdentifier().getColumnName();
    }
  }

  public static Map<String, AttributeKind> getValueTypeFromFunction(
      Map<String, FunctionExpression> functionExpressionMap,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    Map<String, AttributeKind> aliasToValueType = new HashMap<>();
    for (Entry<String, FunctionExpression> entry : functionExpressionMap.entrySet()) {
      String alias = entry.getKey();
      FunctionExpression functionExpression = entry.getValue();
      aliasToValueType.put(
          alias, getValueTypeFromFunction(functionExpression, attributeMetadataMap));
    }
    return aliasToValueType;
  }

  public static AttributeKind getValueTypeFromFunction(
      FunctionExpression functionExpression, Map<String, AttributeMetadata> attributeMetadataMap) {
    // assumes 1 level of aggregation for now, like the rest of the code
    // Also, for the type, it should follow the outer most aggregation type
    String attributeName =
        functionExpression.getArgumentsList().stream()
            .filter(e -> e.getValueCase() == ValueCase.COLUMNIDENTIFIER)
            .map(e -> e.getColumnIdentifier().getColumnName())
            .findFirst()
            .orElseThrow(); // Should have validated the FunctionExpression using
                            // AggregationValidator

    AttributeMetadata metadata = attributeMetadataMap.get(attributeName);
    Preconditions.checkArgument(
        metadata != null,
        "Failed to find value type for this function because it is unable to find the metadata for %s",
        attributeName);

    FunctionType functionType = functionExpression.getFunction();
    switch (functionType) {
      case COUNT:
      case DISTINCTCOUNT:
        return AttributeKind.TYPE_INT64;
      case MIN:
      case MAX:
      case SUM:
        AttributeKind attributeKind = metadata.getValueKind();
        // Min/Max/Sum function only applicable to numerical data
        Preconditions.checkArgument(
            AttributeKind.TYPE_DOUBLE.equals(attributeKind)
                || AttributeKind.TYPE_INT64.equals(attributeKind),
            "Incompatible data type for this function. Function : %s,"
                + " Attribute Kind: %s. Attribute Name : %s",
            functionType.name(),
            attributeKind.name(),
            attributeName);

        return attributeKind;
      case AVGRATE:
      case AVG:
      case PERCENTILE:
        return AttributeKind.TYPE_DOUBLE;
      default:
        return metadata.getValueKind();
    }
  }

  public static Value getValueFromFunction(
          long startTime,
          long endTime,
          Map<String, AttributeMetadata> attributeMetadataMap,
          org.hypertrace.core.query.service.api.Value column,
          ColumnMetadata metadata,
          FunctionExpression functionExpression) {
    // AVG_RATE is adding a specific implementation because Pinot does not directly support this function,
    // so it has to be parsed separately.
    Value convertedValue;
    if (FunctionType.AVGRATE == functionExpression.getFunction()) {
      convertedValue =
              ArithmeticValueUtil.computeAvgRate(functionExpression, column, startTime, endTime);
    } else {
      convertedValue =
              QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
                      MetricAggregationFunctionUtil.getValueTypeFromFunction(
                              functionExpression, attributeMetadataMap),
                      attributeMetadataMap,
                      metadata,
                      column);
    }
    return convertedValue;
  }
}
