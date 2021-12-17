package org.hypertrace.gateway.service.common.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;

/** Class with some utility methods around Aggregated metrics, alias in the entity requests. */
public class MetricAggregationFunctionUtil {

  /**
   * Given the selection expressions, returns a map from the metric name/alias to the Aggregation
   * function that was requested. This will be useful to parse the results.
   */
  public static Map<String, FunctionExpression> getAggMetricToFunction(
      List<Expression> selections) {
    Map<String, FunctionExpression> result = new HashMap<>();
    for (Expression expression : selections) {
      if (expression.getValueCase() == ValueCase.FUNCTION) {
        result.put(
            ExpressionReader.getSelectionResultName(expression).orElseThrow(),
            expression.getFunction());
      }
    }
    return result;
  }

  public static Map<String, AttributeKind> getValueTypeForFunctionType(
      Map<String, FunctionExpression> functionExpressionMap,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    Map<String, AttributeKind> aliasToValueType = new HashMap<>();
    for (Entry<String, FunctionExpression> entry : functionExpressionMap.entrySet()) {
      String alias = entry.getKey();
      FunctionExpression functionExpression = entry.getValue();
      aliasToValueType.put(
          alias, getValueTypeForFunctionType(functionExpression, attributeMetadataMap));
    }
    return aliasToValueType;
  }

  public static AttributeKind getValueTypeForFunctionType(
      FunctionExpression functionExpression, Map<String, AttributeMetadata> attributeMetadataMap) {
    // assumes 1 level of aggregation for now, like the rest of the code
    // Also, for the type, it should follow the outer most aggregation type
    String attributeId =
        ExpressionReader.getSelectionAttributeId(functionExpression)
            .orElseThrow(); // Should have validated the FunctionExpression using
    // AggregationValidator

    AttributeMetadata metadata = attributeMetadataMap.get(attributeId);
    Preconditions.checkArgument(
        metadata != null,
        "Failed to find value type for this function because it is unable to find the metadata for %s",
        attributeId);

    return getValueTypeForFunctionType(functionExpression.getFunction(), metadata);
  }

  public static AttributeKind getValueTypeForFunctionType(
      FunctionType functionType, AttributeMetadata attributeMetadata) {
    switch (functionType) {
      case COUNT:
      case DISTINCTCOUNT:
        return AttributeKind.TYPE_INT64;
      case MIN:
      case MAX:
      case SUM:
        AttributeKind attributeKind = attributeMetadata.getValueKind();
        // Min/Max/Sum function only applicable to numerical data
        Preconditions.checkArgument(
            AttributeKind.TYPE_DOUBLE.equals(attributeKind)
                || AttributeKind.TYPE_INT64.equals(attributeKind),
            "Incompatible data type for this function. Function : %s,"
                + " Attribute Kind: %s. Attribute ID : %s",
            functionType.name(),
            attributeKind.name(),
            attributeMetadata.getId());

        return attributeKind;
      case AVGRATE:
      case AVG:
      case PERCENTILE:
        return AttributeKind.TYPE_DOUBLE;
      default:
        return attributeMetadata.getValueKind();
    }
  }
}
