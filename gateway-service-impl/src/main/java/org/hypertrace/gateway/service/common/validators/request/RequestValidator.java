package org.hypertrace.gateway.service.common.validators.request;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.validators.Validator;
import org.hypertrace.gateway.service.common.validators.aggregation.TimeAggregationValidator;
import org.hypertrace.gateway.service.common.validators.function.AggregationValidator;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;

public abstract class RequestValidator<REQUEST> extends Validator {
  private final AggregationValidator aggregationValidator = new AggregationValidator();
  private final TimeAggregationValidator timeAggregationValidator = new TimeAggregationValidator();

  public abstract void validate(
      REQUEST request, Map<String, AttributeMetadata> attributeMetadataMap);

  protected void validateTimeAggregations(
      List<TimeAggregation> timeAggregations,
      long startTimeMillis,
      long endTimeMillis,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    timeAggregations.forEach(
        timeAggregation ->
            validateTimeAggregation(
                timeAggregation, startTimeMillis, endTimeMillis, attributeMetadataMap));
  }

  private void validateTimeAggregation(
      TimeAggregation timeAggregation,
      long startTimeMillis,
      long endTimeMillis,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    timeAggregationValidator.validateWithinTimeBounds(
        timeAggregation, startTimeMillis, endTimeMillis);
    checkArgument(timeAggregation.hasAggregation(), "Time aggregation should have an aggregation");
    validateAttributeExists(attributeMetadataMap, timeAggregation.getAggregation());
    checkArgument(
        timeAggregation.getAggregation().hasFunction(),
        "Time aggregation expression should be a function");
    aggregationValidator.validate(timeAggregation.getAggregation());
  }

  protected void validateFunctionExpressions(
      List<Expression> selections, Map<String, AttributeMetadata> attributeMetadataMap) {
    selections.stream()
        .filter(expr -> expr.getValueCase() == Expression.ValueCase.FUNCTION)
        .forEach(expression -> validateFunctionExpression(expression, attributeMetadataMap));
  }

  private void validateFunctionExpression(
      Expression expression, Map<String, AttributeMetadata> attributeMetadataMap) {
    validateAttributeExists(attributeMetadataMap, expression);
    aggregationValidator.validate(expression);
  }

  protected void validateSimpleSelections(
      List<Expression> selections, Map<String, AttributeMetadata> attributeMetadataMap) {
    selections.stream()
        .filter(ExpressionReader::isAttributeSelection)
        .forEach(expression -> validateAttributeExists(attributeMetadataMap, expression));
  }

  protected void validateAttributeExists(
      Map<String, AttributeMetadata> attributeMetadataMap, Expression expression) {
    Set<String> attributeIds = ExpressionReader.extractAttributeIds(expression);
    attributeIds.forEach(
        name ->
            checkArgument(
                attributeMetadataMap.containsKey(name), "Attribute {%s} not found", name));
  }
}
