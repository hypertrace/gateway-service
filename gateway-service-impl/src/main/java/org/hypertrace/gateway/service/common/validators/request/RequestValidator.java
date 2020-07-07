package org.hypertrace.gateway.service.common.validators.request;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
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
        .filter(expr -> expr.getValueCase() == Expression.ValueCase.COLUMNIDENTIFIER)
        .forEach(expression -> validateAttributeExists(attributeMetadataMap, expression));
  }

  protected void validateAttributeExists(
      Map<String, AttributeMetadata> attributeMetadataMap, Expression expression) {
    Set<String> attributeNames = getAttributeNames(expression);
    attributeNames.forEach(
        name ->
            checkArgument(
                attributeMetadataMap.containsKey(name), "Attribute {%s} not found", name));
  }

  // This validation is too strong since some aggregatable attributes like duration are defined as
  // ATTRIBUTE instead of METRIC
  private void validateAggregationAtrribute(
      Map<String, AttributeMetadata> attributeMetadataMap, Expression expression) {
    Set<String> attributeNames = getAttributeNames(expression);
    attributeNames.forEach(
        name -> {
          checkArgument(attributeMetadataMap.containsKey(name), "Attribute {%s} not found", name);
          checkArgument(
              attributeMetadataMap.get(name).getType() == AttributeType.METRIC,
              "Attribute {%s} must be of type {METRIC} but is of type {%s}",
              name,
              attributeMetadataMap.get(name).getType());
        });
  }

  private Set<String> getAttributeNames(List<Expression> expressions) {
    Set<String> attributeNames = new HashSet<>();
    expressions.forEach(
        expr -> {
          extractColumn(attributeNames, expr);
        });
    return attributeNames;
  }

  private Set<String> getAttributeNames(Expression expression) {
    Set<String> attributeNames = new HashSet<>();
    extractColumn(attributeNames, expression);
    return attributeNames;
  }

  private void extractColumn(
      Set<String> columns, org.hypertrace.gateway.service.v1.common.Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        String columnName = expression.getColumnIdentifier().getColumnName();
        columns.add(columnName);
        break;
      case FUNCTION:
        for (org.hypertrace.gateway.service.v1.common.Expression fexp :
            expression.getFunction().getArgumentsList()) {
          extractColumn(columns, fexp);
        }
        break;
      case ORDERBY:
        extractColumn(columns, expression.getOrderBy().getExpression());
      case LITERAL:
      case VALUE_NOT_SET:
        break;
    }
  }
}
