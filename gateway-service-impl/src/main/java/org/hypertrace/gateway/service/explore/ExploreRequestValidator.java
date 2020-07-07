package org.hypertrace.gateway.service.explore;

import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.validators.request.RequestValidator;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExploreRequestValidator extends RequestValidator<ExploreRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreRequestValidator.class);

  @Override
  protected Logger getLogger() {
    return LOG;
  }

  public void validate(
      ExploreRequest request, Map<String, AttributeMetadata> attributeMetadataMap) {
    checkArgument(!request.getContext().isEmpty(), "'context' not specified in the request.");
    // Validate limit > 0. Require clients to set it.
    checkArgument(request.getLimit() > 0, "Limit should be non zero");

    // Check that one of selections or time aggregations is not empty and that both of them do not
    // have a non-zero length
    validateEitherSelectionsOrTimeAggregations(request);
    // Check that all the selections are other column or function
    validateAllSelectionsAreEitherColumnOrAggregations(request.getSelectionList());

    validateFunctionExpressions(request.getSelectionList(), attributeMetadataMap);
    validateTimeAggregations(
        request.getTimeAggregationList(),
        request.getStartTimeMillis(),
        request.getEndTimeMillis(),
        attributeMetadataMap);
    validateSimpleSelections(request.getSelectionList(), attributeMetadataMap);
    validateGroupBy(request.getGroupByList(), request.getSelectionList(), attributeMetadataMap);
    // Validate all time aggregations have the same period. Required by Explore API Design
    validateAllTimeAggregationsHaveTheSamePeriod(request.getTimeAggregationList());
  }

  private void validateGroupBy(
      List<Expression> groupByExpressions,
      List<Expression> selections,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    checkArgument(
        groupByExpressions.stream().allMatch(Expression::hasColumnIdentifier),
        "Group By expressions should all be on columns(for now).");
    validateSimpleSelections(groupByExpressions, attributeMetadataMap);

    // validate that when there are GroupBy expressions, all the selections are aggregations
    if (!groupByExpressions.isEmpty()) {
      checkArgument(
          selections.stream().allMatch(Expression::hasFunction),
          "When Group By is specified, all selections should be Aggregations.");
    }
  }

  private void validateAllTimeAggregationsHaveTheSamePeriod(
      List<TimeAggregation> timeAggregations) {
    timeAggregations.stream()
        .findFirst()
        .ifPresent(
            timeAggregation -> {
              Period period = timeAggregation.getPeriod();
              checkArgument(
                  timeAggregations.stream()
                      .allMatch(timeAggregation1 -> period.equals(timeAggregation1.getPeriod())),
                  "All time aggregations should have the same period. Required by Design.");
            });
  }

  private void validateEitherSelectionsOrTimeAggregations(ExploreRequest request) {
    checkArgument(
        request.getSelectionList().isEmpty() ^ request.getTimeAggregationList().isEmpty(),
        "The request should have only selections or only time aggregations. Not a mixture and one of them should not be empty.");
  }

  private void validateAllSelectionsAreEitherColumnOrAggregations(List<Expression> selections) {
    checkArgument(
        selections.stream().allMatch(Expression::hasColumnIdentifier)
            || selections.stream().allMatch(Expression::hasFunction),
        "The selections should either be all columns or all aggregations");
  }
}
