package org.hypertrace.gateway.service.explore;

import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

public class TimeAggregationsWithGroupByRequestHandler implements IRequestHandler {

  private final RequestHandler normalRequestHandler;
  private final TimeAggregationsRequestHandler timeAggregationsRequestHandler;

  TimeAggregationsWithGroupByRequestHandler(
      QueryServiceClient queryServiceClient, int requestTimeout,
      AttributeMetadataProvider attributeMetadataProvider) {
    this.normalRequestHandler =
        new RequestHandler(queryServiceClient, requestTimeout, attributeMetadataProvider);
    this.timeAggregationsRequestHandler = new TimeAggregationsRequestHandler(
        queryServiceClient, requestTimeout, attributeMetadataProvider);
  }

  @Override
  public ExploreResponse.Builder handleRequest(
      ExploreRequestContext requestContext, ExploreRequest request) {
    // 1. Create a GroupBy request and get the response for the GroupBy
    ExploreRequest groupByRequest = buildGroupByRequest(request);
    ExploreRequestContext groupByRequestContext =
        new ExploreRequestContext(
            requestContext.getTenantId(), groupByRequest, requestContext.getHeaders());
    ExploreResponse.Builder groupByResponse =
        normalRequestHandler.handleRequest(groupByRequestContext, groupByRequest);

    // No need for a second query if no results.
    if (groupByResponse.getRowBuilderList().isEmpty()) {
      return ExploreResponse.newBuilder();
    }

    // 2. Create a Time Aggregations request for the groups found in the request above. This will be
    // the actual query response
    ExploreRequest timeAggregationsRequest = buildTimeAggregationsRequest(request, groupByResponse);
    ExploreRequestContext timeAggregationsRequestContext =
        new ExploreRequestContext(
            requestContext.getTenantId(), timeAggregationsRequest, requestContext.getHeaders());
    ExploreResponse.Builder timeAggregationsResponse =
        timeAggregationsRequestHandler.handleRequest(
            timeAggregationsRequestContext, timeAggregationsRequest);

    // 3. If includeRestGroup is set, invoke TheRestGroupRequestHandler
    if (request.getIncludeRestGroup()) {
      timeAggregationsRequestHandler
          .getTheRestGroupRequestHandler()
          .getRowsForTheRestGroup(requestContext, request, timeAggregationsResponse);
    }

    return timeAggregationsResponse;
  }

  private ExploreRequest buildGroupByRequest(ExploreRequest originalRequest) {
    ExploreRequest.Builder requestBuilder =
        ExploreRequest.newBuilder(originalRequest)
            .clearTimeAggregation() // Clear the time aggregations. We will move the time
                                    // aggregations expressions into selections
            .setIncludeRestGroup(
                false); // Set includeRestGroup to false. We will handle the Rest group results
                        // separately

    // Move Time aggregation expressions to selections.
    originalRequest
        .getTimeAggregationList()
        .forEach(timeAggregation -> requestBuilder.addSelection(timeAggregation.getAggregation()));

    return requestBuilder.build();
  }

  private ExploreRequest buildTimeAggregationsRequest(
      ExploreRequest originalRequest, ExploreResponse.Builder groupByResponse) {
    ExploreRequest.Builder requestBuilder =
        ExploreRequest.newBuilder(originalRequest)
            .setIncludeRestGroup(
                false) // Set includeRestGroup to false. We will handle the Rest group results
                       // separately
            .setLimit(
                groupByResponse
                    .getRowCount()); // Set limit to be size of groupByResponse rows count.

    // Create an "IN clause" filter to fetch time series only for the matching groups in the Group
    // By Response
    Filter.Builder inClauseFilter =
        createInClauseFilterFromGroupByResults(originalRequest, groupByResponse);
    if (requestBuilder.hasFilter()
        && !(requestBuilder.getFilter().equals(Filter.getDefaultInstance()))) {
      requestBuilder.getFilterBuilder().addChildFilter(inClauseFilter);
    } else {
      requestBuilder.setFilter(inClauseFilter);
    }

    return requestBuilder.build();
  }

  private Filter.Builder createInClauseFilterFromGroupByResults(
      ExploreRequest originalRequest, ExploreResponse.Builder groupByResponse) {
    Filter.Builder filterBuilder = Filter.newBuilder();
    filterBuilder.setOperator(Operator.AND);
    originalRequest
        .getGroupByList()
        .forEach(
            groupBy -> {
              String columnName = groupBy.getColumnIdentifier().getColumnName();
              Set<String> inClauseValues = getInClauseValues(columnName, groupByResponse);
              filterBuilder.addChildFilter(createInClauseChildFilter(columnName, inClauseValues));
            });

    return filterBuilder;
  }

  private Set<String> getInClauseValues(
      String columnName, ExploreResponse.Builder exploreResponse) {
    return exploreResponse.getRowBuilderList().stream()
        .map(row -> row.getColumnsMap().get(columnName))
        .map(Value::getString)
        .collect(Collectors.toUnmodifiableSet());
  }

  private Filter.Builder createInClauseChildFilter(String columnName, Set<String> inClauseValues) {
    return Filter.newBuilder()
        .setLhs(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName)))
        .setOperator(Operator.IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING_ARRAY)
                                .addAllStringArray(inClauseValues))));
  }
}
