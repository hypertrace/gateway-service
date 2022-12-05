package org.hypertrace.gateway.service.explore;

import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeAggregationsWithGroupByRequestHandler implements IRequestHandler {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimeAggregationsWithGroupByRequestHandler.class);

  private final AttributeMetadataProvider attributeMetadataProvider;
  private final RequestHandler normalRequestHandler;
  private final TimeAggregationsRequestHandler timeAggregationsRequestHandler;

  TimeAggregationsWithGroupByRequestHandler(
      QueryServiceClient queryServiceClient, AttributeMetadataProvider attributeMetadataProvider) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.normalRequestHandler = new RequestHandler(queryServiceClient, attributeMetadataProvider);
    this.timeAggregationsRequestHandler =
        new TimeAggregationsRequestHandler(queryServiceClient, attributeMetadataProvider);
  }

  @Override
  public ExploreResponse.Builder handleRequest(
      ExploreRequestContext requestContext, ExploreRequest request) {
    // This type of handler is always a group by
    requestContext.setHasGroupBy(true);
    // 1. Create a GroupBy request and get the response for the GroupBy
    ExploreRequest groupByRequest = buildGroupByRequest(request);
    ExploreRequestContext groupByRequestContext =
        new ExploreRequestContext(requestContext.getGrpcContext(), groupByRequest);
    ExploreResponse.Builder groupByResponse =
        normalRequestHandler.handleRequest(groupByRequestContext, groupByRequest);

    // No need for a second query if no results.
    if (groupByResponse.getRowBuilderList().isEmpty()) {
      return ExploreResponse.newBuilder();
    }

    // 2. Create a Time Aggregations request for the groups found in the request above. This will be
    // the actual query response
    ExploreRequest timeAggregationsRequest =
        buildTimeAggregationsRequest(requestContext, request, groupByResponse);
    ExploreRequestContext timeAggregationsRequestContext =
        new ExploreRequestContext(requestContext.getGrpcContext(), timeAggregationsRequest);
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
            .clearOffset() // Overall request offset doesn't apply to getting the actual groups
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
      ExploreRequestContext originalRequestContext,
      ExploreRequest originalRequest,
      ExploreResponse.Builder groupByResponse) {
    ExploreRequest.Builder requestBuilder =
        ExploreRequest.newBuilder(originalRequest)
            .setIncludeRestGroup(
                false); // Set includeRestGroup to false. Rest group results handled separately

    // Create an "IN clause" filter to fetch time series only for the matching groups in the Group
    // By Response
    Filter.Builder inClauseFilter =
        createInClauseFilterFromGroupByResults(
            originalRequestContext, originalRequest, groupByResponse);
    if (requestBuilder.hasFilter()
        && !(requestBuilder.getFilter().equals(Filter.getDefaultInstance()))) {
      requestBuilder.getFilterBuilder().addChildFilter(inClauseFilter);
    } else {
      requestBuilder.setFilter(inClauseFilter);
    }

    return requestBuilder.build();
  }

  private Filter.Builder createInClauseFilterFromGroupByResults(
      ExploreRequestContext originalRequestContext,
      ExploreRequest originalRequest,
      ExploreResponse.Builder groupByResponse) {
    Filter.Builder filterBuilder = Filter.newBuilder();
    filterBuilder.setOperator(Operator.AND);
    originalRequest
        .getGroupByList()
        .forEach(
            groupBy -> {
              Optional<Value> maybeInClauseValues =
                  getInClauseValues(originalRequestContext, groupBy, groupByResponse);
              maybeInClauseValues.ifPresent(
                  inClauseValues ->
                      filterBuilder.addChildFilter(
                          createInClauseChildFilter(groupBy, inClauseValues)));
            });

    return filterBuilder;
  }

  private Optional<Value> getInClauseValues(
      ExploreRequestContext originalRequestContext,
      Expression groupBy,
      ExploreResponse.Builder exploreResponse) {
    String groupByResultName = ExpressionReader.getSelectionResultName(groupBy).orElseThrow();
    Optional<AttributeMetadata> maybeGroupByAttributeMetadata =
        attributeMetadataProvider.getAttributeMetadata(
            originalRequestContext, originalRequestContext.getContext(), groupByResultName);

    if (maybeGroupByAttributeMetadata.isEmpty()) {
      return Optional.empty();
    }

    AttributeMetadata groupByAttributeMetadata = maybeGroupByAttributeMetadata.get();
    // RHS value of in clause filter should always be an array to apply IN filter clause on groupBy
    // expression
    Value.Builder valueBuilder = Value.newBuilder();
    switch (groupByAttributeMetadata.getValueKind()) {
      case TYPE_STRING:
      case TYPE_STRING_ARRAY:
        valueBuilder.setValueType(ValueType.STRING_ARRAY);
        break;
      case TYPE_INT64:
      case TYPE_INT64_ARRAY:
        valueBuilder.setValueType(ValueType.LONG_ARRAY);
        break;
      case TYPE_DOUBLE:
      case TYPE_DOUBLE_ARRAY:
        valueBuilder.setValueType(ValueType.DOUBLE_ARRAY);
        break;
      case TYPE_BOOL:
      case TYPE_BOOL_ARRAY:
        valueBuilder.setValueType(ValueType.BOOLEAN_ARRAY);
        break;
      case TYPE_STRING_MAP:
      case TYPE_TIMESTAMP:
      case TYPE_BYTES:
      case UNRECOGNIZED:
      case KIND_UNDEFINED:
        LOG.error(
            "Group by isn't supported for attribute metadata {} of expression {}",
            groupByAttributeMetadata,
            groupBy);
        break;
    }

    for (Row row : exploreResponse.getRowList()) {
      Value groupByValue = row.getColumnsMap().get(groupByResultName);
      switch (groupByValue.getValueType()) {
        case STRING:
          valueBuilder.addStringArray(groupByValue.getString());
          continue;
        case STRING_ARRAY:
          valueBuilder.addAllStringArray(groupByValue.getStringArrayList());
          continue;
        case LONG:
          valueBuilder.addLongArray(groupByValue.getLong());
          continue;
        case LONG_ARRAY:
          valueBuilder.addAllLongArray(groupByValue.getLongArrayList());
          continue;
        case BOOL:
          valueBuilder.addBooleanArray(groupByValue.getBoolean());
          continue;
        case BOOLEAN_ARRAY:
          valueBuilder.addAllBooleanArray(groupByValue.getBooleanArrayList());
          continue;
        case DOUBLE:
          valueBuilder.addDoubleArray(groupByValue.getDouble());
          continue;
        case DOUBLE_ARRAY:
          valueBuilder.addAllDoubleArray(groupByValue.getDoubleArrayList());
          continue;
        case TIMESTAMP:
        case STRING_MAP:
        case UNRECOGNIZED:
        case UNSET:
          LOG.error(
              "Unable to extract value for column {} and value {}", groupByResultName, groupByValue);
      }
    }

    if (valueBuilder.getValueType() == null) {
      return Optional.empty();
    }

    return Optional.of(valueBuilder.build());
  }

  private Filter.Builder createInClauseChildFilter(
      Expression groupBySelectionExpression, Value inClauseValues) {
    return Filter.newBuilder()
        .setLhs(groupBySelectionExpression)
        .setOperator(Operator.IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(LiteralConstant.newBuilder().setValue(inClauseValues)));
  }
}
