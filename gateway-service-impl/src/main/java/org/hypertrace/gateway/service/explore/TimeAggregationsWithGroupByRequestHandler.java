package org.hypertrace.gateway.service.explore;

import com.google.common.collect.Streams;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
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

  TimeAggregationsWithGroupByRequestHandler(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestHandler normalRequestHandler,
      TimeAggregationsRequestHandler timeAggregationsRequestHandler) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.normalRequestHandler = normalRequestHandler;
    this.timeAggregationsRequestHandler = timeAggregationsRequestHandler;
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
              Optional<Value> maybeInClauseValue =
                  getInClauseValues(originalRequestContext, groupBy, groupByResponse);
              maybeInClauseValue.ifPresent(
                  inClauseValue ->
                      filterBuilder.addChildFilter(
                          createInClauseChildFilter(groupBy, inClauseValue)));
            });

    return filterBuilder;
  }

  private Optional<Value> getInClauseValues(
      ExploreRequestContext originalRequestContext,
      Expression groupBy,
      ExploreResponse.Builder exploreResponse) {
    String groupByResultName =
        ExpressionReader.getAttributeIdFromAttributeSelection(groupBy).orElseThrow();
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            originalRequestContext, originalRequestContext.getContext());
    Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap =
        remapAttributeMetadataByResultName(
            originalRequestContext.getExploreRequest(), attributeMetadataMap);

    if (!resultKeyToAttributeMetadataMap.containsKey(groupByResultName)) {
      return Optional.empty();
    }

    AttributeMetadata groupByAttributeMetadata =
        resultKeyToAttributeMetadataMap.get(groupByResultName);
    Optional<ValueType> maybeValueType = getInClauseFilterValueType(groupByAttributeMetadata);
    if (maybeValueType.isEmpty()) {
      LOG.error(
          "Group by isn't supported for attribute metadata {} of expression {}",
          groupByAttributeMetadata,
          groupBy);
      return Optional.empty();
    }

    ValueType valueType = maybeValueType.get();
    Value.Builder valueBuilder = Value.newBuilder().setValueType(valueType);

    for (Row row : exploreResponse.getRowList()) {
      Value groupByValue = row.getColumnsMap().get(groupByResultName);
      buildInClauseFilterValue(groupByValue, valueBuilder);
    }

    return Optional.of(valueBuilder.build());
  }

  private Filter.Builder createInClauseChildFilter(
      Expression groupBySelectionExpression, Value inClauseValue) {
    return Filter.newBuilder()
        .setLhs(groupBySelectionExpression)
        .setOperator(Operator.IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(LiteralConstant.newBuilder().setValue(inClauseValue)));
  }

  private void buildInClauseFilterValue(Value value, Value.Builder valueBuilder) {
    switch (value.getValueType()) {
      case STRING:
        valueBuilder.addStringArray(value.getString());
        break;
      case STRING_ARRAY:
        valueBuilder.addAllStringArray(value.getStringArrayList());
        break;
      case LONG:
        valueBuilder.addLongArray(value.getLong());
        break;
      case LONG_ARRAY:
        valueBuilder.addAllLongArray(value.getLongArrayList());
        break;
      case BOOL:
        valueBuilder.addBooleanArray(value.getBoolean());
        break;
      case BOOLEAN_ARRAY:
        valueBuilder.addAllBooleanArray(value.getBooleanArrayList());
        break;
      case DOUBLE:
        valueBuilder.addDoubleArray(value.getDouble());
        break;
      case DOUBLE_ARRAY:
        valueBuilder.addAllDoubleArray(value.getDoubleArrayList());
        break;
      case TIMESTAMP:
      case STRING_MAP:
      case UNRECOGNIZED:
      case UNSET:
        LOG.error("Unable to extract value from {}", value);
    }
  }

  private Optional<ValueType> getInClauseFilterValueType(AttributeMetadata attributeMetadata) {
    // RHS value of in clause filter should always be an array to apply IN filter clause on groupBy
    // expression
    switch (attributeMetadata.getValueKind()) {
      case TYPE_STRING:
      case TYPE_STRING_ARRAY:
        return Optional.of(ValueType.STRING_ARRAY);
      case TYPE_INT64:
      case TYPE_INT64_ARRAY:
        return Optional.of(ValueType.LONG_ARRAY);
      case TYPE_DOUBLE:
      case TYPE_DOUBLE_ARRAY:
        return Optional.of(ValueType.DOUBLE_ARRAY);
      case TYPE_BOOL:
      case TYPE_BOOL_ARRAY:
        return Optional.of(ValueType.BOOLEAN_ARRAY);
      case TYPE_STRING_MAP:
      case TYPE_TIMESTAMP:
      case TYPE_BYTES:
      case UNRECOGNIZED:
      case KIND_UNDEFINED:
      default:
        return Optional.empty();
    }
  }

  private Map<String, AttributeMetadata> remapAttributeMetadataByResultName(
      ExploreRequest request, Map<String, AttributeMetadata> attributeMetadataByIdMap) {
    return AttributeMetadataUtil.remapAttributeMetadataByResultKey(
        Streams.concat(
                request.getSelectionList().stream(),
                request.getTimeAggregationList().stream().map(TimeAggregation::getAggregation),
                // Add groupBy to Selection list.
                // The expectation from the Gateway service client is that they do not add the group
                // by expressions to the selection expressions in the request
                request.getGroupByList().stream())
            .collect(Collectors.toUnmodifiableList()),
        attributeMetadataByIdMap);
  }
}
