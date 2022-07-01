package org.hypertrace.gateway.service.explore;

import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createTimeColumnGroupByExpression;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ColumnName;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeAggregationsRequestHandler extends RequestHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TimeAggregationsRequestHandler.class);

  TimeAggregationsRequestHandler(
      QueryServiceClient queryServiceClient, AttributeMetadataProvider attributeMetadataProvider) {
    super(queryServiceClient, attributeMetadataProvider);
  }

  @Override
  QueryRequest buildQueryRequest(
      ExploreRequestContext requestContext,
      ExploreRequest request,
      AttributeMetadataProvider attributeMetadataProvider) {
    // Set hasGroupBy=true in the request context since we will group by the timestamp column
    // regardless of the presence of a groupBy or not.
    requestContext.setHasGroupBy(true);
    QueryRequest.Builder builder = QueryRequest.newBuilder();

    // 1. Align the startTime and endTime with period boundaries if there are TimeAggregations
    request = createPeriodBoundaryAlignedExploreRequest(request);

    // 2. Add filter
    builder.setFilter(
        constructQueryServiceFilter(request, requestContext, attributeMetadataProvider));

    // 3.  Add TimeAggregations
    addTimeAggregationsToRequest(request, builder, requestContext, attributeMetadataProvider);

    // 4. Add GroupBy
    addGroupByExpressions(builder, request);

    // 5. Set Limit.
    builder.setLimit(request.getLimit());
    requestContext.setOrderByExpressions(getRequestOrderByExpressions(request));

    return builder.build();
  }

  /**
   * Need to add the interval start timer Order By expression so that after we get the results from
   * Query Service, we can sort by this column.
   *
   * @param request
   * @return
   */
  @Override
  public List<OrderByExpression> getRequestOrderByExpressions(ExploreRequest request) {
    List<OrderByExpression> existingOrderBys = super.getRequestOrderByExpressions(request);
    List<OrderByExpression> resolvedOrderBys = new ArrayList<>();

    if (!this.containsIntervalOrdering(existingOrderBys)) {
      // Create an OrderBy Expression based on the interval start time column name. We will need to
      // sort based on this as the first column.
      OrderByExpression defaultIntervalOrdering =
          OrderByExpression.newBuilder()
              .setOrder(SortOrder.ASC)
              .setExpression(
                  QueryExpressionUtil.buildAttributeExpression(
                      ColumnName.INTERVAL_START_TIME.name()))
              .build();

      resolvedOrderBys.add(defaultIntervalOrdering);
    }

    resolvedOrderBys.addAll(existingOrderBys);

    return resolvedOrderBys;
  }

  private boolean containsIntervalOrdering(List<OrderByExpression> orderByExpressions) {
    return orderByExpressions.stream()
        .map(OrderByExpression::getExpression)
        .map(ExpressionReader::getAttributeIdFromAttributeSelection)
        .flatMap(Optional::stream)
        .anyMatch(name -> name.equals(ColumnName.INTERVAL_START_TIME.name()));
  }

  private void addTimeAggregationsToRequest(
      ExploreRequest request,
      QueryRequest.Builder builder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    // Convert Time aggregations to selections.
    request
        .getTimeAggregationList()
        .forEach(
            timeAggregation ->
                addTimeAggregationToRequest(timeAggregation, builder, requestContext));

    // Get the time column name and add a time column Group By
    String timeColumn =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, requestContext, request.getContext());
    long periodSecs = getPeriodSecsFromTimeAggregations(request.getTimeAggregationList());
    builder.addGroupBy(createTimeColumnGroupByExpression(timeColumn, periodSecs));
  }

  private void addTimeAggregationToRequest(
      TimeAggregation timeAggregation,
      QueryRequest.Builder builder,
      ExploreRequestContext requestContext) {
    builder.addSelection(
        QueryAndGatewayDtoConverter.convertToQueryExpression(timeAggregation.getAggregation()));
    requestContext.mapAliasToTimeAggregation(
        timeAggregation
            .getAggregation()
            .getFunction()
            .getAlias(), // Required to be set by the validators
        timeAggregation);
  }

  /**
   * Call this if and only if the exploreRequest has timeAggregations.
   *
   * @param exploreRequest
   * @return
   */
  private ExploreRequest createPeriodBoundaryAlignedExploreRequest(ExploreRequest exploreRequest) {
    long periodSecs = getPeriodSecsFromTimeAggregations(exploreRequest.getTimeAggregationList());

    long alignedStartTime =
        QueryExpressionUtil.alignToPeriodBoundary(
            exploreRequest.getStartTimeMillis(), periodSecs, true);
    long alignedEndTime =
        QueryExpressionUtil.alignToPeriodBoundary(
            exploreRequest.getEndTimeMillis(), periodSecs, false);

    return ExploreRequest.newBuilder(exploreRequest)
        .setStartTimeMillis(alignedStartTime)
        .setEndTimeMillis(alignedEndTime)
        .build();
  }

  private long getPeriodSecsFromTimeAggregations(List<TimeAggregation> timeAggregations) {
    // Get period - all the time aggregations should have the same period.
    Period period = timeAggregations.stream().findFirst().orElseThrow().getPeriod();
    ChronoUnit unit = ChronoUnit.valueOf(period.getUnit());
    return Duration.of(period.getValue(), unit).getSeconds();
  }

  @Override
  protected void handleQueryServiceResponseSingleRow(
      Row row,
      ResultSetMetadata resultSetMetadata,
      ExploreResponse.Builder builder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    var rowBuilder = org.hypertrace.gateway.service.v1.common.Row.newBuilder();

    // First column is the time column. (Also the column name is "dateTimeConvert", Pinot's function
    // name for time conversion)
    // We will need to manually create a Long type value for it since it's a timestamp.
    org.hypertrace.gateway.service.v1.common.Value timeColumnValue =
        org.hypertrace.gateway.service.v1.common.Value.newBuilder()
            .setValueType(ValueType.LONG)
            .setLong(Long.parseLong(row.getColumn(0).getString()))
            .build();

    rowBuilder.putColumns(ColumnName.INTERVAL_START_TIME.name(), timeColumnValue);

    // Read the rest of the columns
    for (int i = 1; i < resultSetMetadata.getColumnMetadataCount(); i++) {
      handleQueryServiceResponseSingleColumn(
          row.getColumn(i),
          resultSetMetadata.getColumnMetadata(i),
          rowBuilder,
          requestContext,
          attributeMetadataProvider);
    }
    builder.addRow(rowBuilder);
  }

  @Override
  protected void handleQueryServiceResponseSingleColumn(
      Value queryServiceValue,
      ColumnMetadata metadata,
      org.hypertrace.gateway.service.v1.common.Row.Builder rowBuilder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    TimeAggregation timeAggregation =
        requestContext.getTimeAggregationByAlias(metadata.getColumnName());
    if (timeAggregation != null) { // Time aggregation with Function expression value
      handleQueryServiceResponseSingleColumn(
          queryServiceValue,
          metadata,
          rowBuilder,
          requestContext,
          attributeMetadataProvider,
          timeAggregation.getAggregation().getFunction());
    } else { // Simple columnId Expression value eg. groupBy columns or column selections
      handleQueryServiceResponseSingleColumn(
          queryServiceValue, metadata, rowBuilder, requestContext, attributeMetadataProvider, null);
    }
  }

  protected Logger getLogger() {
    return LOG;
  }
}
