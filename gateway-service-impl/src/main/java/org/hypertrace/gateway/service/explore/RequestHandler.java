package org.hypertrace.gateway.service.explore;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.util.ArithmeticValueUtil;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.DataCollectionUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.common.util.OrderByUtil;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandler implements RequestHandlerWithSorting {
  private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
  private final QueryServiceClient queryServiceClient;
  private final int requestTimeout;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final TheRestGroupRequestHandler theRestGroupRequestHandler;

  RequestHandler(
      QueryServiceClient queryServiceClient, int qsRequestTimeout,
      AttributeMetadataProvider attributeMetadataProvider) {
    this.queryServiceClient = queryServiceClient;
    this.requestTimeout = qsRequestTimeout;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.theRestGroupRequestHandler = new TheRestGroupRequestHandler(this);
  }

  @Override
  public ExploreResponse.Builder handleRequest(
      ExploreRequestContext requestContext, ExploreRequest request) {
    QueryRequest queryRequest =
        buildQueryRequest(requestContext, request, attributeMetadataProvider);

    Iterator<ResultSetChunk> resultSetChunkIterator = executeQuery(requestContext, queryRequest);

    return handleQueryServiceResponse(
        requestContext, resultSetChunkIterator, requestContext, attributeMetadataProvider);
  }

  QueryRequest buildQueryRequest(
      ExploreRequestContext requestContext,
      ExploreRequest request,
      AttributeMetadataProvider attributeMetadataProvider) {
    // Track if we have Group By so we can determine if we need to do Order By, Limit and Offset
    // ourselves.
    if (!request.getGroupByList().isEmpty()) {
      requestContext.setHasGroupBy(true);
    }

    QueryRequest.Builder builder = QueryRequest.newBuilder();

    // 1. Add selections. All selections should either be only column or only function, never both.
    // The validator should catch this.
    List<Expression> aggregatedSelections =
        ExpressionReader.getFunctionExpressions(request.getSelectionList().stream());
    aggregatedSelections.forEach(
        (aggregatedSelection) -> {
          requestContext.mapAliasToFunctionExpression(
              aggregatedSelection.getFunction().getAlias(), aggregatedSelection.getFunction());
          builder.addSelection(
              QueryAndGatewayDtoConverter.convertToQueryExpression(aggregatedSelection));
        });

    List<Expression> columnSelections =
        ExpressionReader.getColumnExpressions(request.getSelectionList().stream());
    columnSelections.forEach(
        (columnSelection) ->
            builder.addSelection(
                QueryAndGatewayDtoConverter.convertToQueryExpression(columnSelection)));

    // 2. Add filter
    builder.setFilter(
        constructQueryServiceFilter(request, requestContext, attributeMetadataProvider));

    // 3. Add GroupBy
    addGroupByExpressions(builder, request);

    // 4. If there's no Group By, Set Limit, Offset and Order By.
    // Otherwise, specify a large limit and track actual limit, offset and order by expression list
    // so we can compute
    // these once the we get the results.
    if (requestContext
        .hasGroupBy()) { // Will need to do the Ordering, Limit and Offset ourselves after we get
                         // the Group By Results
      builder.setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);
      requestContext.setOrderByExpressions(getRequestOrderByExpressions(request));
    } else { // No Group By: Use Pinot's Order By, Limit and Offset
      addSortLimitAndOffset(request, builder);
    }

    return builder.build();
  }

  private Iterator<ResultSetChunk> executeQuery(
      ExploreRequestContext context, QueryRequest queryRequest) {
    if (getLogger().isDebugEnabled()) {
      try {
        getLogger()
            .debug(
                "Sending Request to Query Service ======== \n {}",
                JsonFormat.printer().print(queryRequest));
      } catch (InvalidProtocolBufferException e) {
        getLogger()
            .error(
                String.format("Proto2Json Error logging QueryRequest: %s", queryRequest),
                e.getCause());
      }
    }

    return queryServiceClient.executeQuery(queryRequest, context.getHeaders(), requestTimeout);
  }

  Filter constructQueryServiceFilter(
      ExploreRequest request,
      ExploreRequestContext exploreRequestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    return QueryAndGatewayDtoConverter.addTimeAndSpaceFiltersAndConvertToQueryFilter(
        request.getStartTimeMillis(),
        request.getEndTimeMillis(),
        request.getSpaceId(),
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, exploreRequestContext, request.getContext()),
        AttributeMetadataUtil.getSpaceAttributeId(attributeMetadataProvider, exploreRequestContext, request.getContext()),
        request.getFilter());
  }

  void addGroupByExpressions(QueryRequest.Builder builder, ExploreRequest request) {
    request
        .getGroupByList()
        .forEach((expression -> addGroupByExpressionToBuilder(builder, expression)));
  }

  private void addSortLimitAndOffset(ExploreRequest request, QueryRequest.Builder queryBuilder) {
    if (request.getOrderByCount() > 0) {
      List<OrderByExpression> orderByExpressions = request.getOrderByList();
      queryBuilder.addAllOrderBy(
          QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(orderByExpressions));
    }

    queryBuilder.setLimit(request.getLimit());
    queryBuilder.setOffset(request.getOffset());
  }

  @Override
  public List<OrderByExpression> getRequestOrderByExpressions(ExploreRequest request) {
    return OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
        request.getOrderByList(), request.getSelectionList(), request.getTimeAggregationList());
  }

  private void addGroupByExpressionToBuilder(QueryRequest.Builder builder, Expression expression) {
    org.hypertrace.core.query.service.api.Expression.Builder expressionBuilder =
        org.hypertrace.core.query.service.api.Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder()
                    .setColumnName(expression.getColumnIdentifier().getColumnName())
                    .setAlias(expression.getColumnIdentifier().getAlias()));
    // Add groupBy expression to GroupBy list
    builder.addGroupBy(expressionBuilder);
    // Add groupBy to Selection list. The expectation from the Gateway service client is that they
    // do not add the
    // group by expressions to the selection expressions in the request
    builder.addSelection(expressionBuilder);
  }

  private ExploreResponse.Builder handleQueryServiceResponse(
      ExploreRequestContext context,
      Iterator<ResultSetChunk> resultSetChunkIterator,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    ExploreResponse.Builder builder = ExploreResponse.newBuilder();

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (getLogger().isDebugEnabled()) {
        getLogger().debug("Received chunk: " + chunk.toString());
      }

      if (chunk.getRowCount() < 1) {
        break;
      }

      if (!chunk.hasResultSetMetadata()) {
        getLogger().warn("Chunk doesn't have result metadata so couldn't process the response.");
        break;
      }

      chunk
          .getRowList()
          .forEach(
              (row) ->
                  handleQueryServiceResponseSingleRow(
                      row,
                      chunk.getResultSetMetadata(),
                      builder,
                      requestContext,
                      attributeMetadataProvider));
    }

    // If there's a Group By in the request, we need to do the sorting and pagination ourselves.
    if (requestContext.hasGroupBy()) {
      sortAndPaginatePostProcess(
          builder,
          requestContext.getOrderByExpressions(),
          requestContext.getLimit(),
          requestContext.getOffset());
    }

    if (requestContext.hasGroupBy() && requestContext.getIncludeRestGroup()) {
      theRestGroupRequestHandler.getRowsForTheRestGroup(
          context, requestContext.getExploreRequest(), builder);
    }

    return builder;
  }

  protected void handleQueryServiceResponseSingleRow(
      Row row,
      ResultSetMetadata resultSetMetadata,
      ExploreResponse.Builder builder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    var rowBuilder = org.hypertrace.gateway.service.v1.common.Row.newBuilder();
    for (int i = 0; i < resultSetMetadata.getColumnMetadataCount(); i++) {
      handleQueryServiceResponseSingleColumn(
          row.getColumn(i),
          resultSetMetadata.getColumnMetadata(i),
          rowBuilder,
          requestContext,
          attributeMetadataProvider);
    }
    builder.addRow(rowBuilder);
  }

  protected void handleQueryServiceResponseSingleColumn(
      Value queryServiceValue,
      ColumnMetadata metadata,
      org.hypertrace.gateway.service.v1.common.Row.Builder rowBuilder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    org.hypertrace.gateway.service.v1.common.FunctionExpression function =
        requestContext.getFunctionExpressionByAlias(metadata.getColumnName());
    handleQueryServiceResponseSingleColumn(
        queryServiceValue,
        metadata,
        rowBuilder,
        requestContext,
        attributeMetadataProvider,
        function);
  }

  void handleQueryServiceResponseSingleColumn(
      Value queryServiceValue,
      ColumnMetadata metadata,
      org.hypertrace.gateway.service.v1.common.Row.Builder rowBuilder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider,
      FunctionExpression function) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, requestContext.getContext());
    org.hypertrace.gateway.service.v1.common.Value gwValue;
    if (function != null) { // Function expression value
      gwValue =
          MetricAggregationFunctionUtil.getValueFromFunction(
              requestContext.getStartTimeMillis(),
              requestContext.getEndTimeMillis(),
              attributeMetadataMap,
              queryServiceValue,
              metadata,
              function);
    } else { // Simple columnId Expression value eg. groupBy columns or column selections
      gwValue = getValueForColumnIdExpression(queryServiceValue, metadata, attributeMetadataMap);
    }

    rowBuilder.putColumns(metadata.getColumnName(), gwValue);
  }


  private org.hypertrace.gateway.service.v1.common.Value getValueForColumnIdExpression(
      Value queryServiceValue,
      ColumnMetadata metadata,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    return QueryAndGatewayDtoConverter.convertToGatewayValue(
        metadata.getColumnName(), queryServiceValue, attributeMetadataMap);
  }

  @Override
  public void sortAndPaginatePostProcess(
      ExploreResponse.Builder builder,
      List<OrderByExpression> orderByExpressions,
      int limit,
      int offset) {
    List<org.hypertrace.gateway.service.v1.common.Row.Builder> rowBuilders =
        builder.getRowBuilderList();

    List<org.hypertrace.gateway.service.v1.common.Row.Builder> sortedRowBuilders =
        sortAndPaginateRowBuilders(rowBuilders, orderByExpressions, limit, offset);

    builder.clearRow();
    sortedRowBuilders.forEach(builder::addRow);
  }

  protected List<org.hypertrace.gateway.service.v1.common.Row.Builder> sortAndPaginateRowBuilders(
      List<org.hypertrace.gateway.service.v1.common.Row.Builder> rowBuilders,
      List<OrderByExpression> orderByExpressions,
      int limit,
      int offset) {
    RowComparator rowComparator = new RowComparator(orderByExpressions);

    return DataCollectionUtil.limitAndSort(
        rowBuilders.stream(), limit, offset, orderByExpressions.size(), rowComparator);
  }

  protected Logger getLogger() {
    return LOG;
  }

  TheRestGroupRequestHandler getTheRestGroupRequestHandler() {
    return this.theRestGroupRequestHandler;
  }
}
