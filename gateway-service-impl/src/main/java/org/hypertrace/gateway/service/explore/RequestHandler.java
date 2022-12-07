package org.hypertrace.gateway.service.explore;

import static org.hypertrace.core.query.service.client.QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT;

import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.DataCollectionUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.common.util.OrderByUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.explore.entity.EntityServiceEntityFetcher;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequestHandler implements RequestHandlerWithSorting {
  private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
  private final QueryServiceClient queryServiceClient;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final TheRestGroupRequestHandler theRestGroupRequestHandler;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;
  private final QueryServiceEntityFetcher queryServiceEntityFetcher;
  private final EntityServiceEntityFetcher entityServiceEntityFetcher;

  public RequestHandler(
      QueryServiceClient queryServiceClient,
      EntityQueryServiceClient entityQueryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    this.queryServiceClient = queryServiceClient;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.theRestGroupRequestHandler = new TheRestGroupRequestHandler(this);
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    this.queryServiceEntityFetcher =
        new QueryServiceEntityFetcher(
            queryServiceClient, attributeMetadataProvider, entityIdColumnsConfigs);
    this.entityServiceEntityFetcher =
        new EntityServiceEntityFetcher(
            attributeMetadataProvider, entityIdColumnsConfigs, entityQueryServiceClient);
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

    Optional<List<String>> maybeEntityIds = getEntityIdsIfNecessary(requestContext, request);
    QueryRequest.Builder builder = QueryRequest.newBuilder();

    // 1. Add selections. All selections should either be only column or only function, never both.
    // The validator should catch this.
    List<Expression> aggregatedSelections =
        ExpressionReader.getFunctionExpressions(request.getSelectionList());
    aggregatedSelections.forEach(
        aggregatedSelection -> {
          requestContext.mapAliasToFunctionExpression(
              aggregatedSelection.getFunction().getAlias(), aggregatedSelection.getFunction());
          builder.addSelection(
              QueryAndGatewayDtoConverter.convertToQueryExpression(aggregatedSelection));
        });

    List<Expression> columnSelections =
        ExpressionReader.getAttributeExpressions(request.getSelectionList());
    columnSelections.forEach(
        columnSelection ->
            builder.addSelection(
                QueryAndGatewayDtoConverter.convertToQueryExpression(columnSelection)));

    // 2. Add filter
    builder.setFilter(
        constructQueryServiceFilter(
            request,
            requestContext,
            attributeMetadataProvider,
            maybeEntityIds.orElse(Collections.emptyList())));

    // 3. Add GroupBy
    addGroupByExpressions(builder, request);

    // 4. If there's no Group By, Set Limit, Offset and Order By.
    // Otherwise, specify a large limit and track actual limit, offset and order by expression list
    // so we can compute
    // these once the we get the results.
    if (requestContext
        .hasGroupBy()) { // Will need to do the Ordering, Limit and Offset ourselves after we get
      // the Group By Results
      builder.setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);
      requestContext.setOrderByExpressions(getRequestOrderByExpressions(request));
    } else { // No Group By: Use Pinot's Order By, Limit and Offset
      addSortLimitAndOffset(request, builder);
    }

    return builder.build();
  }

  private Optional<List<String>> getEntityIdsIfNecessary(
      ExploreRequestContext context, ExploreRequest exploreRequest) {

    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(context, exploreRequest.getContext());
    // Check if there is any filter present with EDS only source. If not then return,
    // else query the respective entityIds from the EDS source.
    Optional<org.hypertrace.gateway.service.v1.common.Filter> maybeEdsFilter =
        buildFilter(exploreRequest.getFilter(), AttributeSource.EDS, attributeMetadataMap);
    if (maybeEdsFilter.isEmpty()) {
      return Optional.empty();
    }

    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            context,
            exploreRequest.getContext());

    Set<String> allEntityIds = this.getEntityIdsFromQueryService(context, exploreRequest);
    List<Expression> groupBySelections =
        entityIdAttributeIds.stream()
            .map(attributeId -> QueryExpressionUtil.buildAttributeExpression(attributeId).build())
            .collect(Collectors.toUnmodifiableList());

    ExploreRequest.Builder exploreRequestBuilder =
        ExploreRequest.newBuilder()
            .setContext(exploreRequest.getContext())
            .addAllGroupBy(groupBySelections);
    maybeEdsFilter.ifPresent(exploreRequestBuilder::setFilter);

    Iterator<org.hypertrace.entity.query.service.v1.ResultSetChunk> resultSetChunkIterator =
        this.entityServiceEntityFetcher.getResults(
            context, exploreRequestBuilder.build(), allEntityIds);
    ExploreResponse.Builder responseBuilder = ExploreResponse.newBuilder();
    readEntityServiceChunkResults(context, responseBuilder, resultSetChunkIterator);

    return Optional.of(
        responseBuilder.getRowList().stream()
            .map(row -> row.getColumnsMap().values().stream().findFirst())
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(org.hypertrace.gateway.service.v1.common.Value::getString)
            .collect(Collectors.toUnmodifiableList()));
  }

  void readEntityServiceChunkResults(
      ExploreRequestContext requestContext,
      ExploreResponse.Builder builder,
      Iterator<org.hypertrace.entity.query.service.v1.ResultSetChunk> resultSetChunkIterator) {
    while (resultSetChunkIterator.hasNext()) {
      org.hypertrace.entity.query.service.v1.ResultSetChunk chunk = resultSetChunkIterator.next();
      getLogger().debug("Received chunk: {}", chunk);

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
              row ->
                  handleEntityServiceResultRow(
                      row,
                      chunk.getResultSetMetadata(),
                      builder,
                      requestContext,
                      attributeMetadataProvider));
    }
  }

  private void handleEntityServiceResultRow(
      org.hypertrace.entity.query.service.v1.Row row,
      org.hypertrace.entity.query.service.v1.ResultSetMetadata resultSetMetadata,
      ExploreResponse.Builder builder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    var rowBuilder = org.hypertrace.gateway.service.v1.common.Row.newBuilder();
    for (int i = 0; i < resultSetMetadata.getColumnMetadataCount(); i++) {
      org.hypertrace.entity.query.service.v1.ColumnMetadata metadata =
          resultSetMetadata.getColumnMetadata(i);
      FunctionExpression function =
          requestContext.getFunctionExpressionByAlias(metadata.getColumnName());
      handleEntityServiceResultColumn(
          row.getColumn(i),
          metadata,
          rowBuilder,
          requestContext,
          attributeMetadataProvider,
          function);
    }
    builder.addRow(rowBuilder);
  }

  private void handleEntityServiceResultColumn(
      org.hypertrace.entity.query.service.v1.Value value,
      org.hypertrace.entity.query.service.v1.ColumnMetadata metadata,
      org.hypertrace.gateway.service.v1.common.Row.Builder rowBuilder,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider,
      FunctionExpression function) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, requestContext.getContext());
    Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap =
        this.remapAttributeMetadataByResultNameForEntity(
            requestContext.getExploreRequest(), attributeMetadataMap);
    org.hypertrace.gateway.service.v1.common.Value gwValue;
    if (function != null) {
      // Function expression value
      gwValue =
          EntityServiceAndGatewayServiceConverter.convertToGatewayValueForMetricValue(
              MetricAggregationFunctionUtil.getValueTypeForFunctionType(
                  function, attributeMetadataMap),
              resultKeyToAttributeMetadataMap,
              metadata,
              value);
    } else {
      // Simple columnId expression value eg. groupBy columns or column selections
      gwValue =
          EntityServiceAndGatewayServiceConverter.convertToGatewayValue(
              metadata.getColumnName(), value, resultKeyToAttributeMetadataMap);
    }

    rowBuilder.putColumns(metadata.getColumnName(), gwValue);
  }

  private Map<String, AttributeMetadata> remapAttributeMetadataByResultNameForEntity(
      ExploreRequest request, Map<String, AttributeMetadata> attributeMetadataByIdMap) {
    return AttributeMetadataUtil.remapAttributeMetadataByResultKey(
        Streams.concat(
                request.getSelectionList().stream(),
                // Add groupBy to Selection list.
                // The expectation from the Gateway service client is that they do not add the group
                // by expressions to the selection expressions in the request
                request.getGroupByList().stream())
            .collect(Collectors.toUnmodifiableList()),
        attributeMetadataByIdMap);
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

    return queryServiceClient.executeQuery(context, queryRequest);
  }

  Filter constructQueryServiceFilter(
      ExploreRequest request,
      ExploreRequestContext exploreRequestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    return this.constructQueryServiceFilter(
        request, exploreRequestContext, attributeMetadataProvider, Collections.emptyList());
  }

  Filter constructQueryServiceFilter(
      ExploreRequest request,
      ExploreRequestContext exploreRequestContext,
      AttributeMetadataProvider attributeMetadataProvider,
      List<String> entityIds) {
    return QueryAndGatewayDtoConverter.addTimeSpaceAndIdFiltersAndConvertToQueryFilter(
        request.getStartTimeMillis(),
        request.getEndTimeMillis(),
        request.getSpaceId(),
        entityIds,
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, exploreRequestContext, request.getContext()),
        AttributeMetadataUtil.getSpaceAttributeId(
            attributeMetadataProvider, exploreRequestContext, request.getContext()),
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            exploreRequestContext,
            request.getContext()),
        request.getFilter());
  }

  void addGroupByExpressions(QueryRequest.Builder builder, ExploreRequest request) {
    request
        .getGroupByList()
        .forEach(expression -> addGroupByExpressionToBuilder(builder, expression));
  }

  public Set<String> getEntityIdsFromQueryService(
      ExploreRequestContext requestContext, ExploreRequest exploreRequest) {
    EntitiesRequestContext entitiesRequestContext =
        convert(attributeMetadataProvider, requestContext);
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, exploreRequest.getContext());

    EntitiesRequest.Builder entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(exploreRequest.getContext())
            .setStartTimeMillis(exploreRequest.getStartTimeMillis())
            .setEndTimeMillis(exploreRequest.getEndTimeMillis());

    Optional<org.hypertrace.gateway.service.v1.common.Filter> maybeQsFilters =
        buildFilter(exploreRequest.getFilter(), AttributeSource.QS, attributeMetadataMap);
    maybeQsFilters.ifPresent(entitiesRequest::setFilter);

    EntityFetcherResponse response =
        queryServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest.build());
    return response.getEntityKeyBuilderMap().values().stream()
        .map(Builder::getId)
        .collect(Collectors.toUnmodifiableSet());
  }

  private EntitiesRequestContext convert(
      AttributeMetadataProvider attributeMetadataProvider, ExploreRequestContext requestContext) {
    String entityType = requestContext.getContext();

    String timestampAttributeId =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, requestContext, entityType);
    return new EntitiesRequestContext(
        requestContext.getGrpcContext(),
        requestContext.getStartTimeMillis(),
        requestContext.getEndTimeMillis(),
        entityType,
        timestampAttributeId);
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
    // Add groupBy expression to GroupBy list
    builder.addGroupBy(QueryAndGatewayDtoConverter.convertToQueryExpression(expression));
    // Add groupBy to Selection list. The expectation from the Gateway service client is that they
    // do not add the
    // group by expressions to the selection expressions in the request
    builder.addSelection(QueryAndGatewayDtoConverter.convertToQueryExpression(expression));
  }

  private ExploreResponse.Builder handleQueryServiceResponse(
      ExploreRequestContext context,
      Iterator<ResultSetChunk> resultSetChunkIterator,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    ExploreResponse.Builder builder = ExploreResponse.newBuilder();

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      getLogger().debug("Received chunk: {}", chunk);
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
              row ->
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
          requestContext.getRowLimitBeforeRest(),
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
    FunctionExpression function =
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
    Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap =
        this.remapAttributeMetadataByResultName(
            requestContext.getExploreRequest(), attributeMetadataMap);
    org.hypertrace.gateway.service.v1.common.Value gwValue;
    if (function != null) { // Function expression value
      gwValue =
          QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
              MetricAggregationFunctionUtil.getValueTypeForFunctionType(
                  function, attributeMetadataMap),
              resultKeyToAttributeMetadataMap,
              metadata,
              queryServiceValue);
    } else { // Simple columnId Expression value eg. groupBy columns or column selections
      gwValue =
          getValueForColumnIdExpression(
              queryServiceValue, metadata, resultKeyToAttributeMetadataMap);
    }

    rowBuilder.putColumns(metadata.getColumnName(), gwValue);
  }

  private org.hypertrace.gateway.service.v1.common.Value getValueForColumnIdExpression(
      Value queryServiceValue,
      ColumnMetadata metadata,
      Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap) {
    return QueryAndGatewayDtoConverter.convertToGatewayValue(
        metadata.getColumnName(), queryServiceValue, resultKeyToAttributeMetadataMap);
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

  protected TheRestGroupRequestHandler getTheRestGroupRequestHandler() {
    return this.theRestGroupRequestHandler;
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

  private Optional<org.hypertrace.gateway.service.v1.common.Filter> buildFilter(
      org.hypertrace.gateway.service.v1.common.Filter filter,
      AttributeSource source,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    if (filter.equals(org.hypertrace.gateway.service.v1.common.Filter.getDefaultInstance())) {
      return Optional.empty();
    }

    org.hypertrace.gateway.service.v1.common.Operator operator = filter.getOperator();
    switch (operator) {
      case UNDEFINED:
        return Optional.empty();
      case AND:
      case OR:
        return buildCompositeFilter(filter, source, operator, attributeMetadataMap);
      default:
        List<AttributeSource> availableSources =
            attributeMetadataMap
                .get(
                    ExpressionReader.getAttributeIdFromAttributeSelection(filter.getLhs())
                        .orElseThrow())
                .getSourcesList();
        return isValidSource(availableSources, source)
            ? Optional.of(
                org.hypertrace.gateway.service.v1.common.Filter.newBuilder(filter).build())
            : Optional.empty();
    }
  }

  private boolean isValidSource(List<AttributeSource> availableSources, AttributeSource source) {
    if (AttributeSource.EDS.equals(source)) {
      return availableSources.size() == 1 && availableSources.contains(source);
    }

    return availableSources.contains(source);
  }

  private Optional<org.hypertrace.gateway.service.v1.common.Filter> buildCompositeFilter(
      org.hypertrace.gateway.service.v1.common.Filter filter,
      AttributeSource source,
      org.hypertrace.gateway.service.v1.common.Operator operator,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    org.hypertrace.gateway.service.v1.common.Filter.Builder filterBuilder =
        org.hypertrace.gateway.service.v1.common.Filter.newBuilder();
    for (org.hypertrace.gateway.service.v1.common.Filter childFilter :
        filter.getChildFilterList()) {
      buildFilter(childFilter, source, attributeMetadataMap)
          .ifPresent(filterBuilder::addChildFilter);
    }
    if (filterBuilder.getChildFilterCount() > 0) {
      filterBuilder.setOperator(operator);
      return Optional.of(filterBuilder.build());
    } else {
      return Optional.empty();
    }
  }
}
