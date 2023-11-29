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
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
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
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      QueryServiceEntityFetcher queryServiceEntityFetcher,
      EntityServiceEntityFetcher entityServiceEntityFetcher) {
    this.queryServiceClient = queryServiceClient;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.theRestGroupRequestHandler = new TheRestGroupRequestHandler(this);
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    this.queryServiceEntityFetcher = queryServiceEntityFetcher;
    this.entityServiceEntityFetcher = entityServiceEntityFetcher;
  }

  @Override
  public ExploreResponse.Builder handleRequest(
      ExploreRequestContext requestContext, ExploreRequest request) {
    QueryRequest queryRequest =
        buildQueryRequest(requestContext, request, attributeMetadataProvider);

    Iterator<ResultSetChunk> resultSetChunkIterator = executeQuery(requestContext, queryRequest);

    return handleQueryServiceResponse(
        request, requestContext, resultSetChunkIterator, requestContext, attributeMetadataProvider);
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

    Optional<List<String>> maybeEntityIds = getEntityIdsToFilter(requestContext, request);
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

    if (requestContext.hasGroupBy() && request.getIncludeRestGroup() && request.getOffset() > 0) {
      // including rest group with offset is an invalid combination
      // throwing unsupported operation exception for this case
      LOG.error(
          "Query having group by with both offset and include rest is an invalid combination : {}",
          request);
      throw new UnsupportedOperationException(
          "Query having group by with both offset and include rest is an invalid combination "
              + request);
    }

    // 3. Add GroupBy
    addGroupByExpressions(builder, request);

    // 4. Add order by along with setting limit, offset
    addSortLimitAndOffset(request, requestContext, builder);

    return builder.build();
  }

  private Optional<List<String>> getEntityIdsToFilter(
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

    Set<String> allEntityIds = this.getEntityIdsFromQueryService(context, exploreRequest);
    ExploreRequest edsExploreRequest =
        buildExploreRequest(context, exploreRequest.getContext(), maybeEdsFilter.orElseThrow());
    List<org.hypertrace.gateway.service.v1.common.Row> resultRows =
        this.entityServiceEntityFetcher.getResults(context, edsExploreRequest, allEntityIds);

    return Optional.of(
        resultRows.stream()
            .map(row -> row.getColumnsMap().values().stream().findFirst())
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(org.hypertrace.gateway.service.v1.common.Value::getString)
            .collect(Collectors.toUnmodifiableList()));
  }

  private ExploreRequest buildExploreRequest(
      ExploreRequestContext exploreRequestContext,
      String context,
      org.hypertrace.gateway.service.v1.common.Filter edsFilter) {
    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfigs, exploreRequestContext, context);
    List<Expression> groupBySelections =
        entityIdAttributeIds.stream()
            .map(attributeId -> QueryExpressionUtil.buildAttributeExpression(attributeId).build())
            .collect(Collectors.toUnmodifiableList());

    return ExploreRequest.newBuilder()
        .setContext(context)
        .setFilter(edsFilter)
        .addAllGroupBy(groupBySelections)
        .build();
  }

  protected Set<String> getEntityIdsFromQueryService(
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

  private void addSortLimitAndOffset(
      ExploreRequest request,
      ExploreRequestContext requestContext,
      QueryRequest.Builder queryBuilder) {
    if (request.getOrderByCount() > 0) {
      List<OrderByExpression> orderByExpressions = request.getOrderByList();
      queryBuilder.addAllOrderBy(
          QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(orderByExpressions));
    }

    // handle group by scenario with group limit set
    if (requestContext.hasGroupBy()) {
      int limit = request.getLimit();
      if (request.getGroupLimit() > 0) {
        // in group by scenario, set limit to minimum of limit or group-limit
        limit = Math.min(request.getLimit(), request.getGroupLimit());
      }
      // pinot doesn't handle offset with group by correctly
      // we will add offset to limit itself and then ignore results till offset in response
      limit += request.getOffset();
      // don't exceed default group by limit
      if (limit > DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT) {
        LOG.error(
            "Trying to query for rows more than the default limit {} : {}",
            DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT,
            request);
        throw new UnsupportedOperationException(
            "Trying to query for rows more than the default limit " + request);
      }
      queryBuilder.setLimit(limit);
    } else {
      queryBuilder.setLimit(request.getLimit());
      queryBuilder.setOffset(request.getOffset());
    }
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
      ExploreRequest request,
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

    // If request has group by, and includeRestGroup is set, and we have not reached limit
    // then invoke TheRestGroupRequestHandler
    if (requestContext.hasGroupBy()
        && requestContext.getIncludeRestGroup()
        && builder.getRowCount() < request.getLimit()) {
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
    if (offset > 0) {
      List<org.hypertrace.gateway.service.v1.common.Row.Builder> rowBuilders =
          builder.getRowBuilderList();
      List<org.hypertrace.gateway.service.v1.common.Row.Builder> rowBuildersPostSkip =
          rowBuilders.stream().skip(offset).collect(Collectors.toUnmodifiableList());
      builder.clearRow();
      rowBuildersPostSkip.forEach(builder::addRow);
    }
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
