package org.hypertrace.gateway.service.explore;

import static org.hypertrace.core.query.service.client.QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT;

import com.google.common.collect.Streams;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Status;
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
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfig;
import org.hypertrace.gateway.service.explore.entity.EntityServiceEntityFetcher;
import org.hypertrace.gateway.service.v1.common.AttributeExpression;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link RequestHandler} is currently used only when the selections, group bys and filters are on
 * QS. Multiple sources are supported but when filters are on EDS only source.
 *
 * <p>If there are filters on attributes from EDS source only, a. handler will first query QS source
 * with time range filter to get all the possible Ids b. Then it will filter the ids from step (a),
 * with EDS filters c. And finally, with entity ids queries from step (b) we will query the
 * selections from QS. In this approach we are making three network calls but handling less data in
 * memory.
 *
 * <p>Other approach could be to first query all the data from QS source (limit of 10000) along with
 * selections and then filter out data from EDS filters. In this way we will only send two queries
 * but need to handle lots of data in memory.
 * </ul>
 */
public class RequestHandler implements RequestHandlerWithSorting {
  private static final Logger LOG = LoggerFactory.getLogger(RequestHandler.class);
  private static final Expression NULL_VALUE_EXPRESSION =
      Expression.newBuilder()
          .setLiteral(
              LiteralConstant.newBuilder()
                  .setValue(
                      org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                          .setValueType(ValueType.STRING)
                          .setString("null")
                          .build())
                  .build())
          .build();

  private final QueryServiceClient queryServiceClient;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final TheRestGroupRequestHandler theRestGroupRequestHandler;
  private final EntityIdColumnsConfig entityIdColumnsConfig;
  private final QueryServiceEntityFetcher queryServiceEntityFetcher;
  private final EntityServiceEntityFetcher entityServiceEntityFetcher;

  public RequestHandler(
      QueryServiceClient queryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfig entityIdColumnsConfig,
      QueryServiceEntityFetcher queryServiceEntityFetcher,
      EntityServiceEntityFetcher entityServiceEntityFetcher) {
    this.queryServiceClient = queryServiceClient;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.theRestGroupRequestHandler = new TheRestGroupRequestHandler(this);
    this.entityIdColumnsConfig = entityIdColumnsConfig;
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

    org.hypertrace.gateway.service.v1.common.Filter qsSourceFilter = request.getFilter();
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(requestContext, request.getContext());
    if (hasOnlyAttributeSource(request.getFilter(), AttributeSource.EDS, attributeMetadataMap)) {
      List<String> entityIds =
          getEntityIdsToFilterFromSourceEDS(requestContext, request, attributeMetadataMap);
      List<String> entityIdAttributes =
          AttributeMetadataUtil.getIdAttributeIds(
              attributeMetadataProvider,
              entityIdColumnsConfig,
              requestContext,
              request.getContext());
      qsSourceFilter =
          org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
              .setOperator(Operator.AND)
              .addChildFilter(
                  buildFilter(request.getFilter(), AttributeSource.QS, attributeMetadataMap)
                      .orElse(request.getFilter()))
              .addChildFilter(
                  createEntityIdAttributeFilter(
                      entityIdAttributes,
                      org.hypertrace.gateway.service.v1.common.Operator.IN,
                      entityIds))
              .build();
    }

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
            request, qsSourceFilter, requestContext, attributeMetadataProvider));

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

  // This is to get all the entity Ids for the EDS source filter.
  // 1. First filter out entity ids based on the time range from QS filter
  // 2. Then filter out entity ids return in 1 based on EDS filter.
  private List<String> getEntityIdsToFilterFromSourceEDS(
      ExploreRequestContext context,
      ExploreRequest exploreRequest,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    LOG.debug("Querying entity ids from EDS source {}", exploreRequest);
    // Check if there is any filter present with EDS only source. If not then return,
    // else query the respective entityIds from the EDS source.
    Optional<org.hypertrace.gateway.service.v1.common.Filter> maybeEdsFilter =
        buildFilter(exploreRequest.getFilter(), AttributeSource.EDS, attributeMetadataMap);
    if (maybeEdsFilter.isEmpty()) {
      return Collections.emptyList();
    }

    Set<String> allEntityIds =
        this.getEntityIdsInTimeRangeFromQueryService(context, exploreRequest);

    ExploreRequest edsExploreRequest =
        buildExploreRequest(context, exploreRequest.getContext(), maybeEdsFilter.orElseThrow());
    ExploreRequestContext edsRequestExploreContext =
        new ExploreRequestContext(context.getGrpcContext(), edsExploreRequest);
    List<org.hypertrace.gateway.service.v1.common.Row> resultRows =
        this.entityServiceEntityFetcher.getResults(
            edsRequestExploreContext, edsExploreRequest, allEntityIds);
    return resultRows.stream()
        .map(row -> row.getColumnsMap().values().stream().findFirst())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(org.hypertrace.gateway.service.v1.common.Value::getString)
        .collect(Collectors.toUnmodifiableList());
  }

  private ExploreRequest buildExploreRequest(
      ExploreRequestContext exploreRequestContext,
      String context,
      org.hypertrace.gateway.service.v1.common.Filter edsFilter) {
    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfig, exploreRequestContext, context);
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

  protected Set<String> getEntityIdsInTimeRangeFromQueryService(
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
        request, request.getFilter(), exploreRequestContext, attributeMetadataProvider);
  }

  Filter constructQueryServiceFilter(
      ExploreRequest request,
      org.hypertrace.gateway.service.v1.common.Filter requestFilter,
      ExploreRequestContext exploreRequestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    return QueryAndGatewayDtoConverter.addTimeAndSpaceFiltersAndConvertToQueryFilter(
        request.getStartTimeMillis(),
        request.getEndTimeMillis(),
        request.getSpaceId(),
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, exploreRequestContext, request.getContext()),
        AttributeMetadataUtil.getSpaceAttributeId(
            attributeMetadataProvider, exploreRequestContext, request.getContext()),
        requestFilter);
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

  private org.hypertrace.gateway.service.v1.common.Filter createEntityIdAttributeFilter(
      List<String> entityIdAttributes,
      org.hypertrace.gateway.service.v1.common.Operator operator,
      List<String> entityIds) {
    if (entityIdAttributes.size() != 1) {
      throw Status.FAILED_PRECONDITION
          .withDescription("entity must have one id attribute.")
          .asRuntimeException();
    }

    if (entityIds.isEmpty()) {
      // TODO: have a better approach here. One possible solution could be to
      //  form a dummy response based on the selections provided

      // Having empty entity ids is valid filter because this means that
      // EDS source has filtered out all the entities and the result should
      // be empty. But QS doesn't recognize empty IN filter as valid filter
      // and ignoring this filter will generate wrong results.
      //
      // We could have return empty but clients expects response to contain
      // information as per the selections they have sent. So we are converting
      // empty entity Id filter into a false filter (id != null && id == null)
      // so that results are always empty but must contain the valid selections.
      return org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
          .setOperator(Operator.AND)
          .addChildFilter(
              org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
                  .setLhs(buildAttributeExpression(entityIdAttributes.get(0)))
                  .setOperator(Operator.EQ)
                  .setRhs(NULL_VALUE_EXPRESSION)
                  .build())
          .addChildFilter(
              org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
                  .setLhs(buildAttributeExpression(entityIdAttributes.get(0)))
                  .setOperator(Operator.NEQ)
                  .setRhs(NULL_VALUE_EXPRESSION)
                  .build())
          .build();
    }

    return org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
        .setLhs(buildAttributeExpression(entityIdAttributes.get(0)))
        .setOperator(operator)
        .setRhs(buildStringArrayExpression(entityIds))
        .build();
  }

  private Expression buildStringArrayExpression(List<String> values) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(
                    org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                        .setValueType(ValueType.STRING_ARRAY)
                        .addAllStringArray(values)
                        .build())
                .build())
        .build();
  }

  private Expression buildAttributeExpression(String value) {
    return Expression.newBuilder()
        .setAttributeExpression(AttributeExpression.newBuilder().setAttributeId(value).build())
        .build();
  }

  private boolean hasOnlyAttributeSource(
      org.hypertrace.gateway.service.v1.common.Filter filter,
      AttributeSource source,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    if (filter.equals(org.hypertrace.gateway.service.v1.common.Filter.getDefaultInstance())) {
      return false;
    }

    org.hypertrace.gateway.service.v1.common.Operator operator = filter.getOperator();
    switch (operator) {
      case UNDEFINED:
        return false;
      case AND:
      case OR:
        for (org.hypertrace.gateway.service.v1.common.Filter childFilter :
            filter.getChildFilterList()) {
          if (hasOnlyAttributeSource(childFilter, source, attributeMetadataMap)) {
            return true;
          }
        }
        return false;
      default:
        List<AttributeSource> availableSources =
            attributeMetadataMap
                .get(
                    ExpressionReader.getAttributeIdFromAttributeSelection(filter.getLhs())
                        .orElseThrow())
                .getSourcesList();
        return availableSources.size() == 1 && availableSources.contains(source);
    }
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
        return availableSources.contains(source)
            ? Optional.of(
                org.hypertrace.gateway.service.v1.common.Filter.newBuilder(filter).build())
            : Optional.empty();
    }
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
