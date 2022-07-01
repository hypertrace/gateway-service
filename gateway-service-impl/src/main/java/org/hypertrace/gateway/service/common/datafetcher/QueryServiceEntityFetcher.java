package org.hypertrace.gateway.service.common.datafetcher;

import static java.util.Objects.isNull;
import static org.hypertrace.core.query.service.client.QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT;
import static org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter.convertToQueryExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createCountByColumnSelection;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createDistinctCountByColumnSelection;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringNullLiteralExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createTimeColumnGroupByExpression;
import static org.hypertrace.gateway.service.common.util.ExpressionReader.getExpectedResultNamesForEachAttributeId;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.QueryRequestContext;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.converters.QueryRequestUtil;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntitiesRequestValidator;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Health;
import org.hypertrace.gateway.service.v1.common.Interval;
import org.hypertrace.gateway.service.v1.common.MetricSeries;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the {@link IEntityFetcher} using the QueryService as the data source */
public class QueryServiceEntityFetcher implements IEntityFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(QueryServiceEntityFetcher.class);
  private static final String COUNT_COLUMN_NAME = "COUNT";

  private final EntitiesRequestValidator entitiesRequestValidator = new EntitiesRequestValidator();
  private final QueryServiceClient queryServiceClient;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;

  public QueryServiceEntityFetcher(
      QueryServiceClient queryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    this.queryServiceClient = queryServiceClient;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
  }

  @Override
  public EntityFetcherResponse getEntities(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());
    Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap =
        this.remapAttributeMetadataByResultName(entitiesRequest, attributeMetadataMap);
    // Validate EntitiesRequest
    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);

    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            requestContext,
            entitiesRequest.getEntityType());
    List<org.hypertrace.gateway.service.v1.common.Expression> aggregates =
        ExpressionReader.getFunctionExpressions(entitiesRequest.getSelectionList());

    Map<String, List<String>> requestedAliasesByEntityIdAttributeIds =
        getExpectedResultNamesForEachAttributeId(
            entitiesRequest.getSelectionList(), entityIdAttributeIds);

    QueryRequest.Builder builder =
        constructSelectionQuery(requestContext, entitiesRequest, entityIdAttributeIds, aggregates);

    adjustLimitAndOffset(builder, entitiesRequest.getLimit(), entitiesRequest.getOffset());

    if (!entitiesRequest.getOrderByList().isEmpty()) {
      // Order by from the request.
      builder.addAllOrderBy(
          QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(
              entitiesRequest.getOrderByList()));
    }

    QueryRequest queryRequest = builder.build();

    LOG.debug("Sending Query to Query Service ======== \n {}", queryRequest);

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(requestContext, queryRequest);

    // We want to retain the order as returned from the respective source. Hence using a
    // LinkedHashMap
    Map<EntityKey, Entity.Builder> entityBuilders = new LinkedHashMap<>();
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      LOG.debug("Received chunk: {}", chunk);

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        // Construct the entity id from the entityIdAttributeIds columns
        EntityKey entityKey =
            EntityKey.of(
                IntStream.range(0, entityIdAttributeIds.size())
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));
        Builder entityBuilder = entityBuilders.computeIfAbsent(entityKey, k -> Entity.newBuilder());
        entityBuilder.setEntityType(entitiesRequest.getEntityType());
        entityBuilder.setId(entityKey.toString());
        // Always include the id in entity since that's needed to make follow up queries in
        // optimal fashion. If this wasn't really requested by the client, it should be removed
        // as post processing.
        for (int i = 0; i < entityIdAttributeIds.size(); i++) {
          entityBuilder.putAttribute(
              entityIdAttributeIds.get(i),
              Value.newBuilder()
                  .setString(entityKey.getAttributes().get(i))
                  .setValueType(ValueType.STRING)
                  .build());
        }

        requestedAliasesByEntityIdAttributeIds.forEach(
            (attributeId, requestedAliasList) ->
                requestedAliasList.forEach(
                    requestedAlias ->
                        entityBuilder.putAttribute(
                            requestedAlias, entityBuilder.getAttributeOrThrow(attributeId))));

        for (int i = entityIdAttributeIds.size();
            i < chunk.getResultSetMetadata().getColumnMetadataCount();
            i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
          org.hypertrace.core.query.service.api.Value columnValue = row.getColumn(i);
          buildEntity(
              entityBuilder,
              requestContext,
              metadata,
              columnValue,
              resultKeyToAttributeMetadataMap,
              aggregates.isEmpty());
        }
      }
    }
    return new EntityFetcherResponse(entityBuilders);
  }

  private void adjustLimitAndOffset(QueryRequest.Builder builder, int limit, int offset) {
    // If there is more than one groupBy column, we cannot set the same limit that came
    // in the request since that might return less entities than needed when the same
    // entity has different values for the other group by columns. Example: A service entity's
    // name changes and that will now have two different names.
    // For now, we pass a high value of limit in this case so that we get all the entities.
    // Limit has to be applied post the query in this case. Setting offset also might be wrong
    // here, hence not setting it.

    boolean canApplyLimit = limit > 0;
    boolean canApplyOffset = offset > 0;

    // If we cannot apply limit, limit the number of results to a default limit
    if (!canApplyLimit || builder.getGroupByCount() > 1) {
      builder.setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);
    } else {
      builder.setLimit(limit);
    }

    if (canApplyOffset) {
      builder.setOffset(offset);
    }
  }

  private QueryRequest.Builder constructSelectionQuery(
      EntitiesRequestContext requestContext,
      EntitiesRequest entitiesRequest,
      List<String> entityIdAttributeIds,
      List<org.hypertrace.gateway.service.v1.common.Expression> aggregates) {
    List<Expression> idExpressions =
        entityIdAttributeIds.stream()
            .map(QueryRequestUtil::createAttributeExpression)
            .collect(Collectors.toList());
    Filter.Builder filterBuilder =
        constructQueryServiceFilter(entitiesRequest, requestContext, entityIdAttributeIds);

    QueryRequest.Builder builder =
        QueryRequest.newBuilder()
            .setFilter(filterBuilder)
            // Add EntityID attributes as the first selection and group by
            .addAllSelection(idExpressions)
            .addAllGroupBy(idExpressions);

    /* if there's aggregates, then add update request context alias <-> function expression map*/
    for (org.hypertrace.gateway.service.v1.common.Expression aggregate : aggregates) {
      requestContext.mapAliasToFunctionExpression(
          aggregate.getFunction().getAlias(), aggregate.getFunction());
      builder.addSelection(QueryAndGatewayDtoConverter.convertToQueryExpression(aggregate));
    }

    // Add all expressions in the select/group that are already not part of the EntityID attributes
    // We do this mainly because we're reading the other non-id attributes of entities also
    // from OLAP store but ideally they should be coming from entity service.
    // TODO: Query non identifying attributes from entity service in parallel to this query
    //  and remove this logic.
    entitiesRequest.getSelectionList().stream()
        .filter(ExpressionReader::isAttributeSelection)
        .filter(
            expression ->
                ExpressionReader.getAttributeIdFromAttributeSelection(expression)
                    .map(attributeId -> !entityIdAttributeIds.contains(attributeId))
                    .orElse(true))
        .forEach(
            expression -> {
              Expression.Builder expBuilder = convertToQueryExpression(expression);
              builder.addSelection(expBuilder);
              builder.addGroupBy(expBuilder);
            });

    // Pinot's GroupBy queries need at least one aggregate operation in the selection
    // so we add count(*) as a dummy placeholder.
    if (aggregates.isEmpty()) {
      builder.addSelection(
          createCountByColumnSelection(
              Optional.ofNullable(entityIdAttributeIds.get(0)).orElseThrow()));
    }
    return builder;
  }

  private void buildEntity(
      Entity.Builder entityBuilder,
      QueryRequestContext requestContext,
      ColumnMetadata metadata,
      org.hypertrace.core.query.service.api.Value columnValue,
      Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap,
      boolean isSkipCountColumn) {

    // Ignore the count column since we introduced that ourselves into the query
    if (isSkipCountColumn
        && StringUtils.equalsIgnoreCase(COUNT_COLUMN_NAME, metadata.getColumnName())) {
      return;
    }

    // aggregate
    if (requestContext.containsFunctionExpression(metadata.getColumnName())) {
      addAggregateMetric(
          entityBuilder, requestContext, metadata, columnValue, resultKeyToAttributeMetadataMap);
    } else {
      // attribute
      addEntityAttribute(entityBuilder, metadata, columnValue, resultKeyToAttributeMetadataMap);
    }
  }

  private void addEntityAttribute(
      Entity.Builder entityBuilder,
      ColumnMetadata metadata,
      org.hypertrace.core.query.service.api.Value columnValue,
      Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap) {

    String resultKey = metadata.getColumnName();
    if (!resultKeyToAttributeMetadataMap.containsKey(resultKey)) {
      LOG.warn("Missing attribute metadata for key {}", resultKey);
    }

    entityBuilder.putAttribute(
        resultKey,
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(
            columnValue, resultKeyToAttributeMetadataMap.get(resultKey)));
  }

  private void addAggregateMetric(
      Entity.Builder entityBuilder,
      QueryRequestContext requestContext,
      ColumnMetadata metadata,
      org.hypertrace.core.query.service.api.Value columnValue,
      Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap) {

    FunctionExpression functionExpression =
        requestContext.getFunctionExpressionByAlias(metadata.getColumnName());
    AttributeMetadata functionAttributeMetadata =
        resultKeyToAttributeMetadataMap.get(metadata.getColumnName());

    if (isNull(functionAttributeMetadata)) {
      LOG.warn("Missing attribute metadata for {}", metadata.getColumnName());
    }
    List<org.hypertrace.gateway.service.v1.common.Expression> healthExpressions =
        functionExpression.getArgumentsList().stream()
            .filter(org.hypertrace.gateway.service.v1.common.Expression::hasHealth)
            .collect(Collectors.toList());
    Preconditions.checkArgument(healthExpressions.size() <= 1);
    Health health = Health.NOT_COMPUTED;

    Value convertedValue =
        QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
            MetricAggregationFunctionUtil.getValueTypeForFunctionType(
                functionExpression.getFunction(), functionAttributeMetadata),
            resultKeyToAttributeMetadataMap,
            metadata,
            columnValue);

    entityBuilder.putMetric(
        metadata.getColumnName(),
        AggregatedMetricValue.newBuilder()
            .setValue(convertedValue)
            .setFunction(functionExpression.getFunction())
            .setHealth(health)
            .build());
  }

  @Override
  public EntityFetcherResponse getTimeAggregatedMetrics(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    // No need to make execute the rest of this if there are no TimeAggregations in the request.
    if (entitiesRequest.getTimeAggregationCount() == 0) {
      return new EntityFetcherResponse();
    }
    // Only supported filter is entityIds IN ["id1", "id2", "id3"]
    List<String> idColumns =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            requestContext,
            entitiesRequest.getEntityType());
    String timeColumn =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, requestContext, entitiesRequest.getEntityType());
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());

    Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap =
        this.remapAttributeMetadataByResultName(entitiesRequest, attributeMetadataMap);

    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);

    entitiesRequest
        .getTimeAggregationList()
        .forEach(
            timeAggregation ->
                requestContext.mapAliasToTimeAggregation(
                    timeAggregation
                        .getAggregation()
                        .getFunction()
                        .getAlias(), // Required to be set by the validators
                    timeAggregation));

    // First group the Aggregations based on the period so that we can issue separate queries
    // to QueryService for each different Period.
    Collection<List<TimeAggregation>> result =
        entitiesRequest.getTimeAggregationList().stream()
            .collect(Collectors.groupingBy(TimeAggregation::getPeriod))
            .values();

    Map<EntityKey, Map<String, MetricSeries.Builder>> entityMetricSeriesMap = new LinkedHashMap<>();
    for (List<TimeAggregation> batch : result) {
      Period period = batch.get(0).getPeriod();
      ChronoUnit unit = ChronoUnit.valueOf(period.getUnit());
      long periodSecs = Duration.of(period.getValue(), unit).getSeconds();
      QueryRequest request =
          buildTimeSeriesQueryRequest(
              entitiesRequest, requestContext, periodSecs, batch, idColumns, timeColumn);

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Sending time series queryRequest to query service: ======== \n {}",
            request.toString());
      }

      Iterator<ResultSetChunk> resultSetChunkIterator =
          queryServiceClient.executeQuery(requestContext, request);

      while (resultSetChunkIterator.hasNext()) {
        ResultSetChunk chunk = resultSetChunkIterator.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received chunk: " + chunk.toString());
        }

        if (chunk.getRowCount() < 1) {
          break;
        }

        if (!chunk.hasResultSetMetadata()) {
          LOG.warn("Chunk doesn't have result metadata so couldn't process the response.");
          break;
        }

        for (Row row : chunk.getRowList()) {
          // Construct the entity id from the entityIdAttributeIds columns
          EntityKey entityKey =
              EntityKey.of(
                  IntStream.range(0, idColumns.size())
                      .mapToObj(value -> row.getColumn(value).getString())
                      .toArray(String[]::new));

          Map<String, MetricSeries.Builder> metricSeriesMap =
              entityMetricSeriesMap.computeIfAbsent(entityKey, k -> new LinkedHashMap<>());

          Interval.Builder intervalBuilder = Interval.newBuilder();

          // Second column is the time column
          Value value =
              QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(
                  row.getColumn(idColumns.size()));
          if (value.getValueType() == ValueType.STRING) {
            long startTime = Long.parseLong(value.getString());
            long endTime = startTime + TimeUnit.SECONDS.toMillis(periodSecs);
            intervalBuilder.setStartTimeMillis(startTime);
            intervalBuilder.setEndTimeMillis(endTime);

            for (int i = idColumns.size() + 1;
                i < chunk.getResultSetMetadata().getColumnMetadataCount();
                i++) {
              ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
              TimeAggregation timeAggregation =
                  requestContext.getTimeAggregationByAlias(metadata.getColumnName());
              if (timeAggregation == null) {
                LOG.warn("Couldn't find an aggregate for column: {}", metadata.getColumnName());
                continue;
              }

              FunctionType functionType =
                  timeAggregation.getAggregation().getFunction().getFunction();
              AttributeMetadata functionAttributeMetadata =
                  resultKeyToAttributeMetadataMap.get(metadata.getColumnName());

              Value convertedValue =
                  QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
                      MetricAggregationFunctionUtil.getValueTypeForFunctionType(
                          functionType, functionAttributeMetadata),
                      resultKeyToAttributeMetadataMap,
                      metadata,
                      row.getColumn(i));

              List<org.hypertrace.gateway.service.v1.common.Expression> healthExpressions =
                  timeAggregation.getAggregation().getFunction().getArgumentsList().stream()
                      .filter(org.hypertrace.gateway.service.v1.common.Expression::hasHealth)
                      .collect(Collectors.toList());
              Preconditions.checkArgument(healthExpressions.size() <= 1);
              Health health = Health.NOT_COMPUTED;

              MetricSeries.Builder seriesBuilder =
                  metricSeriesMap.computeIfAbsent(
                      metadata.getColumnName(), k -> getMetricSeriesBuilder(timeAggregation));
              seriesBuilder.addValue(
                  Interval.newBuilder(intervalBuilder.build())
                      .setValue(convertedValue)
                      .setHealth(health));
            }
          } else {
            LOG.warn(
                "Was expecting STRING values only but received valueType: {}",
                value.getValueType());
          }
        }
      }
    }

    Map<EntityKey, Entity.Builder> resultMap = new LinkedHashMap<>();
    for (Map.Entry<EntityKey, Map<String, MetricSeries.Builder>> entry :
        entityMetricSeriesMap.entrySet()) {
      Entity.Builder entityBuilder =
          Entity.newBuilder()
              .setEntityType(entitiesRequest.getEntityType())
              .setId(entry.getKey().toString())
              .putAllMetricSeries(
                  entry.getValue().entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey, e -> getSortedMetricSeries(e.getValue()))));
      for (int i = 0; i < idColumns.size(); i++) {
        entityBuilder.putAttribute(
            idColumns.get(i),
            Value.newBuilder()
                .setString(entry.getKey().getAttributes().get(i))
                .setValueType(ValueType.STRING)
                .build());
      }
      resultMap.put(entry.getKey(), entityBuilder);
    }
    return new EntityFetcherResponse(resultMap);
  }

  @Override
  public long getTotal(EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());
    // Validate EntitiesRequest
    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);

    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            requestContext,
            entitiesRequest.getEntityType());

    Filter.Builder filterBuilder =
        constructQueryServiceFilter(entitiesRequest, requestContext, entityIdAttributeIds);

    QueryRequest queryRequest =
        QueryRequest.newBuilder()
            .addSelection(
                createDistinctCountByColumnSelection(
                    Optional.ofNullable(entityIdAttributeIds.get(0)).orElseThrow()))
            .setLimit(1)
            .setFilter(filterBuilder)
            .build();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending Query to Query Service ======== \n {}", queryRequest);
    }

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(requestContext, queryRequest);

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received chunk: " + chunk.toString());
      }

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        // only DISTINCTCOUNT column is requested as a selection
        if (row.getColumnList().size() != 1) {
          break;
        }

        return Long.parseLong(row.getColumn(0).getString());
      }
    }

    LOG.error(
        "Unable to query total number of entities from query service for query request: {}",
        queryRequest);
    return 0;
  }

  private QueryRequest buildTimeSeriesQueryRequest(
      EntitiesRequest entitiesRequest,
      EntitiesRequestContext context,
      long periodSecs,
      List<TimeAggregation> timeAggregationBatch,
      List<String> idColumns,
      String timeColumn) {
    long alignedStartTime =
        QueryExpressionUtil.alignToPeriodBoundary(
            entitiesRequest.getStartTimeMillis(), periodSecs, true);
    long alignedEndTime =
        QueryExpressionUtil.alignToPeriodBoundary(
            entitiesRequest.getEndTimeMillis(), periodSecs, false);
    EntitiesRequest timeAlignedEntitiesRequest =
        EntitiesRequest.newBuilder(entitiesRequest)
            .setStartTimeMillis(alignedStartTime)
            .setEndTimeMillis(alignedEndTime)
            .build();

    QueryRequest.Builder builder = QueryRequest.newBuilder();
    timeAggregationBatch.forEach(
        e ->
            builder.addSelection(
                QueryAndGatewayDtoConverter.convertToQueryExpression(e.getAggregation())));

    Filter.Builder queryFilter =
        constructQueryServiceFilter(timeAlignedEntitiesRequest, context, idColumns);
    builder.setFilter(queryFilter);

    // First group by the id columns.
    builder.addAllGroupBy(
        idColumns.stream()
            .map(QueryRequestUtil::createAttributeExpression)
            .collect(Collectors.toList()));

    // Secondary grouping is on time.
    builder.addGroupBy(createTimeColumnGroupByExpression(timeColumn, periodSecs));

    // Pinot truncates the GroupBy results to 10 when there is no limit explicitly but
    // here we neither want the results to be truncated nor apply the limit coming from client.
    // We would like to get all entities based on filters so we set the limit to a high value.
    // TODO: Figure out a reasonable computed limit instead of this hardcoded one. Probably
    //  requested limit * expected max number of time series buckets
    builder.setLimit(DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);

    return builder.build();
  }

  /**
   * Converts the filter in the given request to the query service filter and adds a non null filter
   * on entity id.
   */
  private Filter.Builder constructQueryServiceFilter(
      EntitiesRequest entitiesRequest,
      EntitiesRequestContext context,
      List<String> entityIdAttributeIds) {
    // adds the Id != "null" filter to remove null entities.
    Filter.Builder filterBuilder =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addAllChildFilter(
                entityIdAttributeIds.stream()
                    .map(
                        entityIdAttribute ->
                            createFilter(
                                entityIdAttribute,
                                Operator.NEQ,
                                createStringNullLiteralExpression()))
                    .collect(Collectors.toList()));

    Filter timeSpaceAndProvidedFilter =
        QueryAndGatewayDtoConverter.addTimeAndSpaceFiltersAndConvertToQueryFilter(
            entitiesRequest.getStartTimeMillis(),
            entitiesRequest.getEndTimeMillis(),
            entitiesRequest.getSpaceId(),
            context.getTimestampAttributeId(),
            AttributeMetadataUtil.getSpaceAttributeId(
                attributeMetadataProvider, context, entitiesRequest.getEntityType()),
            entitiesRequest.getFilter());

    if (timeSpaceAndProvidedFilter.equals(Filter.getDefaultInstance())) {
      return filterBuilder;
    }
    if (timeSpaceAndProvidedFilter.getOperator().equals(Operator.AND)) {
      return filterBuilder.addAllChildFilter(timeSpaceAndProvidedFilter.getChildFilterList());
    }
    return filterBuilder.addChildFilter(timeSpaceAndProvidedFilter);
  }

  private MetricSeries.Builder getMetricSeriesBuilder(TimeAggregation timeAggregation) {
    MetricSeries.Builder series = MetricSeries.newBuilder();
    series.setAggregation(timeAggregation.getAggregation().getFunction().getFunction().name());
    series.setPeriod(timeAggregation.getPeriod());
    return series;
  }

  private Map<String, AttributeMetadata> remapAttributeMetadataByResultName(
      EntitiesRequest request, Map<String, AttributeMetadata> attributeMetadataByIdMap) {
    return AttributeMetadataUtil.remapAttributeMetadataByResultKey(
        Streams.concat(
                request.getSelectionList().stream(),
                request.getTimeAggregationList().stream().map(TimeAggregation::getAggregation))
            .collect(Collectors.toList()),
        attributeMetadataByIdMap);
  }
}
