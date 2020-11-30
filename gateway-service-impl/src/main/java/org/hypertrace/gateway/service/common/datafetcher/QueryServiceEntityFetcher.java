package org.hypertrace.gateway.service.common.datafetcher;

import static org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter.convertToQueryExpression;
import static org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter.convertToQueryFilter;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.query.service.util.QueryRequestUtil;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.QueryRequestContext;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.util.ArithmeticValueUtil;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntitiesRequestValidator;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
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

/**
 * An implementation of the {@link IEntityFetcher} using the QueryService as the data source
 */
public class QueryServiceEntityFetcher implements IEntityFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(QueryServiceEntityFetcher.class);
  private static final String COUNT_COLUMN_NAME = "Count";
  private static final String QUERY_SERVICE_NULL = "null";

  private final EntitiesRequestValidator entitiesRequestValidator = new EntitiesRequestValidator();
  private final QueryServiceClient queryServiceClient;
  private final int requestTimeout;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;

  public QueryServiceEntityFetcher(
      QueryServiceClient queryServiceClient, int qsRequestTimeout,
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    this.queryServiceClient = queryServiceClient;
    this.requestTimeout = qsRequestTimeout;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
  }

  @Override
  public EntityFetcherResponse getEntities(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());
    // Validate EntitiesRequest
    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);

    List<String> entityIdAttributes =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfigs, requestContext, entitiesRequest.getEntityType());
    List<org.hypertrace.gateway.service.v1.common.Expression> aggregates =
        ExpressionReader.getFunctionExpressions(entitiesRequest.getSelectionList().stream());

    QueryRequest.Builder builder =
        constructSelectionQuery(requestContext, entitiesRequest, entityIdAttributes, aggregates);

    adjustLimitAndOffset(builder, entitiesRequest.getLimit(), entitiesRequest.getOffset());

    QueryRequest queryRequest = builder.build();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending Query to Query Service ======== \n {}", queryRequest);
    }

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(queryRequest, requestContext.getHeaders(),
            requestTimeout);

    // We want to retain the order as returned from the respective source. Hence using a
    // LinkedHashMap
    Map<EntityKey, Entity.Builder> entityBuilders = new LinkedHashMap<>();
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received chunk: " + chunk.toString());
      }

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        // Construct the entity id from the entityIdAttributes columns
        EntityKey entityKey =
            EntityKey.of(
                IntStream.range(0, entityIdAttributes.size())
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));
        Builder entityBuilder = entityBuilders.computeIfAbsent(entityKey, k -> Entity.newBuilder());
        entityBuilder.setEntityType(entitiesRequest.getEntityType());

        // Always include the id in entity since that's needed to make follow up queries in
        // optimal fashion. If this wasn't really requested by the client, it should be removed
        // as post processing.
        for (int i = 0; i < entityIdAttributes.size(); i++) {
          entityBuilder.putAttribute(
              entityIdAttributes.get(i),
              org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                  .setString(entityKey.getAttributes().get(i))
                  .setValueType(org.hypertrace.gateway.service.v1.common.ValueType.STRING)
                  .build());
        }

        for (int i = entityIdAttributes.size();
            i < chunk.getResultSetMetadata().getColumnMetadataCount();
            i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
          org.hypertrace.core.query.service.api.Value columnValue = row.getColumn(i);
          addEntityAttribute(entityBuilder,
              metadata,
              columnValue,
              attributeMetadataMap,
              aggregates.isEmpty());
        }
      }
    }

    return new EntityFetcherResponse(entityBuilders);
  }

  @Override
  public EntityFetcherResponse getAggregatedMetrics(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    // Only supported filter is entityIds IN ["id1", "id2", "id3"]
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());
    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);

    List<org.hypertrace.gateway.service.v1.common.Expression> aggregates =
        ExpressionReader.getFunctionExpressions(entitiesRequest.getSelectionList().stream());
    if (aggregates.isEmpty()) {
      return new EntityFetcherResponse();
    }

    List<String> entityIdAttributes =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfigs, requestContext, entitiesRequest.getEntityType());

    QueryRequest.Builder builder =
        constructSelectionQuery(requestContext, entitiesRequest, entityIdAttributes, aggregates);
    adjustLimitAndOffset(builder, entitiesRequest.getLimit(), entitiesRequest.getOffset());

    QueryRequest request = builder.build();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending Aggregated Metrics Request to Query Service ======== \n {}", request);
    }

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(request, requestContext.getHeaders(),
            requestTimeout);

    // We want to retain the order as returned from the respective source. Hence using a
    // LinkedHashMap
    Map<EntityKey, Builder> entityMap = new LinkedHashMap<>();

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
        // Construct the EntityKey from the EntityId attributes columns
        EntityKey entityKey =
            EntityKey.of(
                IntStream.range(0, entityIdAttributes.size())
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));
        Builder entityBuilder = entityMap.computeIfAbsent(entityKey, k -> Entity.newBuilder());
        entityBuilder.setEntityType(entitiesRequest.getEntityType());

        // Always include the id in entity since that's needed to make follow up queries in
        // optimal fashion. If this wasn't really requested by the client, it should be removed
        // as post processing.
        for (int i = 0; i < entityIdAttributes.size(); i++) {
          entityBuilder.putAttribute(
              entityIdAttributes.get(i),
              org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                  .setString(entityKey.getAttributes().get(i))
                  .setValueType(org.hypertrace.gateway.service.v1.common.ValueType.STRING)
                  .build());
        }

        for (int i = entityIdAttributes.size();
            i < chunk.getResultSetMetadata().getColumnMetadataCount();
            i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
          org.hypertrace.core.query.service.api.Value columnValue = row.getColumn(i);
          addAggregateMetric(entityBuilder,
              requestContext,
              entitiesRequest,
              metadata,
              columnValue,
              attributeMetadataMap);
        }
      }
    }
    return new EntityFetcherResponse(entityMap);
  }

  @Override
  public EntityFetcherResponse getEntitiesAndAggregatedMetrics(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    // Validate EntitiesRequest
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());
    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);
    List<String> entityIdAttributes =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfigs, requestContext, entitiesRequest.getEntityType());

    List<org.hypertrace.gateway.service.v1.common.Expression> aggregates =
        ExpressionReader.getFunctionExpressions(entitiesRequest.getSelectionList().stream());

    QueryRequest.Builder builder =
        constructSelectionQuery(requestContext, entitiesRequest, entityIdAttributes, aggregates);

    adjustLimitAndOffset(builder, entitiesRequest.getLimit(), entitiesRequest.getOffset());

    // Order by from the request.
    builder.addAllOrderBy(
            QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(entitiesRequest.getOrderByList()));

    QueryRequest queryRequest = builder.build();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending Query to Query Service ======== \n {}", queryRequest);
    }

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(queryRequest, requestContext.getHeaders(), requestTimeout);

    // We want to retain the order as returned from the respective source. Hence using a
    // LinkedHashMap
    Map<EntityKey, Entity.Builder> entityBuilders = new LinkedHashMap<>();
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received chunk: " + chunk.toString());
      }

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        // Construct the entity id from the entityIdAttributes columns
        EntityKey entityKey =
            EntityKey.of(
                IntStream.range(0, entityIdAttributes.size())
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));
        Builder entityBuilder = entityBuilders.computeIfAbsent(entityKey, k -> Entity.newBuilder());
        entityBuilder.setEntityType(entitiesRequest.getEntityType());

        // Always include the id in entity since that's needed to make follow up queries in
        // optimal fashion. If this wasn't really requested by the client, it should be removed
        // as post processing.
        for (int i = 0; i < entityIdAttributes.size(); i++) {
          entityBuilder.putAttribute(
              entityIdAttributes.get(i),
              org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                  .setString(entityKey.getAttributes().get(i))
                  .setValueType(org.hypertrace.gateway.service.v1.common.ValueType.STRING)
                  .build());
        }

        for (int i = entityIdAttributes.size();
            i < chunk.getResultSetMetadata().getColumnMetadataCount();
            i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
          org.hypertrace.core.query.service.api.Value columnValue = row.getColumn(i);
          // add entity attributes from selections

          org.hypertrace.gateway.service.v1.common.FunctionExpression function =
              requestContext.getFunctionExpressionByAlias(metadata.getColumnName());
          /* this is aggregated metric column*/
          if (function != null) {
            addAggregateMetric(entityBuilder,
                requestContext,
                entitiesRequest,
                metadata,
                columnValue,
                attributeMetadataMap);
          } else { // A simple column selection
            addEntityAttribute(entityBuilder,
                metadata,
                columnValue,
                attributeMetadataMap,
                aggregates.isEmpty());
          }

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
    if (builder.getGroupByCount() > 1) {
      builder.setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);
    } else {
      builder.setLimit(limit);
      builder.setOffset(offset);
    }
  }

  private QueryRequest.Builder constructSelectionQuery(EntitiesRequestContext requestContext,
      EntitiesRequest entitiesRequest,
      List<String> entityIdAttributes,
      List<org.hypertrace.gateway.service.v1.common.Expression> aggregates) {
    List<Expression> idExpressions =
        entityIdAttributes.stream()
            .map(QueryRequestUtil::createColumnExpression)
            .map(Expression.Builder::build)
            .collect(Collectors.toList());
    Filter.Builder filterBuilder =
        constructQueryServiceFilter(entitiesRequest, requestContext, entityIdAttributes);

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
        .filter(expression -> expression.getValueCase() == ValueCase.COLUMNIDENTIFIER)
        .filter(
            expression ->
                !entityIdAttributes.contains(expression.getColumnIdentifier().getColumnName()))
        .forEach(
            expression -> {
              Expression.Builder expBuilder = convertToQueryExpression(expression);
              builder.addSelection(expBuilder);
              builder.addGroupBy(expBuilder);
            });

    // Pinot's GroupBy queries need at least one aggregate operation in the selection
    // so we add count(*) as a dummy placeholder.
    if (aggregates.isEmpty()) {
      builder.addSelection(QueryRequestUtil.createCountByColumnSelection(entityIdAttributes.toArray(new String[]{})));
    }
    return builder;
  }

  private void addEntityAttribute(Entity.Builder entityBuilder,
      ColumnMetadata metadata,
      org.hypertrace.core.query.service.api.Value columnValue,
      Map<String, AttributeMetadata> attributeMetadataMap,
      boolean isSkipCountColumn) {

    // Ignore the count column since we introduced that ourselves into the query
    if (isSkipCountColumn &&
        StringUtils.equals(COUNT_COLUMN_NAME, metadata.getColumnName())) {
      return;
    }

    String attributeName = metadata.getColumnName();
    entityBuilder.putAttribute(
        attributeName,
        QueryAndGatewayDtoConverter.convertToGatewayValue(
            attributeName,
            columnValue,
            attributeMetadataMap));
  }

  private void addAggregateMetric(Entity.Builder entityBuilder,
      QueryRequestContext requestContext,
      EntitiesRequest entitiesRequest,
      ColumnMetadata metadata,
      org.hypertrace.core.query.service.api.Value columnValue,
      Map<String, AttributeMetadata> attributeMetadataMap) {

    org.hypertrace.gateway.service.v1.common.FunctionExpression function =
        requestContext.getFunctionExpressionByAlias(metadata.getColumnName());
    List<org.hypertrace.gateway.service.v1.common.Expression> healthExpressions =
        function.getArgumentsList().stream()
            .filter(org.hypertrace.gateway.service.v1.common.Expression::hasHealth)
            .collect(Collectors.toList());
    Preconditions.checkArgument(healthExpressions.size() <= 1);
    Health health = Health.NOT_COMPUTED;

    if (FunctionType.AVGRATE == function.getFunction()) {
      Value avgRateValue =
          ArithmeticValueUtil.computeAvgRate(
              function,
              columnValue,
              entitiesRequest.getStartTimeMillis(),
              entitiesRequest.getEndTimeMillis());

      entityBuilder.putMetric(
          metadata.getColumnName(),
          AggregatedMetricValue.newBuilder()
              .setValue(avgRateValue)
              .setFunction(function.getFunction())
              .setHealth(health)
              .build());
    } else {
      Value gwValue =
          QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
              MetricAggregationFunctionUtil.getValueTypeFromFunction(
                  function, attributeMetadataMap),
              attributeMetadataMap,
              metadata,
              columnValue);
      entityBuilder.putMetric(
          metadata.getColumnName(),
          AggregatedMetricValue.newBuilder()
              .setValue(gwValue)
              .setFunction(function.getFunction())
              .setHealth(health)
              .build());
    }
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
            attributeMetadataProvider, entityIdColumnsConfigs, requestContext, entitiesRequest.getEntityType());
    String timeColumn =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, requestContext, entitiesRequest.getEntityType());
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());

    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);

    entitiesRequest
        .getTimeAggregationList()
        .forEach(
            (timeAggregation ->
                requestContext.mapAliasToTimeAggregation(
                    timeAggregation
                        .getAggregation()
                        .getFunction()
                        .getAlias(), // Required to be set by the validators
                    timeAggregation)));

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
          queryServiceClient.executeQuery(request, requestContext.getHeaders(), requestTimeout);

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
          // Construct the entity id from the entityIdAttributes columns
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
            long time = Long.parseLong(value.getString());
            intervalBuilder.setStartTimeMillis(time);
            intervalBuilder.setEndTimeMillis(time + TimeUnit.SECONDS.toMillis(periodSecs));

            for (int i = idColumns.size() + 1;
                i < chunk.getResultSetMetadata().getColumnMetadataCount();
                i++) {
              ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
              org.hypertrace.gateway.service.v1.common.TimeAggregation timeAggregation =
                  requestContext.getTimeAggregationByAlias(metadata.getColumnName());

              if (timeAggregation == null) {
                LOG.warn("Couldn't find an aggregate for column: {}", metadata.getColumnName());
                continue;
              }

              Value convertedValue =
                  QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
                      MetricAggregationFunctionUtil.getValueTypeFromFunction(
                          timeAggregation.getAggregation().getFunction(), attributeMetadataMap),
                      attributeMetadataMap,
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
              .putAllMetricSeries(
                  entry.getValue().entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey, e -> getSortedMetricSeries(e.getValue()))));
      for (int i = 0; i < idColumns.size(); i++) {
        entityBuilder.putAttribute(
            idColumns.get(i),
            org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                .setString(entry.getKey().getAttributes().get(i))
                .setValueType(org.hypertrace.gateway.service.v1.common.ValueType.STRING)
                .build());
      }
      resultMap.put(entry.getKey(), entityBuilder);
    }
    return new EntityFetcherResponse(resultMap);
  }

  @Override
  public int getTotalEntities(EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, entitiesRequest.getEntityType());
    // Validate EntitiesRequest
    entitiesRequestValidator.validate(entitiesRequest, attributeMetadataMap);
    return getTotalEntitiesForMultipleEntityId(requestContext, entitiesRequest);
  }

  private int getTotalEntitiesForMultipleEntityId(EntitiesRequestContext requestContext,
      EntitiesRequest entitiesRequest) {
    EntityFetcherResponse entityFetcherResponse = getEntities(
        requestContext,
        EntitiesRequest.newBuilder(entitiesRequest)
            .clearSelection()
            .clearTimeAggregation()
            .clearOrderBy()
            .setOffset(0)
            .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build()
        );
    return entityFetcherResponse.size();
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

    Filter.Builder queryFilter = constructQueryServiceFilter(timeAlignedEntitiesRequest, context, idColumns);
    builder.setFilter(queryFilter);

    // First group by the id columns.
    builder.addAllGroupBy(
        idColumns.stream()
            .map(QueryRequestUtil::createColumnExpression)
            .map(Expression.Builder::build)
            .collect(Collectors.toList()));

    // Secondary grouping is on time.
    builder.addGroupBy(
        Expression.newBuilder()
            .setFunction(QueryRequestUtil.createTimeColumnGroupByFunction(timeColumn, periodSecs)));

    // Pinot truncates the GroupBy results to 10 when there is no limit explicitly but
    // here we neither want the results to be truncated nor apply the limit coming from client.
    // We would like to get all entities based on filters so we set the limit to a high value.
    // TODO: Figure out a reasonable computed limit instead of this hardcoded one. Probably
    //  requested limit * expected max number of time series buckets
    builder.setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);

    return builder.build();
  }

  /**
   * Converts the filter in the given request to the query service filter and adds a non null filter on entity id.
   */
  private Filter.Builder constructQueryServiceFilter(EntitiesRequest entitiesRequest, EntitiesRequestContext context,
                                                     List<String> entityIdAttributes) {
    // adds the Id != "null" filter to remove null entities.
    Filter.Builder filterBuilder = Filter.newBuilder()
        .setOperator(Operator.AND)
        .addAllChildFilter(
            entityIdAttributes.stream()
                .map(
                    entityIdAttribute ->
                        QueryRequestUtil.createColumnValueFilter(
                            entityIdAttribute, Operator.NEQ, QUERY_SERVICE_NULL))
                .map(Filter.Builder::build)
                .collect(Collectors.toList()));

    // Convert the existing filter into query service filter.
    Filter queryFilter = convertToQueryFilter(entitiesRequest.getFilter()).build();
    if (!Filter.getDefaultInstance().equals(queryFilter)) {
      filterBuilder.addChildFilter(queryFilter);
    }

    // Time range is a mandatory filter for query service, hence add it if it's not already present.
    if (!hasTimeRangeFilter(queryFilter, context.getTimestampAttributeId())) {
      filterBuilder.addChildFilter(Filter.newBuilder().setOperator(Operator.AND)
          .addChildFilter(createTimeFilter(context.getTimestampAttributeId(), Operator.GE, entitiesRequest.getStartTimeMillis()))
          .addChildFilter(createTimeFilter(context.getTimestampAttributeId(), Operator.LT, entitiesRequest.getEndTimeMillis())));
    }

    return filterBuilder;
  }

  private static Filter createTimeFilter(String columnName, Operator op, long value) {
    ColumnIdentifier.Builder timeColumn = ColumnIdentifier.newBuilder().setColumnName(columnName);
    Expression.Builder lhs = Expression.newBuilder().setColumnIdentifier(timeColumn);
    LiteralConstant.Builder constant = LiteralConstant.newBuilder().setValue(
        org.hypertrace.core.query.service.api.Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG).setLong(value));
    Expression.Builder rhs = Expression.newBuilder().setLiteral(constant);
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  private boolean hasTimeRangeFilter(Filter filter, String timestampAttributeId) {
    if (filter.getOperator() == Operator.AND || filter.getOperator() == Operator.OR) {
      return filter.getChildFilterList().stream()
          .anyMatch(f -> hasTimeRangeFilter(f, timestampAttributeId));
    } else {
      return filter.getLhs().getValueCase() == Expression.ValueCase.COLUMNIDENTIFIER &&
          filter.getLhs().getColumnIdentifier().getColumnName().equals(timestampAttributeId);
    }
  }

  private MetricSeries.Builder getMetricSeriesBuilder(
      org.hypertrace.gateway.service.v1.common.TimeAggregation timeAggregation) {
    MetricSeries.Builder series = MetricSeries.newBuilder();
    series.setAggregation(timeAggregation.getAggregation().getFunction().getFunction().name());
    series.setPeriod(timeAggregation.getPeriod());
    return series;
  }
}
