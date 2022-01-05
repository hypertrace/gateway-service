package org.hypertrace.gateway.service.common.datafetcher;

import static org.hypertrace.gateway.service.common.util.ExpressionReader.getExpectedResultNamesForEachAttributeId;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link IEntityFetcher} using the EntityDataService as the data source
 */
public class EntityDataServiceEntityFetcher implements IEntityFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(EntityDataServiceEntityFetcher.class);

  private final EntityQueryServiceClient entityQueryServiceClient;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;

  public EntityDataServiceEntityFetcher(
      EntityQueryServiceClient entityQueryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    this.entityQueryServiceClient = entityQueryServiceClient;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
  }

  @Override
  public EntityFetcherResponse getEntities(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            requestContext,
            entitiesRequest.getEntityType());
    Map<String, List<String>> requestedAliasesByEntityIdAttributeIds =
        getExpectedResultNamesForEachAttributeId(
            entitiesRequest.getSelectionList(), entityIdAttributeIds);
    EntityQueryRequest.Builder builder =
        EntityQueryRequest.newBuilder()
            .setEntityType(entitiesRequest.getEntityType())
            .setFilter(
                EntityServiceAndGatewayServiceConverter.convertToEntityServiceFilter(
                    entitiesRequest.getFilter()))
            // Add EntityID attributes as the first selection
            .addAllSelection(
                entityIdAttributeIds.stream()
                    .map(
                        entityIdAttr ->
                            EntityServiceAndGatewayServiceConverter.createColumnExpression(
                                    entityIdAttr)
                                .build())
                    .collect(Collectors.toList()));

    // add time filter for supported scope
    if (!entitiesRequest.getIncludeNonLiveEntities()) {
      EntityServiceAndGatewayServiceConverter.addBetweenTimeFilter(
          entitiesRequest.getStartTimeMillis(),
          entitiesRequest.getEndTimeMillis(),
          attributeMetadataProvider,
          entitiesRequest,
          builder,
          requestContext);
    }

    // Add all expressions in the select that are already not part of the EntityID attributes
    entitiesRequest.getSelectionList().stream()
        .filter(ExpressionReader::isAttributeSelection)
        .filter(
            expression ->
                ExpressionReader.getSelectionAttributeId(expression)
                    .map(attributeId -> !entityIdAttributeIds.contains(attributeId))
                    .orElse(true))
        .forEach(
            expression ->
                builder.addSelection(
                    EntityServiceAndGatewayServiceConverter.convertToEntityServiceExpression(
                        expression)));

    int limit = entitiesRequest.getLimit();
    if (limit > 0) {
      builder.setLimit(limit);
    }

    int offset = entitiesRequest.getOffset();
    if (offset > 0) {
      builder.setOffset(offset);
    }

    if (!entitiesRequest.getOrderByList().isEmpty()) {
      builder.addAllOrderBy(
          EntityServiceAndGatewayServiceConverter.convertToOrderByExpressions(
              entitiesRequest.getOrderByList()));
    }

    EntityQueryRequest entityQueryRequest = builder.build();
    LOG.debug("Sending Query to EDS  ======== \n {}", entityQueryRequest);
    Iterator<ResultSetChunk> resultSetChunkIterator =
        entityQueryServiceClient.execute(builder.build(), requestContext.getHeaders());

    Map<String, AttributeMetadata> resultMetadataMap =
        this.getAttributeMetadataByAlias(requestContext, entitiesRequest);
    // We want to retain the order as returned from the respective source. Hence using a
    // LinkedHashMap
    Map<EntityKey, Builder> entityBuilders = new LinkedHashMap<>();
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      LOG.debug("Received chunk: {}", chunk);

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        // Construct the entity id from the entityIdAttributes columns
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
          String resultName = chunk.getResultSetMetadata().getColumnMetadata(i).getColumnName();
          AttributeMetadata attributeMetadata = resultMetadataMap.get(resultName);
          entityBuilder.putAttribute(
              resultName,
              EntityServiceAndGatewayServiceConverter.convertQueryValueToGatewayValue(
                  row.getColumn(i), attributeMetadata));
        }
      }
    }

    return new EntityFetcherResponse(entityBuilders);
  }

  @Override
  public EntityFetcherResponse getTimeAggregatedMetrics(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    throw new UnsupportedOperationException("Fetching time series data not supported by EDS");
  }

  @Override
  public long getTotal(EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest) {
    EntityQueryRequest.Builder builder =
        EntityQueryRequest.newBuilder()
            .setEntityType(entitiesRequest.getEntityType())
            .setFilter(
                EntityServiceAndGatewayServiceConverter.convertToEntityServiceFilter(
                    entitiesRequest.getFilter()));

    // add time filter for supported scope
    EntityServiceAndGatewayServiceConverter.addBetweenTimeFilter(
        entitiesRequest.getStartTimeMillis(),
        entitiesRequest.getEndTimeMillis(),
        attributeMetadataProvider,
        entitiesRequest,
        builder,
        requestContext);

    EntityQueryRequest entityQueryRequest = builder.build();

    TotalEntitiesRequest totalEntitiesRequest =
        TotalEntitiesRequest.newBuilder()
            .setEntityType(entitiesRequest.getEntityType())
            .setFilter(entityQueryRequest.getFilter())
            .build();

    TotalEntitiesResponse response =
        entityQueryServiceClient.total(totalEntitiesRequest, requestContext.getHeaders());
    return response.getTotal();
  }

  private Map<String, AttributeMetadata> getAttributeMetadataByAlias(
      EntitiesRequestContext requestContext, EntitiesRequest request) {
    Map<String, AttributeMetadata> attributeMetadataByIdMap =
        attributeMetadataProvider.getAttributesMetadata(requestContext, request.getEntityType());
    return request.getSelectionList().stream()
        .filter(
            expression ->
                ExpressionReader.getSelectionAttributeId(expression)
                    .map(attributeMetadataByIdMap::containsKey)
                    .orElse(false))
        .map(
            expression ->
                Map.entry(
                    ExpressionReader.getSelectionResultName(expression).orElseThrow(),
                    attributeMetadataByIdMap.get(
                        ExpressionReader.getSelectionAttributeId(expression).orElseThrow())))
        .collect(Collectors.toUnmodifiableMap(Entry::getKey, Entry::getValue, (x, y) -> x));
  }
}
