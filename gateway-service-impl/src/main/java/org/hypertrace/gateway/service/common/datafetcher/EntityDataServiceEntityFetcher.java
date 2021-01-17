package org.hypertrace.gateway.service.common.datafetcher;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
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
    List<String> mappedEntityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfigs, requestContext, entitiesRequest.getEntityType());
    EntityQueryRequest.Builder builder =
        EntityQueryRequest.newBuilder()
            .setEntityType(entitiesRequest.getEntityType())
            .setFilter(
                EntityServiceAndGatewayServiceConverter.convertToEntityServiceFilter(
                    entitiesRequest.getFilter()))
            // Add EntityID attributes as the first selection
            .addAllSelection(
                mappedEntityIdAttributeIds.stream()
                    .map(
                        entityIdAttr ->
                            EntityServiceAndGatewayServiceConverter.createColumnExpression(
                                    entityIdAttr)
                                .build())
                    .collect(Collectors.toList()));

    // add time filter for supported scope
    EntityServiceAndGatewayServiceConverter.addBetweenTimeFilter(
        entitiesRequest.getStartTimeMillis(),
        entitiesRequest.getEndTimeMillis(),
        attributeMetadataProvider,
        entitiesRequest,
        builder,
        requestContext);

    // Add all expressions in the select that are already not part of the EntityID attributes
    entitiesRequest.getSelectionList().stream()
        .filter(expression -> expression.getValueCase() == ValueCase.COLUMNIDENTIFIER)
        .filter(
            expression ->
                !mappedEntityIdAttributeIds.contains(
                    expression.getColumnIdentifier().getColumnName()))
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending Query to EDS  ======== \n {}", entityQueryRequest);
    }

    Iterator<ResultSetChunk> resultSetChunkIterator =
        entityQueryServiceClient.execute(builder.build(), requestContext.getHeaders());

    // We want to retain the order as returned from the respective source. Hence using a
    // LinkedHashMap
    Map<EntityKey, Builder> entityBuilders = new LinkedHashMap<>();
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
                IntStream.range(0, mappedEntityIdAttributeIds.size())
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));
        Builder entityBuilder = entityBuilders.computeIfAbsent(entityKey, k -> Entity.newBuilder());
        entityBuilder.setEntityType(entitiesRequest.getEntityType());
        entityBuilder.setId(entityKey.toString());

        // Always include the id in entity since that's needed to make follow up queries in
        // optimal fashion. If this wasn't really requested by the client, it should be removed
        // as post processing.
        for (int i = 0; i < mappedEntityIdAttributeIds.size(); i++) {
          entityBuilder.putAttribute(
              mappedEntityIdAttributeIds.get(i),
              Value.newBuilder()
                  .setString(entityKey.getAttributes().get(i))
                  .setValueType(ValueType.STRING)
                  .build());
        }

        for (int i = mappedEntityIdAttributeIds.size();
            i < chunk.getResultSetMetadata().getColumnMetadataCount();
            i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);

          String attributeName = metadata.getColumnName();
          entityBuilder.putAttribute(
              attributeName,
              EntityServiceAndGatewayServiceConverter.convertToGatewayValue(
                  attributeName,
                  row.getColumn(i),
                  attributeMetadataProvider.getAttributesMetadata(
                      requestContext, entitiesRequest.getEntityType())));
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
}
