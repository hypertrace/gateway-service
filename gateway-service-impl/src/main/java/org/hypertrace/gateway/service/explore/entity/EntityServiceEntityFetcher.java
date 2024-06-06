package org.hypertrace.gateway.service.explore.entity;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Filter.Builder;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfig;
import org.hypertrace.gateway.service.explore.ExploreRequestContext;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityServiceEntityFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityServiceEntityFetcher.class);
  private static final String TOTAL_ALIAS_NAME = "total";
  private static final int DEFAULT_ENTITY_REQUEST_LIMIT = 10_000;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final EntityIdColumnsConfig entityIdColumnsConfig;
  private final EntityQueryServiceClient entityQueryServiceClient;

  public EntityServiceEntityFetcher(
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfig entityIdColumnsConfig,
      EntityQueryServiceClient entityQueryServiceClient) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityIdColumnsConfig = entityIdColumnsConfig;
    this.entityQueryServiceClient = entityQueryServiceClient;
  }

  public List<Row> getResults(
      ExploreRequestContext requestContext, ExploreRequest exploreRequest, Set<String> entityIds) {
    EntityQueryRequest request = buildRequest(requestContext, exploreRequest, entityIds);
    Iterator<ResultSetChunk> result =
        entityQueryServiceClient.execute(request, requestContext.getHeaders());
    return readChunkResults(requestContext, result);
  }

  public int getTotal(ExploreRequestContext requestContext, ExploreRequest exploreRequest) {
    EntityQueryRequest request = buildTotalEntitiesRequest(requestContext, exploreRequest);
    Iterator<ResultSetChunk> resultSetChunkIterator =
        entityQueryServiceClient.execute(request, requestContext.getHeaders());
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      LOGGER.info("Total received chunk is: {}", chunk);
      if (chunk.getRowCount() < 1) {
        break;
      }

      if (!chunk.hasResultSetMetadata()
          || chunk.getResultSetMetadata().getColumnMetadataList().size() != 1
          || !TOTAL_ALIAS_NAME.equals(
              chunk.getResultSetMetadata().getColumnMetadata(0).getColumnName())) {
        LOGGER.warn("Chunk doesn't have result metadata so couldn't process the response.");
        break;
      }
      return chunk.getRow(0).getColumn(0).getInt();
    }

    return 0;
  }

  protected List<Row> readChunkResults(
      ExploreRequestContext requestContext, Iterator<ResultSetChunk> resultSetChunkIterator) {
    List<Row> resultRows = new ArrayList<>();
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      LOGGER.debug("Received chunk: {}", chunk);

      if (chunk.getRowCount() < 1) {
        break;
      }

      if (!chunk.hasResultSetMetadata()) {
        LOGGER.warn("Chunk doesn't have result metadata so couldn't process the response.");
        break;
      }

      resultRows.addAll(
          chunk.getRowList().stream()
              .map(
                  row ->
                      handleRow(
                          row,
                          chunk.getResultSetMetadata(),
                          requestContext,
                          attributeMetadataProvider))
              .collect(Collectors.toUnmodifiableList()));
    }
    return resultRows;
  }

  private Row handleRow(
      org.hypertrace.entity.query.service.v1.Row row,
      ResultSetMetadata resultSetMetadata,
      ExploreRequestContext requestContext,
      AttributeMetadataProvider attributeMetadataProvider) {
    var rowBuilder = org.hypertrace.gateway.service.v1.common.Row.newBuilder();
    for (int i = 0; i < resultSetMetadata.getColumnMetadataCount(); i++) {
      org.hypertrace.entity.query.service.v1.ColumnMetadata metadata =
          resultSetMetadata.getColumnMetadata(i);
      FunctionExpression function =
          requestContext.getFunctionExpressionByAlias(metadata.getColumnName());
      handleColumn(
          row.getColumn(i),
          metadata,
          rowBuilder,
          requestContext,
          attributeMetadataProvider,
          function);
    }
    return rowBuilder.build();
  }

  private void handleColumn(
      Value value,
      ColumnMetadata metadata,
      Row.Builder rowBuilder,
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

  private Map<String, AttributeMetadata> remapAttributeMetadataByResultName(
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

  private EntityQueryRequest buildTotalEntitiesRequest(
      ExploreRequestContext requestContext, ExploreRequest exploreRequest) {
    String entityType = exploreRequest.getContext();

    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfig, requestContext, entityType);
    EntityQueryRequest.Builder builder =
        EntityQueryRequest.newBuilder()
            .setEntityType(entityType)
            .setFilter(buildFilter(exploreRequest, entityIdAttributeIds, Collections.emptySet()));
    List<org.hypertrace.gateway.service.v1.common.Expression> groupBys =
        ExpressionReader.getAttributeExpressions(exploreRequest.getGroupByList());
    builder.addSelection(
        Expression.newBuilder()
            .setFunction(
                Function.newBuilder()
                    .setFunctionName(FunctionType.DISTINCTCOUNT.name())
                    .addAllArguments(
                        groupBys.stream()
                            .map(
                                attribute ->
                                    Expression.newBuilder()
                                        .setColumnIdentifier(
                                            ColumnIdentifier.newBuilder()
                                                .setColumnName(
                                                    attribute
                                                        .getAttributeExpression()
                                                        .getAttributeId())
                                                .setAlias(
                                                    attribute.getAttributeExpression().getAlias())
                                                .build())
                                        .build())
                            .collect(Collectors.toUnmodifiableList()))
                    .setAlias(TOTAL_ALIAS_NAME)
                    .build())
            .build());
    return builder.build();
  }

  private EntityQueryRequest buildRequest(
      ExploreRequestContext requestContext, ExploreRequest exploreRequest, Set<String> entityIds) {
    String entityType = exploreRequest.getContext();

    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfig, requestContext, entityType);
    EntityQueryRequest.Builder builder =
        EntityQueryRequest.newBuilder()
            .setEntityType(entityType)
            .setFilter(buildFilter(exploreRequest, entityIdAttributeIds, entityIds));

    addGroupBys(exploreRequest, builder);
    addSelections(requestContext, exploreRequest, builder);
    builder.setLimit(exploreRequest.getLimit());
    builder.setOffset(exploreRequest.getOffset());
    return builder.build();
  }

  private void addGroupBys(ExploreRequest exploreRequest, EntityQueryRequest.Builder builder) {
    List<org.hypertrace.gateway.service.v1.common.Expression> groupBys =
        ExpressionReader.getAttributeExpressions(exploreRequest.getGroupByList());
    groupBys.forEach(
        groupBy ->
            builder.addGroupBy(
                EntityServiceAndGatewayServiceConverter.convertToEntityServiceExpression(groupBy)
                    .build()));
    groupBys.forEach(
        groupBy ->
            builder.addSelection(
                EntityServiceAndGatewayServiceConverter.convertToEntityServiceExpression(groupBy)));
  }

  private void addSelections(
      ExploreRequestContext requestContext,
      ExploreRequest exploreRequest,
      EntityQueryRequest.Builder builder) {
    List<org.hypertrace.gateway.service.v1.common.Expression> aggregatedSelections =
        ExpressionReader.getFunctionExpressions(exploreRequest.getSelectionList());
    aggregatedSelections.forEach(
        aggregatedSelection -> {
          requestContext.mapAliasToFunctionExpression(
              aggregatedSelection.getFunction().getAlias(), aggregatedSelection.getFunction());
          builder.addSelection(
              EntityServiceAndGatewayServiceConverter.convertToEntityServiceExpression(
                  aggregatedSelection));
        });
  }

  private Filter.Builder buildFilter(
      ExploreRequest exploreRequest, List<String> entityIdAttributeIds, Set<String> entityIds) {
    Builder filterBuilder =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                EntityServiceAndGatewayServiceConverter.convertToEntityServiceFilter(
                    exploreRequest.getFilter()));
    if (entityIds.isEmpty()) {
      return filterBuilder;
    }

    List<Filter> entityIdsInFilter =
        entityIdAttributeIds.stream()
            .map(
                entityIdAttributeId ->
                    Filter.newBuilder()
                        .setLhs(
                            EntityServiceAndGatewayServiceConverter.createColumnExpression(
                                entityIdAttributeId))
                        .setOperator(Operator.IN)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setValueType(ValueType.STRING_ARRAY)
                                                .addAllStringArray(entityIds)))
                                .build())
                        .build())
            .collect(Collectors.toUnmodifiableList());

    return filterBuilder.addAllChildFilter(entityIdsInFilter);
  }
}
