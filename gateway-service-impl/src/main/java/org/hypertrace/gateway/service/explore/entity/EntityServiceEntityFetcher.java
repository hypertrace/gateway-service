package org.hypertrace.gateway.service.explore.entity;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Filter.Builder;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.explore.ExploreRequestContext;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;

public class EntityServiceEntityFetcher {
  private static final int LIMIT_ENTITY_REQUEST = 10_000;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;
  private final EntityQueryServiceClient entityQueryServiceClient;

  public EntityServiceEntityFetcher(
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      EntityQueryServiceClient entityQueryServiceClient) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    this.entityQueryServiceClient = entityQueryServiceClient;
  }

  public Iterator<ResultSetChunk> getResults(
      ExploreRequestContext requestContext, ExploreRequest exploreRequest, Set<String> entityIds) {
    EntityQueryRequest request = buildRequest(requestContext, exploreRequest, entityIds);
    return entityQueryServiceClient.execute(request, requestContext.getHeaders());
  }

  private EntityQueryRequest buildRequest(
      ExploreRequestContext requestContext, ExploreRequest exploreRequest, Set<String> entityIds) {
    String entityType = exploreRequest.getContext();

    List<String> entityIdAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider, entityIdColumnsConfigs, requestContext, entityType);
    EntityQueryRequest.Builder builder =
        EntityQueryRequest.newBuilder()
            .setEntityType(entityType)
            .setFilter(buildFilter(exploreRequest, entityIdAttributeIds, entityIds));

    addGroupBys(exploreRequest, builder);
    addSelections(requestContext, exploreRequest, builder);
    builder.setLimit(LIMIT_ENTITY_REQUEST);
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
