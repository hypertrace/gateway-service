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
import org.hypertrace.gateway.service.common.util.OrderByUtil;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.explore.ExploreRequestContext;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityServiceEntityFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(EntityServiceEntityFetcher.class);
  private static final int DEFAULT_ENTITY_REQUEST_LIMIT = 10_000;

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
    addLimitAndOffset(requestContext, exploreRequest, builder);

    // TODO: Push order by down to EQS
    // EQS (and document-store) currently doesn't support order by on functional expressions
    // If there are order by expressions, specify a large limit and track actual limit, offset and
    // order by
    // expression list, so we can compute these once we get the results.
    if (!requestContext.getOrderByExpressions().isEmpty()) {
      // Ideally, needs the limit and offset for group by, since the fetcher is only triggered when
      // there is a group by, or a single aggregation selection. A single aggregated selection would
      // always return a single result (i.e. limit 1)
      builder.setOffset(0);
      builder.setLimit(DEFAULT_ENTITY_REQUEST_LIMIT);
      // Will need to do the ordering, limit and offset ourselves after we get the group by results
      requestContext.setOrderByExpressions(getRequestOrderByExpressions(exploreRequest));
    }

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

  private void addLimitAndOffset(
      ExploreRequestContext requestContext,
      ExploreRequest exploreRequest,
      EntityQueryRequest.Builder builder) {
    // handle group by scenario with group limit set
    if (requestContext.hasGroupBy()) {
      int limit = exploreRequest.getLimit();
      if (exploreRequest.getGroupLimit() > 0) {
        // in group by scenario, set limit to minimum of limit or group-limit
        limit = Math.min(exploreRequest.getLimit(), exploreRequest.getGroupLimit());
      }
      // don't exceed default group by limit
      if (limit > DEFAULT_ENTITY_REQUEST_LIMIT) {
        LOG.error(
            "Trying to query for rows more than the default limit {} : {}",
            DEFAULT_ENTITY_REQUEST_LIMIT,
            exploreRequest);
        throw new UnsupportedOperationException(
            "Trying to query for rows more than the default limit " + exploreRequest);
      }
      builder.setLimit(limit);
    } else {
      builder.setLimit(exploreRequest.getLimit());
      builder.setOffset(exploreRequest.getOffset());
    }
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

  private List<OrderByExpression> getRequestOrderByExpressions(ExploreRequest request) {
    return OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
        request.getOrderByList(), request.getSelectionList(), request.getTimeAggregationList());
  }
}
