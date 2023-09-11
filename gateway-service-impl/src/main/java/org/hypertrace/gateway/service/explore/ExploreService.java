package org.hypertrace.gateway.service.explore;

import static org.hypertrace.core.attribute.service.v1.AttributeSource.EDS;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.gateway.service.EntityTypesProvider;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.explore.entity.EntityRequestHandler;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

public class ExploreService {

  private final AttributeMetadataProvider attributeMetadataProvider;
  private final ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

  private final RequestHandler normalRequestHandler;
  private final TimeAggregationsRequestHandler timeAggregationsRequestHandler;
  private final TimeAggregationsWithGroupByRequestHandler timeAggregationsWithGroupByRequestHandler;
  private final EntityRequestHandler entityRequestHandler;
  private final ScopeFilterConfigs scopeFilterConfigs;
  private final EntityTypesProvider entityTypesProvider;

  private Timer queryExecutionTimer;

  public ExploreService(
      QueryServiceClient queryServiceClient,
      EntityQueryServiceClient entityQueryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFiltersConfig,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      EntityTypesProvider entityTypesProvider) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.normalRequestHandler = new RequestHandler(queryServiceClient, attributeMetadataProvider);
    this.timeAggregationsRequestHandler =
        new TimeAggregationsRequestHandler(queryServiceClient, attributeMetadataProvider);
    this.timeAggregationsWithGroupByRequestHandler =
        new TimeAggregationsWithGroupByRequestHandler(
            attributeMetadataProvider, normalRequestHandler, timeAggregationsRequestHandler);
    this.entityRequestHandler =
        new EntityRequestHandler(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            queryServiceClient,
            entityQueryServiceClient);
    this.scopeFilterConfigs = scopeFiltersConfig;
    this.entityTypesProvider = entityTypesProvider;
    initMetrics();
  }

  private void initMetrics() {
    queryExecutionTimer =
        PlatformMetricsRegistry.registerTimer(
            "hypertrace.explore.query.execution", ImmutableMap.of());
  }

  public ExploreResponse explore(RequestContext requestContext, ExploreRequest request) {

    final Instant start = Instant.now();
    try {
      ExploreRequestContext exploreRequestContext =
          new ExploreRequestContext(requestContext.getGrpcContext(), request);

      // Add extra filters based on the scope.
      request =
          ExploreRequest.newBuilder(request)
              .setFilter(
                  scopeFilterConfigs.createScopeFilter(
                      request.getContext(),
                      request.getFilter(),
                      attributeMetadataProvider,
                      exploreRequestContext))
              .build();
      ExploreRequestContext newExploreRequestContext =
          new ExploreRequestContext(requestContext.getGrpcContext(), request);

      Map<String, AttributeMetadata> attributeMetadataMap =
          attributeMetadataProvider.getAttributesMetadata(
              newExploreRequestContext, request.getContext());
      exploreRequestValidator.validate(request, attributeMetadataMap);

      IRequestHandler requestHandler =
          getRequestHandler(request, attributeMetadataMap, requestContext.getTenantId());

      ExploreResponse.Builder responseBuilder =
          requestHandler.handleRequest(newExploreRequestContext, request);

      return responseBuilder.build();
    } finally {
      queryExecutionTimer.record(
          Duration.between(start, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  private boolean isContextAnEntityType(ExploreRequest request, String tenantId) {
    return Stream.concat(
            entityTypesProvider.getEntityTypes(tenantId).stream(),
            Arrays.stream(EntityType.values()).map(EntityType::name))
        .anyMatch(entityType -> entityType.equalsIgnoreCase(request.getContext()));
  }

  private IRequestHandler getRequestHandler(
      ExploreRequest request,
      Map<String, AttributeMetadata> attributeMetadataMap,
      String tenantId) {
    if (isContextAnEntityType(request, tenantId)
        && !hasTimeAggregations(request)
        && !request.getGroupByList().isEmpty()) {
      ExpressionContext expressionContext =
          new ExpressionContext(
              attributeMetadataMap,
              request.getFilter(),
              request.getSelectionList(),
              request.getTimeAggregationList(),
              request.getOrderByList(),
              request.getGroupByList());

      /*
      This has been added because we wanted to use entity request handler if non-live entities
      are requested. Following scenarios needs to be catered while checking for non-live entities

      Case 1: all attributes, source is QS, EDS
      1. Non-Live entities = false, normalRequestHandler must be selected
      2. Non-Live entities = true, entityRequestHandler must be selected

      Case 2: some attributes, Source is QS only
      Ignore the non-live entities flag and return normalRequestHandler

      Case 3: common source for all attributes is EDS only,
      Ignore the non-live entities flag and return entityRequestHandler
       */
      if (includeNonLiveEntities(request)) {
        boolean allFieldsOnEDS =
            ExpressionContext.areAllFieldsOnlyOnCurrentDataSource(expressionContext, EDS.name());
        if (allFieldsOnEDS) {
          return entityRequestHandler;
        }
      }

      Optional<String> source =
          ExpressionContext.getSingleSourceForAllAttributes(expressionContext);
      if ((source.isPresent() && EDS.toString().equals(source.get()))) {
        return entityRequestHandler;
      }
    }
    if (hasTimeAggregationsAndGroupBy(request)) {
      return timeAggregationsWithGroupByRequestHandler;
    }
    if (hasTimeAggregations(request)) {
      return timeAggregationsRequestHandler;
    }

    return normalRequestHandler;
  }

  private boolean includeNonLiveEntities(ExploreRequest request) {
    return request.hasContextOption()
        && request.getContextOption().hasEntityOption()
        && request.getContextOption().getEntityOption().getIncludeNonLiveEntities();
  }

  private boolean hasTimeAggregations(ExploreRequest request) {
    return !request.getTimeAggregationList().isEmpty();
  }

  private boolean hasTimeAggregationsAndGroupBy(ExploreRequest request) {
    return !request.getTimeAggregationList().isEmpty() && !request.getGroupByList().isEmpty();
  }
}
