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
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.converters.ExploreToEntityConverter;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.ExecutionTreeUtils;
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
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;

  private Timer queryExecutionTimer;

  public ExploreService(
      QueryServiceClient queryServiceClient,
      int requestTimeout,
      EntityQueryServiceClient entityQueryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFiltersConfig,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.normalRequestHandler =
        new RequestHandler(queryServiceClient, requestTimeout, attributeMetadataProvider);
    this.timeAggregationsRequestHandler =
        new TimeAggregationsRequestHandler(
            queryServiceClient, requestTimeout, attributeMetadataProvider);
    this.timeAggregationsWithGroupByRequestHandler =
        new TimeAggregationsWithGroupByRequestHandler(
            queryServiceClient, requestTimeout, attributeMetadataProvider);
    this.entityRequestHandler =
        new EntityRequestHandler(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            queryServiceClient,
            requestTimeout,
            entityQueryServiceClient);
    this.scopeFilterConfigs = scopeFiltersConfig;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    initMetrics();
  }

  private void initMetrics() {
    queryExecutionTimer =
        PlatformMetricsRegistry.registerTimer(
            "hypertrace.explore.query.execution", ImmutableMap.of());
  }

  public ExploreResponse explore(
      String tenantId, ExploreRequest request, Map<String, String> requestHeaders) {

    final Instant start = Instant.now();
    try {
      ExploreRequestContext exploreRequestContext =
          new ExploreRequestContext(tenantId, request, requestHeaders);

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
          new ExploreRequestContext(tenantId, request, requestHeaders);

      Map<String, AttributeMetadata> attributeMetadataMap =
          attributeMetadataProvider.getAttributesMetadata(
              newExploreRequestContext, request.getContext());
      exploreRequestValidator.validate(request, attributeMetadataMap);

      IRequestHandler requestHandler = getRequestHandler(newExploreRequestContext, request);

      ExploreResponse.Builder responseBuilder =
          requestHandler.handleRequest(newExploreRequestContext, request);

      return responseBuilder.build();
    } finally {
      queryExecutionTimer.record(
          Duration.between(start, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  private boolean isContextAnEntityType(ExploreRequest request) {
    return Arrays.stream(EntityType.values())
        .anyMatch(entityType -> entityType.name().equalsIgnoreCase(request.getContext()));
  }

  private IRequestHandler getRequestHandler(
      ExploreRequestContext requestContext, ExploreRequest request) {
    if (isContextAnEntityType(request)
        && !hasTimeAggregations(request)
        && !request.getGroupByList().isEmpty()) {
      EntitiesRequestContext entitiesRequestContext =
          ExploreToEntityConverter.convert(attributeMetadataProvider, requestContext);
      ExecutionContext executionContext =
          new ExecutionContext(
              attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequestContext, request);
      Optional<String> source =
          ExecutionTreeUtils.getSingleSourceForAllAttributes(executionContext);
      if (source.isPresent() && EDS.toString().equals(source.get())) {
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

  private boolean hasTimeAggregations(ExploreRequest request) {
    return !request.getTimeAggregationList().isEmpty();
  }

  private boolean hasTimeAggregationsAndGroupBy(ExploreRequest request) {
    return !request.getTimeAggregationList().isEmpty() && !request.getGroupByList().isEmpty();
  }
}
