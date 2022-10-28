package org.hypertrace.gateway.service.explore;

import static org.hypertrace.core.attribute.service.v1.AttributeSource.EDS;
import static org.hypertrace.gateway.service.common.ExpressionLocation.COLUMN_GROUP_BY;
import static org.hypertrace.gateway.service.common.ExpressionLocation.COLUMN_ORDER_BY;
import static org.hypertrace.gateway.service.common.ExpressionLocation.COLUMN_SELECTION;
import static org.hypertrace.gateway.service.common.ExpressionLocation.METRIC_AGGREGATION;
import static org.hypertrace.gateway.service.common.ExpressionLocation.METRIC_ORDER_BY;
import static org.hypertrace.gateway.service.common.ExpressionLocation.TIME_AGGREGATION;

import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.v1.entitytype.EntityType;
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
  private static final String NO_TIME_RANGE_SPECIFIED_ERROR_MESSAGE =
      "Source has to set to EDS if " + "no time range specified";

  private final AttributeMetadataProvider attributeMetadataProvider;
  private final ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

  private final RequestHandler normalRequestHandler;
  private final TimeAggregationsRequestHandler timeAggregationsRequestHandler;
  private final TimeAggregationsWithGroupByRequestHandler timeAggregationsWithGroupByRequestHandler;
  private final EntityRequestHandler entityRequestHandler;
  private final ScopeFilterConfigs scopeFilterConfigs;

  private Timer queryExecutionTimer;

  public ExploreService(
      QueryServiceClient queryServiceClient,
      EntityQueryServiceClient entityQueryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFiltersConfig,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.normalRequestHandler = new RequestHandler(queryServiceClient, attributeMetadataProvider);
    this.timeAggregationsRequestHandler =
        new TimeAggregationsRequestHandler(queryServiceClient, attributeMetadataProvider);
    this.timeAggregationsWithGroupByRequestHandler =
        new TimeAggregationsWithGroupByRequestHandler(
            queryServiceClient, attributeMetadataProvider);
    this.entityRequestHandler =
        new EntityRequestHandler(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            queryServiceClient,
            entityQueryServiceClient);
    this.scopeFilterConfigs = scopeFiltersConfig;
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
      // Add extra filters based on the scope.
      request =
          ExploreRequest.newBuilder(request)
              .setFilter(
                  scopeFilterConfigs.createScopeFilter(
                      request.getContext(),
                      request.getFilter(),
                      attributeMetadataProvider,
                      requestContext))
              .build();
      Map<String, AttributeMetadata> attributeMetadataMap =
          attributeMetadataProvider.getAttributesMetadata(requestContext, request.getContext());

      exploreRequestValidator.validate(request, attributeMetadataMap);

      ExploreRequestContext exploreRequestContext =
          new ExploreRequestContext(requestContext.getGrpcContext(), request);
      ExpressionContext expressionContext =
          new ExpressionContext(
              attributeMetadataMap,
              request.getFilter(),
              request.getSelectionList(),
              request.getTimeAggregationList(),
              request.getOrderByList(),
              request.getGroupByList());

      IRequestHandler requestHandler = getRequestHandler(request, expressionContext);

      ExploreResponse.Builder responseBuilder =
          requestHandler.handleRequest(exploreRequestContext, expressionContext);
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
      ExploreRequest request, ExpressionContext expressionContext) {
    if (isContextAnEntityType(request)
        && !hasTimeAggregations(request)
        && !request.getGroupByList().isEmpty()) {
      Optional<String> source =
          ExpressionContext.getSingleSourceForAttributes(
              expressionContext,
              Set.of(
                  COLUMN_SELECTION,
                  METRIC_AGGREGATION,
                  TIME_AGGREGATION,
                  COLUMN_ORDER_BY,
                  METRIC_ORDER_BY,
                  COLUMN_GROUP_BY));
      if ((source.isPresent() && EDS.toString().equals(source.get())) || !hasTimeRange(request)) {
        return entityRequestHandler;
      }
      if ((source.isPresent() && !EDS.toString().equals(source.get())) && !hasTimeRange(request)) {
        throw new IllegalArgumentException(NO_TIME_RANGE_SPECIFIED_ERROR_MESSAGE);
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

  private boolean hasTimeRange(ExploreRequest request) {
    return request.hasStartTimeMillis() && request.hasEndTimeMillis();
  }
}
