package org.hypertrace.gateway.service.explore;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.transformer.RequestPreProcessor;
import org.hypertrace.gateway.service.common.transformer.ResponsePostProcessor;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

public class ExploreService {
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

  private final RequestHandler normalRequestHandler;
  private final TimeAggregationsRequestHandler timeAggregationsRequestHandler;
  private final TimeAggregationsWithGroupByRequestHandler timeAggregationsWithGroupByRequestHandler;
  private final ScopeFilterConfigs scopeFilterConfigs;
  // Request/Response transformers
  private final RequestPreProcessor requestPreProcessor;
  private final ResponsePostProcessor responsePostProcessor;

  private Timer queryExecutionTimer;

  public ExploreService(
      QueryServiceClient queryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFiltersConfig) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.normalRequestHandler = new RequestHandler(queryServiceClient, attributeMetadataProvider);
    this.timeAggregationsRequestHandler =
        new TimeAggregationsRequestHandler(queryServiceClient, attributeMetadataProvider);
    this.timeAggregationsWithGroupByRequestHandler =
        new TimeAggregationsWithGroupByRequestHandler(
            queryServiceClient, attributeMetadataProvider);
    this.scopeFilterConfigs = scopeFiltersConfig;
    this.requestPreProcessor = new RequestPreProcessor(attributeMetadataProvider);
    this.responsePostProcessor = new ResponsePostProcessor(attributeMetadataProvider);
    initMetrics();
  }

  private void initMetrics() {
    queryExecutionTimer = new Timer();
    PlatformMetricsRegistry.register("explore.query.execution", queryExecutionTimer);
  }

  public ExploreResponse explore(
      String tenantId, ExploreRequest request, Map<String, String> requestHeaders) {
    final Context timerContext = queryExecutionTimer.time();
    try {
      ExploreRequestContext exploreRequestContext =
          new ExploreRequestContext(tenantId, request, requestHeaders);
      ExploreRequest preProcessedRequest = requestPreProcessor.transform(request, exploreRequestContext);
      ExploreRequestContext preProcessedRequestContext = new ExploreRequestContext(tenantId, preProcessedRequest, requestHeaders);

      AttributeScope requestScope = AttributeScope.valueOf(preProcessedRequest.getContext());

      // Add extra filters based on the scope.
      preProcessedRequest =
          ExploreRequest.newBuilder(preProcessedRequest)
              .setFilter(
                  scopeFilterConfigs.createScopeFilter(
                      requestScope,
                      preProcessedRequest.getFilter(),
                      attributeMetadataProvider,
                      preProcessedRequestContext))
              .build();
      ExploreRequestContext newExploreRequestContext =
          new ExploreRequestContext(tenantId, preProcessedRequest, requestHeaders);

      Map<String, AttributeMetadata> attributeMetadataMap =
          attributeMetadataProvider.getAttributesMetadata(newExploreRequestContext, requestScope);
      exploreRequestValidator.validate(preProcessedRequest, attributeMetadataMap);

      IRequestHandler requestHandler = getRequestHandler(preProcessedRequest);

      ExploreResponse.Builder responseBuilder =
          requestHandler.handleRequest(newExploreRequestContext, request);

      responseBuilder = responsePostProcessor.transform(request, exploreRequestContext, responseBuilder);
      return responseBuilder.build();
    } finally {
      timerContext.stop();
    }
  }

  private IRequestHandler getRequestHandler(ExploreRequest request) {
    if (hasTimeAggregationsAndGroupBy(request)) {
      return timeAggregationsWithGroupByRequestHandler;
    } else if (hasTimeAggregations(request)) {
      return timeAggregationsRequestHandler;
    } else {
      return normalRequestHandler;
    }
  }

  private boolean hasTimeAggregations(ExploreRequest request) {
    return !request.getTimeAggregationList().isEmpty();
  }

  private boolean hasTimeAggregationsAndGroupBy(ExploreRequest request) {
    return !request.getTimeAggregationList().isEmpty() && !request.getGroupByList().isEmpty();
  }
}
