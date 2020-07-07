package org.hypertrace.gateway.service.explore;

import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

public class ExploreService {
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

  private final RequestHandler normalRequestHandler;
  private final TimeAggregationsRequestHandler timeAggregationsRequestHandler;
  private final TimeAggregationsWithGroupByRequestHandler timeAggregationsWithGroupByRequestHandler;
  private final ScopeFilterConfigs scopeFilterConfigs;

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
  }

  public ExploreResponse explore(
      String tenantId, ExploreRequest request, Map<String, String> requestHeaders) {
    ExploreRequestContext exploreRequestContext =
        new ExploreRequestContext(tenantId, request, requestHeaders);
    AttributeScope requestScope = AttributeScope.valueOf(request.getContext());

    // Add extra filters based on the scope.
    request =
        ExploreRequest.newBuilder(request)
            .setFilter(
                scopeFilterConfigs.createScopeFilter(
                    requestScope,
                    request.getFilter(),
                    attributeMetadataProvider,
                    exploreRequestContext))
            .build();
    ExploreRequestContext newExploreRequestContext =
        new ExploreRequestContext(tenantId, request, requestHeaders);

    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(newExploreRequestContext, requestScope);
    exploreRequestValidator.validate(request, attributeMetadataMap);

    IRequestHandler requestHandler = getRequestHandler(request);

    ExploreResponse.Builder responseBuilder =
        requestHandler.handleRequest(newExploreRequestContext, request);
    return responseBuilder.build();
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
