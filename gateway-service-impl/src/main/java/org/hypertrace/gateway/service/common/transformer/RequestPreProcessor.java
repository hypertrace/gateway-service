package org.hypertrace.gateway.service.common.transformer;

import java.util.stream.Collectors;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.trace.TraceScope;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre processes the incoming Request by adding extra filters that are defined in the scope filters
 * config
 */
public class RequestPreProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(RequestPreProcessor.class);
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final ScopeFilterConfigs scopeFilterConfigs;

  public RequestPreProcessor(
      AttributeMetadataProvider attributeMetadataProvider, ScopeFilterConfigs scopeFilterConfigs) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.scopeFilterConfigs = scopeFilterConfigs;
  }

  /**
   * This is called once before processing the request.
   *
   * @param originalRequest The original request received
   * @return The modified request with unique selections and any additional filters depending on the
   *     scope config
   */
  public EntitiesRequest process(EntitiesRequest originalRequest, EntitiesRequestContext context) {
    EntitiesRequest.Builder entitiesRequestBuilder = EntitiesRequest.newBuilder(originalRequest);
    // Add any additional filters that maybe defined in the scope filters config
    Filter filter =
        scopeFilterConfigs.createScopeFilter(
            originalRequest.getEntityType(),
            originalRequest.getFilter(),
            attributeMetadataProvider,
            context);

    return entitiesRequestBuilder
        .setFilter(filter)
        .clearSelection()
        .addAllSelection(
            originalRequest.getSelectionList().stream().distinct().collect(Collectors.toList()))
        .build();
  }

  /**
   * This is called once before processing the request.
   *
   * @param originalRequest The original request received
   * @return The modified request with any additional filters in scope filters config
   */
  public TracesRequest transformFilter(
      TracesRequest originalRequest, RequestContext requestContext) {
    TracesRequest.Builder tracesRequestBuilder = TracesRequest.newBuilder(originalRequest);

    // Add any additional filters that maybe defined in the scope filters config
    TraceScope scope = TraceScope.valueOf(originalRequest.getScope());
    Filter filter =
        scopeFilterConfigs.createScopeFilter(
            scope.name(), originalRequest.getFilter(), attributeMetadataProvider, requestContext);
    tracesRequestBuilder.setFilter(filter);

    return tracesRequestBuilder.build();
  }
}
