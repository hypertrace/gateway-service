package org.hypertrace.gateway.service.common.transformer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.util.TimeRangeFilterUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.trace.TraceScope;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
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

  public RequestPreProcessor(AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFilterConfigs) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.scopeFilterConfigs = scopeFilterConfigs;
  }

  /**
   * This is called once before processing the request.
   *
   * @param originalRequest The original request received
   * @return The modified request with any additional filters depending on the scope config
   */
  public EntitiesRequest transformFilter(
      EntitiesRequest originalRequest, EntitiesRequestContext context) {
    EntitiesRequest.Builder entitiesRequestBuilder = EntitiesRequest.newBuilder(originalRequest);
    // Add any additional filters that maybe defined in the scope filters config
    Filter filter = scopeFilterConfigs.createScopeFilter(
        originalRequest.getEntityType(),
        originalRequest.getFilter(),
        attributeMetadataProvider,
        context);

    return entitiesRequestBuilder
        .clearSelection()
        .setFilter(filter)
        // Clean out duplicate columns in selections
        .addAllSelection(getUniqueSelections(originalRequest.getSelectionList()))
        .build();
  }

  /**
   * This is called once before processing the request.
   *
   * @param originalRequest The original request received
   * @return The modified request with any additional filters in scope filters config
   */
  public TracesRequest transformFilter(TracesRequest originalRequest, RequestContext requestContext) {
    TracesRequest.Builder tracesRequestBuilder = TracesRequest.newBuilder(originalRequest);

    // Add any additional filters that maybe defined in the scope filters config
    TraceScope scope = TraceScope.valueOf(originalRequest.getScope());
    Filter filter = scopeFilterConfigs.createScopeFilter(
        scope.name(),
        originalRequest.getFilter(),
        attributeMetadataProvider,
        requestContext);
    tracesRequestBuilder.setFilter(filter);

    return tracesRequestBuilder.build();
  }

  private List<Expression> getUniqueSelections(List<Expression> expressions) {
    List<Expression> selections = new ArrayList<>();
    Set<String> uniqueColumnNames = new HashSet<>();

    for (Expression expression : expressions) {
      // unique columns only
      if (expression.getValueCase() == ValueCase.COLUMNIDENTIFIER) {
        String columnName = expression.getColumnIdentifier().getColumnName();
        if (!uniqueColumnNames.contains(columnName)) {
          uniqueColumnNames.add(columnName);
          selections.add(expression);
        }
      } else {
        selections.add(expression);
      }
    }

    return selections;
  }
}
