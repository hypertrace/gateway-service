package org.hypertrace.gateway.service.entity.query;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeSource;

public class ExecutionTreeUtils {
  static Optional<String> getSingleSourceForAllAttributes(ExecutionContext executionContext) {
    Optional<String> singleSourceFromKeySets = getSingleSourceFromKeySets(executionContext);
    if (singleSourceFromKeySets.isPresent()) {
      return singleSourceFromKeySets;
    }

    return getSingleSourceFromAttributeSourceValueSets(executionContext);
  }

  private static Optional<String> getSingleSourceFromKeySets(ExecutionContext executionContext) {
    Set<String> selectionsSourceSet = executionContext.getSourceToSelectionExpressionMap().keySet();
    Set<String> metricAggregationsSourceSet = executionContext.getSourceToMetricExpressionMap().keySet();
    Set<String> timeAggregationsSourceSet = executionContext.getSourceToTimeAggregationMap().keySet();
    Set<String> filtersSourceSet = executionContext.getSourceToFilterExpressionMap().keySet();
    Set<String> orderBysSourceSet = executionContext.getSourceToOrderByExpressionMap().keySet();

    Set<String> sources = new HashSet<>();
    sources.addAll(selectionsSourceSet);
    sources.addAll(metricAggregationsSourceSet);
    sources.addAll(timeAggregationsSourceSet);
    sources.addAll(filtersSourceSet);
    sources.addAll(orderBysSourceSet);
    if (sources.size() == 1) {
      return sources.stream().findFirst();
    } else {
      return Optional.empty();
    }
  }

  /**
   * Some attributes can be served by more than 1 source. eg. API.apiDiscoveryState. Check if it is
   * possible to serve all the attributes from a single source by computing the intersection of
   * their sources and checking if it's equal to 1.
   * @param executionContext
   * @return
   */
  private static Optional<String> getSingleSourceFromAttributeSourceValueSets(ExecutionContext executionContext) {
    // Compute the intersection of all sources in attributesToSourcesMap and check if it's size is 1
    Set<AttributeSource> attributeSourcesIntersection = executionContext.getAttributeToSourcesMap().values().stream()
        .filter((sourcesSet) -> !sourcesSet.isEmpty())
        .findFirst()
        .orElse(Set.of());

    if (attributeSourcesIntersection.isEmpty()) {
      return Optional.empty();
    }

    attributeSourcesIntersection = new HashSet<>(attributeSourcesIntersection);

    for (Set<AttributeSource> attributeSourcesSet : executionContext.getAttributeToSourcesMap().values()) {
      // retainAll() for sets computes the intersections.
      attributeSourcesIntersection.retainAll(attributeSourcesSet);
    }

    if (attributeSourcesIntersection.size() == 1) {
      return attributeSourcesIntersection.stream().map(Enum::name).findFirst();
    } else {
      return Optional.empty();
    }
  }
}
