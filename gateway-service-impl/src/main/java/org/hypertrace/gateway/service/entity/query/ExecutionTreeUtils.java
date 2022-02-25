package org.hypertrace.gateway.service.entity.query;

import static java.util.function.Predicate.not;
import static org.hypertrace.gateway.service.common.util.ExpressionReader.buildAttributeToSourcesMap;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.gateway.service.common.ExpressionContext;

public class ExecutionTreeUtils {
  /**
   * Returns a non-empty optional if all the attributes in the selection(attributes and
   * aggregations), time aggregations, filter and order by can be read from the same source.
   *
   * @param expressionContext
   * @return
   */
  public static Optional<String> getSingleSourceForAllAttributes(
      ExpressionContext expressionContext) {
    Optional<String> singleSourceFromKeySets = getSingleSourceFromKeySets(expressionContext);

    return singleSourceFromKeySets.or(
        () -> getSingleSourceFromAttributeSourceValueSets(expressionContext));
  }

  /**
   * Returns a non-empty optional if the size union of all keysets is equal to 1. This means that
   * all the attributes can be read from one source.
   *
   * @param expressionContext
   * @return
   */
  private static Optional<String> getSingleSourceFromKeySets(ExpressionContext expressionContext) {
    Set<String> selectionsSourceSet =
        expressionContext.getSourceToSelectionExpressionMap().keySet();
    Set<String> metricAggregationsSourceSet =
        expressionContext.getSourceToMetricExpressionMap().keySet();
    Set<String> timeAggregationsSourceSet =
        expressionContext.getSourceToTimeAggregationMap().keySet();
    Set<String> filtersSourceSet = expressionContext.getSourceToFilterExpressionMap().keySet();
    Set<String> groupBysSourceSet = expressionContext.getSourceToGroupByExpressionMap().keySet();
    Set<String> selectionOrderBysSourceSet =
        expressionContext.getSourceToSelectionOrderByExpressionMap().keySet();
    Set<String> metricAggregationOrderBysSourceSet =
        expressionContext.getSourceToMetricOrderByExpressionMap().keySet();

    Set<String> sources = new HashSet<>();
    sources.addAll(selectionsSourceSet);
    sources.addAll(metricAggregationsSourceSet);
    sources.addAll(timeAggregationsSourceSet);
    sources.addAll(filtersSourceSet);
    sources.addAll(groupBysSourceSet);
    sources.addAll(selectionOrderBysSourceSet);
    sources.addAll(metricAggregationOrderBysSourceSet);
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
   *
   * @param expressionContext
   * @return
   */
  private static Optional<String> getSingleSourceFromAttributeSourceValueSets(
      ExpressionContext expressionContext) {
    // Compute the intersection of all sources in attributesToSourcesMap and check if it's size is 1
    Set<String> attributeSourcesIntersection =
        expressionContext.getAllAttributesToSourcesMap().values().stream()
            .filter(not(Set::isEmpty))
            .findFirst()
            .orElse(Collections.emptySet());

    if (attributeSourcesIntersection.isEmpty()) {
      return Optional.empty();
    }

    attributeSourcesIntersection = new HashSet<>(attributeSourcesIntersection);

    for (Set<String> attributeSourcesSet :
        expressionContext.getAllAttributesToSourcesMap().values()) {
      // retainAll() for sets computes the intersections.
      attributeSourcesIntersection.retainAll(attributeSourcesSet);
    }

    if (attributeSourcesIntersection.size() == 1) {
      return attributeSourcesIntersection.stream().findFirst();
    } else {
      return Optional.empty();
    }
  }

  public static boolean areFiltersOnlyOnCurrentDataSource(
      ExpressionContext expressionContext, String currentSource) {
    Map<String, Set<String>> sourceToFilterAttributeMap =
        expressionContext.getSourceToFilterAttributeMap();

    if (sourceToFilterAttributeMap.isEmpty()) {
      return true;
    }

    Map<String, Set<String>> filterAttributeToSourcesMap =
        expressionContext.getFilterAttributeToSourceMap();

    // all the filter attribute sources should contain current source
    return filterAttributeToSourcesMap.values().stream()
        .allMatch(sources -> sources.contains(currentSource));
  }

  /**
   * Computes common set of sources, if both filters and order bys are requested on the same source
   * sets
   *
   * <p>Look at {@link ExecutionTreeUtils#getIntersectingSourceSets(java.util.Map, java.util.Map)}
   */
  public static Set<String> getSourceSetsIfFilterAndOrderByAreFromSameSourceSets(
      ExpressionContext expressionContext) {

    Map<String, Set<String>> sourceToFilterAttributeMap =
        expressionContext.getSourceToFilterAttributeMap();
    Map<String, Set<String>> sourceToSelectionOrderByAttributeMap =
        expressionContext.getSourceToSelectionOrderByAttributeMap();
    Map<String, Set<String>> sourceToMetricOrderByAttributeMap =
        expressionContext.getSourceToMetricOrderByAttributeMap();

    // merges sourceToSelectionOrderByAttributeMap and sourceToMetricOrderByAttributeMap
    Map<String, Set<String>> sourceToOrderByAttributeMap =
        Stream.concat(
                sourceToSelectionOrderByAttributeMap.entrySet().stream(),
                sourceToMetricOrderByAttributeMap.entrySet().stream())
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (v1, v2) -> {
                      Set<String> mergedSet = new HashSet<>(v1);
                      mergedSet.addAll(v2);
                      return mergedSet;
                    },
                    HashMap::new));

    // A weird case, if there are no filters and order bys
    if (sourceToFilterAttributeMap.isEmpty() && sourceToOrderByAttributeMap.isEmpty()) {
      return Collections.emptySet();
    }

    Map<String, Set<String>> filterAttributeToSourcesMap =
        expressionContext.getFilterAttributeToSourceMap();
    Map<String, Set<String>> orderByAttributeToSourceMap =
        buildAttributeToSourcesMap(sourceToOrderByAttributeMap);

    return getIntersectingSourceSets(filterAttributeToSourcesMap, orderByAttributeToSourceMap);
  }

  /**
   * Computes intersecting source sets from 2 attribute to sources map i.e. computes intersection
   * source sets across all attributes from the map
   *
   * <pre>
   * Examples:
   *
   * 1.
   * ("API.id" -> ["EDS", "QS"], "API.name" -> ["QS", "EDS"])
   * ("API.id" -> ["EDS", "QS"], "API.discoveryState" -> ["EDS"])
   *
   * The intersecting source set across all the attributes would be ["EDS"]
   *
   * 2.
   * ("API.id" -> ["EDS", "QS"], "API.name" -> ["QS", "EDS"])
   * ("API.id" -> ["EDS", "QS"], "API.discoveryState" -> ["EDS", "QS"])
   *
   * The intersecting source set across all the attributes would be ["EDS", "QS"]
   *
   * 3.
   * ("API.id" -> ["EDS"], "API.name" -> ["EDS"])
   * ("API.id" -> ["EDS"], "API.discoveryState" -> ["QS"])
   *
   * The intersecting source set across all the attributes would be []
   *
   * </pre>
   */
  private static Set<String> getIntersectingSourceSets(
      Map<String, Set<String>> attributeToSourcesMapFirst,
      Map<String, Set<String>> attributeToSourcesMapSecond) {
    if (attributeToSourcesMapFirst.isEmpty() && attributeToSourcesMapSecond.isEmpty()) {
      return Collections.emptySet();
    }

    if (attributeToSourcesMapFirst.isEmpty()) {
      return getIntersectingSourceSets(attributeToSourcesMapSecond);
    }

    if (attributeToSourcesMapSecond.isEmpty()) {
      return getIntersectingSourceSets(attributeToSourcesMapFirst);
    }

    Set<String> intersectingSourceSetFirst = getIntersectingSourceSets(attributeToSourcesMapFirst);
    Set<String> intersectingSourceSetSecond =
        getIntersectingSourceSets(attributeToSourcesMapSecond);

    return Sets.intersection(intersectingSourceSetFirst, intersectingSourceSetSecond);
  }

  /**
   * Computes source sets intersection from attribute to sources map
   *
   * <pre>
   * Examples:
   * ("API.id" -> ["EDS", "QS], "API.name" -> ["QS", "EDS]) => ["QS", "EDS]
   * ("API.id" -> ["EDS", "QS], "API.name" -> ["QS"]) => ["QS"]
   * ("API.id" -> ["EDS"], "API.name" -> ["QS]) => []
   * </pre>
   */
  private static Set<String> getIntersectingSourceSets(
      Map<String, Set<String>> attributeToSourcesMap) {
    if (attributeToSourcesMap.isEmpty()) {
      return Collections.emptySet();
    }

    return attributeToSourcesMap.values().stream()
        .reduce(Sets::intersection)
        .orElse(Collections.emptySet());
  }

  /**
   * Removes duplicate selection attributes from other sources using {@param pivotSource} as the
   * pivot source
   */
  public static void removeDuplicateSelectionAttributes(
      EntityExecutionContext executionContext, String pivotSource) {
    ExpressionContext expressionContext = executionContext.getExpressionContext();
    if (!expressionContext.getSourceToSelectionAttributeMap().containsKey(pivotSource)) {
      return;
    }

    Map<String, Set<String>> sourceToSelectionAttributeMap =
        Map.copyOf(expressionContext.getSourceToSelectionAttributeMap());

    Set<String> fetchedAttributes = sourceToSelectionAttributeMap.get(pivotSource);

    for (Map.Entry<String, Set<String>> entry : sourceToSelectionAttributeMap.entrySet()) {
      String sourceKey = entry.getKey();
      if (sourceKey.equals(pivotSource)) {
        continue;
      }

      Set<String> duplicateAttributes = Sets.intersection(fetchedAttributes, entry.getValue());
      executionContext.removeSelectionAttributes(sourceKey, duplicateAttributes);
    }
  }
}
