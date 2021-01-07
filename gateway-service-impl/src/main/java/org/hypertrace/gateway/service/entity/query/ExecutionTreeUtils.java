package org.hypertrace.gateway.service.entity.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;

public class ExecutionTreeUtils {
  /**
   * Returns a non-empty optional if all the attributes in the selection(attributes and aggreagtions),
   * time aggregations, filter and order by can be read from the same source.
   * @param executionContext
   * @return
   */
  static Optional<String> getSingleSourceForAllAttributes(ExecutionContext executionContext) {
    Optional<String> singleSourceFromKeySets = getSingleSourceFromKeySets(executionContext);
    if (singleSourceFromKeySets.isPresent()) {
      return singleSourceFromKeySets;
    }

    return getSingleSourceFromAttributeSourceValueSets(executionContext);
  }

  /**
   * Returns a non-empty optional if the size union of all keysets is equal to 1. This means that all
   * the attributes can be read from one source.
   * @param executionContext
   * @return
   */
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
    Set<String> attributeSourcesIntersection = executionContext.getAllAttributesToSourcesMap().values().stream()
        .filter((sourcesSet) -> !sourcesSet.isEmpty())
        .findFirst()
        .orElse(Set.of());

    if (attributeSourcesIntersection.isEmpty()) {
      return Optional.empty();
    }

    attributeSourcesIntersection = new HashSet<>(attributeSourcesIntersection);

    for (Set<String> attributeSourcesSet : executionContext.getAllAttributesToSourcesMap().values()) {
      // retainAll() for sets computes the intersections.
      attributeSourcesIntersection.retainAll(attributeSourcesSet);
    }

    if (attributeSourcesIntersection.size() == 1) {
      return attributeSourcesIntersection.stream().findFirst();
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns true if the filter will AND on the Entity Id EQ filter. Will work as well for multiple
   * expressions, read from executionContext entity id expressions, that compose the Entity Id.
   * @param executionContext
   * @return
   */
  static boolean hasEntityIdEqualsFilter(ExecutionContext executionContext) {
    Filter filter = executionContext.getEntitiesRequest().getFilter();
    if (filter.equals(Filter.getDefaultInstance())) {
      return false;
    }

    List<Expression> entityIdExpressionList = executionContext.getEntityIdExpressions();
    // No known entity Ids
    if (entityIdExpressionList.size() == 0) {
      return false;
    }

    // Simple EQ filter without ANDs that is the equality filter. Only works when there's only one
    // entityId column. If there were multiple, then this would be an AND filter.
    if (entityIdExpressionList.size() == 1 && simpleFilterEntityIdEqualsFilter(filter, entityIdExpressionList)){
      return true;
    }

    if (filter.getOperator() == Operator.AND && filter.getChildFilterCount() == 0) {
      return false;
    }

    // OR or NOT operator in the filter means that highly likely this is not a straight equality
    // filter.
    if (containsNotOrOrFilter(filter)) {
      return false;
    }

    return hasEntityIdEqualsFilter(filter, entityIdExpressionList);
  }

  private static boolean hasEntityIdEqualsFilter(Filter filter,
                                                 List<Expression> entityIdExpressionList) {
    Operator operator = filter.getOperator();

    if (operator != Operator.AND) {
      return false;
    }

    if (filter.getChildFilterCount() == 0) {
      return false;
    }

    List<Filter> childFilters = filter.getChildFilterList();
    int entityIdsMatched = 0;

    for (Filter childFilter : childFilters) {
      Operator childFilterOperator = childFilter.getOperator();
      if (childFilterOperator == Operator.AND) {
        if (hasEntityIdEqualsFilter(childFilter, entityIdExpressionList)) {
          return true;
        }
      } else if (simpleFilterEntityIdEqualsFilter(childFilter, entityIdExpressionList)) {
        entityIdsMatched++;
      }
    }

    return entityIdsMatched == entityIdExpressionList.size();
  }

  private static boolean simpleFilterEntityIdEqualsFilter(Filter filter,
                                                          List<Expression> entityIdExpressionList) {
    if (filter.getOperator() == Operator.EQ && filter.hasLhs() && filter.getLhs().hasColumnIdentifier()) {
      for (Expression entityIdExpression : entityIdExpressionList) {
        if (filter.getLhs().getColumnIdentifier().getColumnName().equals(entityIdExpression.getColumnIdentifier().getColumnName())) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean containsNotOrOrFilter(Filter filter) {
    Operator operator = filter.getOperator();
    if (operator == Operator.OR || operator == Operator.NOT) {
      return true;
    }

    if (filter.getChildFilterCount() == 0) {
      return false;
    }

    for (Filter childFilter : filter.getChildFilterList()) {
      if (containsNotOrOrFilter(childFilter)) {
        return true;
      }
    }

    return false;
  }

  /**
   * @param executionContext
   * @param sourcesForFetchedAttributes set of sources from which the attributes have already been
   *     fetched
   * @param attributeSelectionSources current set of attribute selection sources to fetch attributes
   * @return set of sources for which the attributes have to be fetched
   *
   * Removes the attribute selection source from {@param attributeSelectionSources}, if the attribute has already been
   * requested from a different source
   *
   * Example: select api.id, api.name
   * api.id -> ["QS", "EDS"] api.name -> ["QS", EDS"]
   *
   * If api.id, api.name has already been fetched from QS, there is no point fetching
   * the same set of attributes from EDS
   *
   * Algorithm:
   * - Generate a set of the attributes(say A) fetched from the above collected sources
   * from {@param sourcesForFetchedAttributes}
   *
   * - for each attribute selection source S from {@param attributeSelectionSources},
   * get all the selection attributes for the source
   * - if the selection attributes for that source are already present in the set A,
   * then this source is redundant to fetch the attributes. Ignore this source
   * - return the remaining set of sources
   */
  public static Set<String> getPendingAttributeSelectionSources(
      ExecutionContext executionContext,
      Set<String> sourcesForFetchedAttributes,
      Set<String> attributeSelectionSources) {
    if (attributeSelectionSources.isEmpty()) {
      return attributeSelectionSources;
    }

    // map of selection attributes to sources map
    Map<String, Set<String>> attributeSelectionToSourceMap =
        ExecutionTreeUtils.buildAttributeToSourcesMap(
            executionContext.getSourceToSelectionExpressionMap());

    // set of attributes which were fetched from child attribute sources
    Set<String> attributesFromChildAttributeSources =
        attributeSelectionToSourceMap.entrySet().stream()
            .filter(
                entry ->
                    !Sets.intersection(entry.getValue(), sourcesForFetchedAttributes).isEmpty())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

    // reverse of attributeSelectionToSourceMap
    // map of source to attribute selection map
    Map<String, Set<String>> sourceToAttributeSelectionMap =
        buildSourceToAttributesMap(executionContext.getSourceToSelectionExpressionMap());

    Set<String> retainedAttributeSelectionSources = new HashSet<>();
    for (String source : attributeSelectionSources) {
      Set<String> selectionAttributesFromSource = sourceToAttributeSelectionMap.get(source);
      // if all the attributes from the selection source have already been fetched,
      // remove the source from selection node, so that it does not fetch the same
      // set of attributes again
      if (!attributesFromChildAttributeSources.containsAll(selectionAttributesFromSource)) {
        retainedAttributeSelectionSources.add(source);
      }
    }

    // return the set of sources for which the attributes have to be fetched
    return retainedAttributeSelectionSources;
  }

  private static Map<String, Set<String>> buildSourceToAttributesMap(
      Map<String, List<Expression>> sourceToExpressionMap) {
    return sourceToExpressionMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .map(ExpressionReader::extractColumns)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet())));
  }

  private static Map<String, Set<String>> buildAttributeToSourcesMap(
      Map<String, List<Expression>> sourceToExpressionMap) {
    Map<String, Set<String>> attributeToSourceMap = new HashMap<>();

    for (Map.Entry<String, List<Expression>> entry : sourceToExpressionMap.entrySet()) {
      String source = entry.getKey();
      List<Expression> expressions = entry.getValue();
      for (Expression expression : expressions) {
        Set<String> columnNames = ExpressionReader.extractColumns(expression);
        for (String columnName : columnNames) {
          attributeToSourceMap.computeIfAbsent(columnName, k -> new HashSet<>()).add(source);
        }
      }
    }
    return attributeToSourceMap;
  }
}
