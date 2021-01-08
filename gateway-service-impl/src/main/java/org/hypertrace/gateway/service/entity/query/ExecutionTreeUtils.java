package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
    Set<String> selectionOrderBysSourceSet = executionContext.getSourceToSelectionOrderByExpressionMap().keySet();
    Set<String> metricAggregationOrderBysSourceSet = executionContext.getSourceToMetricOrderByExpressionMap().keySet();

    Set<String> sources = new HashSet<>();
    sources.addAll(selectionsSourceSet);
    sources.addAll(metricAggregationsSourceSet);
    sources.addAll(timeAggregationsSourceSet);
    sources.addAll(filtersSourceSet);
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
}
