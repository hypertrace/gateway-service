package org.hypertrace.gateway.service.common.util;

import static java.util.function.Predicate.not;

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.entity.query.ExecutionTreeUtils;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;

public class ExpressionReader {
  public static List<Expression> getFunctionExpressions(List<Expression> expressions) {
    return expressions.stream()
        .filter(expression -> expression.getValueCase() == Expression.ValueCase.FUNCTION)
        .collect(Collectors.toUnmodifiableList());
  }

  public static List<Expression> getAttributeExpressions(List<Expression> expressions) {
    return expressions.stream()
        .filter(ExpressionReader::isAttributeSelection)
        .collect(Collectors.toUnmodifiableList());
  }

  public static Set<String> extractAttributeIds(Expression expression) {
    Set<String> columns = new HashSet<>();
    extractAttributeIds(columns, expression);
    return Collections.unmodifiableSet(columns);
  }

  private static void extractAttributeIds(Set<String> columns, Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        String columnName = expression.getColumnIdentifier().getColumnName();
        columns.add(columnName);
        break;
      case ATTRIBUTE_EXPRESSION:
        columns.add(expression.getAttributeExpression().getAttributeId());
        break;
      case FUNCTION:
        for (Expression exp : expression.getFunction().getArgumentsList()) {
          extractAttributeIds(columns, exp);
        }
        break;
      case ORDERBY:
        extractAttributeIds(columns, expression.getOrderBy().getExpression());
        break;
      case LITERAL:
      case VALUE_NOT_SET:
        break;
    }
  }

  public static Optional<String> getAttributeIdFromAttributeSelection(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return Optional.of(expression.getColumnIdentifier().getColumnName());
      case ATTRIBUTE_EXPRESSION:
        return Optional.of(expression.getAttributeExpression().getAttributeId());
      case FUNCTION:
        return getAttributeIdFromAttributeSelection(expression.getFunction());
      default:
        return Optional.empty();
    }
  }

  public static Optional<String> getAttributeIdFromAttributeSelection(
      FunctionExpression functionExpression) {
    return functionExpression.getArgumentsList().stream()
        .map(ExpressionReader::getAttributeIdFromAttributeSelection)
        .flatMap(Optional::stream)
        .findFirst();
  }

  public static boolean isSimpleAttributeSelection(Expression expression) {
    switch (expression.getValueCase()) {
      case ATTRIBUTE_EXPRESSION:
        return !expression.getAttributeExpression().hasSubpath();
      case COLUMNIDENTIFIER:
        return true;
      default:
        return false;
    }
  }

  public static boolean isAttributeSelection(Expression expression) {
    switch (expression.getValueCase()) {
      case ATTRIBUTE_EXPRESSION:
      case COLUMNIDENTIFIER:
        return true;
      default:
        return false;
    }
  }

  public static Optional<String> getSelectionResultName(Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        return Optional.of(
            expression.getColumnIdentifier().getAlias().isEmpty()
                ? expression.getColumnIdentifier().getColumnName()
                : expression.getColumnIdentifier().getAlias());
      case ATTRIBUTE_EXPRESSION:
        return Optional.of(
            expression.getAttributeExpression().getAlias().isEmpty()
                ? expression.getAttributeExpression().getAttributeId()
                : expression.getAttributeExpression().getAlias());
      case FUNCTION:
        FunctionExpression functionExpression = expression.getFunction();
        if (!functionExpression.getAlias().isEmpty()) {
          return Optional.of(functionExpression.getAlias());
        }
        String argumentString =
            functionExpression.getArgumentsList().stream()
                .map(ExpressionReader::getSelectionResultName)
                .flatMap(Optional::stream)
                .collect(Collectors.joining(","));

        return Optional.of(
            String.format("%s_%s", functionExpression.getFunction(), argumentString));
      default:
        return Optional.empty();
    }
  }

  /**
   * Given a source to attributes, builds an attribute to sources map. Basically, a reverse map of
   * the map provided as input
   *
   * <p>Example:
   *
   * <p>("QS" -> API.id, "QS" -> API.name, "EDS" -> API.id) =>
   *
   * <p>("API.id" -> ["QS", "EDS"], "API.name" -> "QS")
   */
  public static Map<String, Set<String>> buildAttributeToSourcesMap(
      Map<String, Set<String>> sourcesToAttributeMap) {
    Map<String, Set<String>> attributeToSourcesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : sourcesToAttributeMap.entrySet()) {
      String source = entry.getKey();
      for (String attribute : entry.getValue()) {
        attributeToSourcesMap.computeIfAbsent(attribute, k -> new HashSet<>()).add(source);
      }
    }
    return Collections.unmodifiableMap(attributeToSourcesMap);
  }

  public static Map<String, List<String>> getExpectedResultNamesForEachAttributeId(
      Collection<Expression> expressionList, Collection<String> attributeIds) {
    return Map.copyOf(
        expressionList.stream()
            .filter(ExpressionReader::isSimpleAttributeSelection)
            .filter(
                attributeSelection ->
                    attributeIds.contains(
                        getAttributeIdFromAttributeSelection(attributeSelection).orElseThrow()))
            .collect(
                Collectors.groupingBy(
                    expression -> getAttributeIdFromAttributeSelection(expression).orElseThrow(),
                    Collectors.mapping(
                        expression -> getSelectionResultName(expression).orElseThrow(),
                        Collectors.toUnmodifiableList()))));
  }

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
   * <p>Look at {@link ExecutionTreeUtils#getIntersectingSourceSets(Map, Map)}
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
}
