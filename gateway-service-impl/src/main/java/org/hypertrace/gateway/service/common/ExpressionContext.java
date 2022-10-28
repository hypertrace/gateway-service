package org.hypertrace.gateway.service.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.OrderByUtil;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpressionContext {
  private static final Logger LOG = LoggerFactory.getLogger(ExpressionContext.class);

  private final Map<String, AttributeMetadata> attributeMetadataMap;

  // selections
  private final List<Expression> selections;
  private ImmutableMap<String, List<Expression>> sourceToSelectionExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToSelectionAttributeMap;
  private ImmutableMap<String, Set<String>> selectionAttributeToSourceMap;

  private ImmutableMap<String, List<Expression>> sourceToMetricAggregationExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToMetricAggregationAttributeMap;
  private ImmutableMap<String, Set<String>> metricAggregationAttributeToSourceMap;

  private final List<TimeAggregation> timeAggregations;
  private ImmutableMap<String, List<TimeAggregation>> sourceToTimeAggregationMap;
  private ImmutableMap<String, Set<String>> sourceToTimeAggregationAttributeMap;
  private ImmutableMap<String, Set<String>> timeAggregationAttributeToSourceMap;

  // order bys
  private final List<OrderByExpression> orderBys;
  private ImmutableMap<String, List<OrderByExpression>> sourceToSelectionOrderByExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToSelectionOrderByAttributeMap;
  private ImmutableMap<String, Set<String>> selectionOrderByAttributeToSourceMap;

  private ImmutableMap<String, List<OrderByExpression>> sourceToMetricOrderByExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToMetricOrderByAttributeMap;
  private ImmutableMap<String, Set<String>> metricOrderByAttributeToSourceMap;

  // filters
  private final Filter filter;
  private ImmutableMap<String, List<Expression>> sourceToFilterExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToFilterAttributeMap;
  private ImmutableMap<String, Set<String>> filterAttributeToSourceMap;

  // group bys
  private final List<Expression> groupBys;
  private ImmutableMap<String, List<Expression>> sourceToGroupByExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToGroupByAttributeMap;
  private ImmutableMap<String, Set<String>> groupByAttributeToSourceMap;

  public ExpressionContext(
      Map<String, AttributeMetadata> attributeMetadataMap,
      Filter filter,
      List<Expression> selections,
      List<TimeAggregation> timeAggregations,
      List<OrderByExpression> orderBys,
      List<Expression> groupBys) {
    this.attributeMetadataMap = attributeMetadataMap;

    this.selections = selections;
    this.timeAggregations = timeAggregations;
    this.filter = filter;
    this.orderBys = orderBys;
    this.groupBys = groupBys;

    buildSourceToSelectionExpressionMaps();
    buildSourceToFilterExpressionMaps();
    buildSourceToOrderByExpressionMaps();
    buildSourceToGroupByExpressionMaps();
  }

  public Map<String, List<Expression>> getSourceToSelectionExpressionMap() {
    return sourceToSelectionExpressionMap;
  }

  public void setSourceToSelectionExpressionMap(
      Map<String, List<Expression>> sourceToSelectionExpressionMap) {
    this.sourceToSelectionExpressionMap =
        ImmutableMap.<String, List<Expression>>builder()
            .putAll(sourceToSelectionExpressionMap)
            .build();
  }

  public Map<String, Set<String>> getSourceToSelectionAttributeMap() {
    return sourceToSelectionAttributeMap;
  }

  public void setSourceToSelectionAttributeMap(
      Map<String, List<Expression>> sourceToSelectionExpressionMap) {
    this.sourceToSelectionAttributeMap = buildSourceToAttributesMap(sourceToSelectionExpressionMap);
  }

  public Map<String, Set<String>> getSelectionAttributeToSourceMap() {
    return selectionAttributeToSourceMap;
  }

  public Map<String, List<Expression>> getSourceToMetricAggregationExpressionMap() {
    return sourceToMetricAggregationExpressionMap;
  }

  public Map<String, Set<String>> getMetricAggregationAttributeToSourceMap() {
    return metricAggregationAttributeToSourceMap;
  }

  public Map<String, List<TimeAggregation>> getSourceToTimeAggregationMap() {
    return sourceToTimeAggregationMap;
  }

  public Map<String, Set<String>> getTimeAggregationAttributeToSourceMap() {
    return timeAggregationAttributeToSourceMap;
  }

  public Map<String, List<OrderByExpression>> getSourceToSelectionOrderByExpressionMap() {
    return sourceToSelectionOrderByExpressionMap;
  }

  public Map<String, Set<String>> getSourceToSelectionOrderByAttributeMap() {
    return sourceToSelectionOrderByAttributeMap;
  }

  public ImmutableMap<String, Set<String>> getSelectionOrderByAttributeToSourceMap() {
    return selectionOrderByAttributeToSourceMap;
  }

  public Map<String, List<OrderByExpression>> getSourceToMetricOrderByExpressionMap() {
    return sourceToMetricOrderByExpressionMap;
  }

  public Map<String, Set<String>> getSourceToMetricOrderByAttributeMap() {
    return sourceToMetricOrderByAttributeMap;
  }

  public Map<String, Set<String>> getMetricOrderByAttributeToSourceMap() {
    return metricOrderByAttributeToSourceMap;
  }

  public Map<String, List<Expression>> getSourceToFilterExpressionMap() {
    return sourceToFilterExpressionMap;
  }

  public Map<String, Set<String>> getSourceToFilterAttributeMap() {
    return sourceToFilterAttributeMap;
  }

  public Map<String, Set<String>> getFilterAttributeToSourceMap() {
    return filterAttributeToSourceMap;
  }

  public Map<String, List<Expression>> getSourceToGroupByExpressionMap() {
    return sourceToGroupByExpressionMap;
  }

  public ImmutableMap<String, Set<String>> getGroupByAttributeToSourceMap() {
    return groupByAttributeToSourceMap;
  }

  private void buildSourceToSelectionExpressionMaps() {
    List<Expression> attributeSelections =
        selections.stream()
            .filter(ExpressionReader::isAttributeSelection)
            .collect(Collectors.toUnmodifiableList());
    sourceToSelectionExpressionMap = getDataSourceToExpressionMap(attributeSelections);
    sourceToSelectionAttributeMap = buildSourceToAttributesMap(sourceToSelectionExpressionMap);
    selectionAttributeToSourceMap = buildAttributeToSourcesMap(sourceToSelectionAttributeMap);

    List<Expression> functionSelections =
        selections.stream()
            .filter(Expression::hasFunction)
            .collect(Collectors.toUnmodifiableList());
    sourceToMetricAggregationExpressionMap = getDataSourceToExpressionMap(functionSelections);
    sourceToMetricAggregationAttributeMap =
        buildSourceToAttributesMap(sourceToMetricAggregationExpressionMap);
    metricAggregationAttributeToSourceMap =
        buildAttributeToSourcesMap(sourceToMetricAggregationAttributeMap);

    sourceToTimeAggregationMap = getDataSourceToTimeAggregation(timeAggregations);
    sourceToTimeAggregationAttributeMap =
        buildSourceToTimeAggregationAttributesMap(sourceToTimeAggregationMap);
    timeAggregationAttributeToSourceMap =
        buildAttributeToSourcesMap(sourceToTimeAggregationAttributeMap);
  }

  private void buildSourceToFilterExpressionMaps() {
    sourceToFilterExpressionMap = getSourceToFilterExpressionMap(filter);
    sourceToFilterAttributeMap = buildSourceToAttributesMap(sourceToFilterExpressionMap);
    filterAttributeToSourceMap = buildAttributeToSourcesMap(sourceToFilterAttributeMap);
  }

  private void buildSourceToOrderByExpressionMaps() {
    // Ensure that the OrderByExpression function alias matches that of a column in the selection or
    // TimeAggregation, since the OrderByComparator uses the alias to match the column name in the
    // QueryService results
    List<OrderByExpression> orderByExpressions =
        OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
            orderBys, selections, timeAggregations);

    List<OrderByExpression> attributeOrderByExpressions =
        orderByExpressions.stream()
            .filter(
                orderByExpression ->
                    ExpressionReader.isAttributeSelection(orderByExpression.getExpression()))
            .collect(Collectors.toUnmodifiableList());

    sourceToSelectionOrderByExpressionMap =
        getDataSourceToOrderByExpressionMap(attributeOrderByExpressions);
    sourceToSelectionOrderByAttributeMap =
        buildSourceToAttributesMap(
            convertOrderByExpressionToExpression(sourceToSelectionOrderByExpressionMap));
    selectionOrderByAttributeToSourceMap =
        buildAttributeToSourcesMap(sourceToSelectionOrderByAttributeMap);

    List<OrderByExpression> functionOrderByExpressions =
        orderByExpressions.stream()
            .filter(orderByExpression -> orderByExpression.getExpression().hasFunction())
            .collect(Collectors.toUnmodifiableList());

    sourceToMetricOrderByExpressionMap =
        getDataSourceToOrderByExpressionMap(functionOrderByExpressions);
    sourceToMetricOrderByAttributeMap =
        buildSourceToAttributesMap(
            convertOrderByExpressionToExpression(sourceToMetricOrderByExpressionMap));
    metricOrderByAttributeToSourceMap =
        buildAttributeToSourcesMap(sourceToMetricOrderByAttributeMap);
  }

  private void buildSourceToGroupByExpressionMaps() {
    sourceToGroupByExpressionMap = getDataSourceToExpressionMap(groupBys);
    sourceToGroupByAttributeMap = buildSourceToAttributesMap(sourceToGroupByExpressionMap);
    groupByAttributeToSourceMap = buildAttributeToSourcesMap(sourceToGroupByAttributeMap);
  }

  private ImmutableMap<String, List<OrderByExpression>> getDataSourceToOrderByExpressionMap(
      List<OrderByExpression> orderByExpressions) {
    Map<String, List<OrderByExpression>> result = new HashMap<>();
    for (OrderByExpression orderByExpression : orderByExpressions) {
      Expression expression = orderByExpression.getExpression();
      Map<String, List<Expression>> map =
          getDataSourceToExpressionMap(Collections.singletonList(expression));
      for (String source : map.keySet()) {
        result.computeIfAbsent(source, k -> new ArrayList<>()).add(orderByExpression);
      }
    }
    return ImmutableMap.<String, List<OrderByExpression>>builder().putAll(result).build();
  }

  private ImmutableMap<String, List<Expression>> getSourceToFilterExpressionMap(Filter filter) {
    Map<String, List<Expression>> sourceToExpressionMap = new HashMap<>();

    getSourceToFilterExpressionMap(filter, sourceToExpressionMap);
    return ImmutableMap.<String, List<Expression>>builder().putAll(sourceToExpressionMap).build();
  }

  private ImmutableMap<String, List<TimeAggregation>> getDataSourceToTimeAggregation(
      List<TimeAggregation> timeAggregations) {
    Map<String, List<TimeAggregation>> result = new HashMap<>();
    for (TimeAggregation timeAggregation : timeAggregations) {
      Map<String, List<Expression>> map =
          getDataSourceToExpressionMap(Collections.singletonList(timeAggregation.getAggregation()));
      // There should only be one element in the map.
      result
          .computeIfAbsent(map.keySet().iterator().next(), k -> new ArrayList<>())
          .add(timeAggregation);
    }
    return ImmutableMap.<String, List<TimeAggregation>>builder().putAll(result).build();
  }

  private void getSourceToFilterExpressionMap(
      Filter filter, Map<String, List<Expression>> sourceToFilterExpressionMap) {
    if (Filter.getDefaultInstance().equals(filter)) {
      return;
    }

    if (filter.hasLhs()) {
      // Assuming RHS never has columnar expressions.
      Expression lhs = filter.getLhs();
      Map<String, List<Expression>> sourceToExpressionMap =
          getDataSourceToExpressionMap(List.of(lhs));
      sourceToExpressionMap.forEach(
          (key, value) ->
              sourceToFilterExpressionMap.merge(
                  key,
                  value,
                  (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                  }));
    }

    if (filter.getChildFilterCount() > 0) {
      filter
          .getChildFilterList()
          .forEach(
              childFilter ->
                  getSourceToFilterExpressionMap(childFilter, sourceToFilterExpressionMap));
    }
  }

  private ImmutableMap<String, List<Expression>> getDataSourceToExpressionMap(
      List<Expression> expressions) {
    if (expressions == null || expressions.isEmpty()) {
      return ImmutableMap.of();
    }
    Map<String, List<Expression>> sourceToExpressionMap = new HashMap<>();
    for (Expression expression : expressions) {
      Set<String> attributeIds = ExpressionReader.extractAttributeIds(expression);
      Set<AttributeSource> sources =
          Arrays.stream(AttributeSource.values()).collect(Collectors.toSet());
      for (String attributeId : attributeIds) {
        List<AttributeSource> sourcesList = attributeMetadataMap.get(attributeId).getSourcesList();
        sources.retainAll(sourcesList);
      }
      if (sources.isEmpty()) {
        LOG.error("Skipping Expression: {}. No source found", expression);
        continue;
      }
      for (AttributeSource source : sources) {
        sourceToExpressionMap
            .computeIfAbsent(source.name(), v -> new ArrayList<>())
            .add(expression);
      }
    }
    return ImmutableMap.<String, List<Expression>>builder().putAll(sourceToExpressionMap).build();
  }

  /**
   * Given a source to expression map, builds a source to attribute map, where the attribute names
   * are extracted out as column names from the expression
   */
  private ImmutableMap<String, Set<String>> buildSourceToAttributesMap(
      Map<String, List<Expression>> sourceToExpressionMap) {
    return ImmutableMap.<String, Set<String>>builder()
        .putAll(
            sourceToExpressionMap.entrySet().stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry ->
                            entry.getValue().stream()
                                .map(ExpressionReader::extractAttributeIds)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toSet()))))
        .build();
  }

  /**
   * Given a source to time aggregation map, builds a source to time aggregation attribute map,
   * where the attribute names are extracted out as column names from the time aggregation
   */
  private ImmutableMap<String, Set<String>> buildSourceToTimeAggregationAttributesMap(
      Map<String, List<TimeAggregation>> sourceToTimeAggregationMap) {
    return buildSourceToAttributesMap(
        sourceToTimeAggregationMap.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    entry ->
                        entry.getValue().stream()
                            .map(TimeAggregation::getAggregation)
                            .collect(Collectors.toUnmodifiableList()))));
  }

  private Map<String, List<Expression>> convertOrderByExpressionToExpression(
      Map<String, List<OrderByExpression>> sourceToOrderByExpressions) {
    return sourceToOrderByExpressions.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry ->
                    entry.getValue().stream()
                        .map(OrderByExpression::getExpression)
                        .collect(Collectors.toList())));
  }

  /**
   * Returns a non-empty optional if all the attributes in the selection(attributes and
   * aggregations), time aggregations, filters, order bys and group bys can be read from the same
   * source.
   *
   * @param expressionContext
   * @return
   */
  public static Optional<String> getSingleSourceForAllAttributes(
      ExpressionContext expressionContext) {
    Set<ExpressionLocation> allLocations = Set.of(ExpressionLocation.values());
    Optional<String> singleSourceFromKeySets =
        getSingleSourceFromKeySets(expressionContext, allLocations);

    return singleSourceFromKeySets.or(
        () -> getSingleSourceFromAttributeSourceValueSets(expressionContext, allLocations));
  }

  /**
   * Returns a non-empty optional if all the attributes in the selection(attributes and
   * aggregations), time aggregations, filters, order bys and group bys can be read from the same
   * source, depending on the provided locations
   *
   * @param expressionContext
   * @return
   */
  public static Optional<String> getSingleSourceForAttributes(
      ExpressionContext expressionContext, Set<ExpressionLocation> locations) {
    Optional<String> singleSourceFromKeySets =
        getSingleSourceFromKeySets(expressionContext, locations);

    return singleSourceFromKeySets.or(
        () -> getSingleSourceFromAttributeSourceValueSets(expressionContext, locations));
  }

  /**
   * Returns a non-empty optional if the size union of all keysets is equal to 1. This means that
   * all the attributes can be read from one source.
   *
   * @param expressionContext
   * @return
   */
  private static Optional<String> getSingleSourceFromKeySets(
      ExpressionContext expressionContext, Set<ExpressionLocation> locations) {
    if (locations.isEmpty()) {
      return Optional.empty();
    }

    Set<String> sources = new HashSet<>();
    for (ExpressionLocation location : locations) {
      switch (location) {
        case COLUMN_SELECTION:
          Set<String> selectionsSourceSet =
              expressionContext.getSourceToSelectionExpressionMap().keySet();
          sources.addAll(selectionsSourceSet);
          continue;
        case METRIC_AGGREGATION:
          Set<String> metricAggregationsSourceSet =
              expressionContext.getSourceToMetricAggregationExpressionMap().keySet();
          sources.addAll(metricAggregationsSourceSet);
          continue;
        case TIME_AGGREGATION:
          Set<String> timeAggregationsSourceSet =
              expressionContext.getSourceToTimeAggregationMap().keySet();
          sources.addAll(timeAggregationsSourceSet);
          continue;
        case COLUMN_FILTER:
          Set<String> filtersSourceSet =
              expressionContext.getSourceToFilterExpressionMap().keySet();
          sources.addAll(filtersSourceSet);
          continue;
        case COLUMN_GROUP_BY:
          Set<String> groupBysSourceSet =
              expressionContext.getSourceToGroupByExpressionMap().keySet();
          sources.addAll(groupBysSourceSet);
          continue;
        case COLUMN_ORDER_BY:
          Set<String> selectionOrderBysSourceSet =
              expressionContext.getSourceToSelectionOrderByExpressionMap().keySet();
          sources.addAll(selectionOrderBysSourceSet);
          continue;
        case METRIC_ORDER_BY:
          Set<String> metricAggregationOrderBysSourceSet =
              expressionContext.getSourceToMetricOrderByExpressionMap().keySet();
          sources.addAll(metricAggregationOrderBysSourceSet);
          continue;
        default:
          LOG.error("Unrecognised expression location {}", location);
      }
    }
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
      ExpressionContext expressionContext, Set<ExpressionLocation> locations) {
    if (locations.isEmpty()) {
      return Optional.empty();
    }

    // retainAll() for sets computes the intersections.
    Set<String> attributeSourcesIntersection = new HashSet<>();
    for (ExpressionLocation location : locations) {
      switch (location) {
        case COLUMN_SELECTION:
          expressionContext
              .getSelectionAttributeToSourceMap()
              .values()
              .forEach(attributeSourcesIntersection::retainAll);
          continue;
        case METRIC_AGGREGATION:
          expressionContext
              .getMetricAggregationAttributeToSourceMap()
              .values()
              .forEach(attributeSourcesIntersection::retainAll);
          continue;
        case TIME_AGGREGATION:
          expressionContext
              .getTimeAggregationAttributeToSourceMap()
              .values()
              .forEach(attributeSourcesIntersection::retainAll);
          continue;
        case COLUMN_FILTER:
          expressionContext
              .getFilterAttributeToSourceMap()
              .values()
              .forEach(attributeSourcesIntersection::retainAll);
          continue;
        case COLUMN_GROUP_BY:
          expressionContext
              .getGroupByAttributeToSourceMap()
              .values()
              .forEach(attributeSourcesIntersection::retainAll);
          continue;
        case COLUMN_ORDER_BY:
          expressionContext
              .getSelectionOrderByAttributeToSourceMap()
              .values()
              .forEach(attributeSourcesIntersection::retainAll);
          continue;
        case METRIC_ORDER_BY:
          expressionContext
              .getMetricOrderByAttributeToSourceMap()
              .values()
              .forEach(attributeSourcesIntersection::retainAll);
          continue;
        default:
          LOG.error("Unrecognised expression location {}", location);
      }
    }
    attributeSourcesIntersection = new HashSet<>(attributeSourcesIntersection);

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
   * <p>Look at {@link ExpressionContext#getIntersectingSourceSets(Map, Map)}
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
   * Given a source to attributes, builds an attribute to sources map. Basically, a reverse map of
   * the map provided as input
   *
   * <p>Example:
   *
   * <p>("QS" -> API.id, "QS" -> API.name, "EDS" -> API.id) =>
   *
   * <p>("API.id" -> ["QS", "EDS"], "API.name" -> "QS")
   */
  private static ImmutableMap<String, Set<String>> buildAttributeToSourcesMap(
      Map<String, Set<String>> sourcesToAttributeMap) {
    Map<String, Set<String>> attributeToSourcesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : sourcesToAttributeMap.entrySet()) {
      String source = entry.getKey();
      for (String attribute : entry.getValue()) {
        attributeToSourcesMap.computeIfAbsent(attribute, k -> new HashSet<>()).add(source);
      }
    }
    return ImmutableMap.<String, Set<String>>builder().putAll(attributeToSourcesMap).build();
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

  @Override
  public String toString() {
    return "ExpressionContext{"
        + "attributeMetadataMap="
        + attributeMetadataMap
        + ", selections="
        + selections
        + ", sourceToSelectionExpressionMap="
        + sourceToSelectionExpressionMap
        + ", sourceToSelectionAttributeMap="
        + sourceToSelectionAttributeMap
        + ", selectionAttributeToSourceMap="
        + selectionAttributeToSourceMap
        + ", sourceToMetricAggregationExpressionMap="
        + sourceToMetricAggregationExpressionMap
        + ", sourceToMetricAggregationAttributeMap="
        + sourceToMetricAggregationAttributeMap
        + ", metricAggregationAttributeToSourceMap="
        + metricAggregationAttributeToSourceMap
        + ", timeAggregations="
        + timeAggregations
        + ", sourceToTimeAggregationMap="
        + sourceToTimeAggregationMap
        + ", sourceToTimeAggregationAttributeMap="
        + sourceToTimeAggregationAttributeMap
        + ", timeAggregationAttributeToSourceMap="
        + timeAggregationAttributeToSourceMap
        + ", orderBys="
        + orderBys
        + ", sourceToSelectionOrderByExpressionMap="
        + sourceToSelectionOrderByExpressionMap
        + ", sourceToSelectionOrderByAttributeMap="
        + sourceToSelectionOrderByAttributeMap
        + ", selectionOrderByAttributeToSourceMap="
        + selectionOrderByAttributeToSourceMap
        + ", sourceToMetricOrderByExpressionMap="
        + sourceToMetricOrderByExpressionMap
        + ", sourceToMetricOrderByAttributeMap="
        + sourceToMetricOrderByAttributeMap
        + ", metricOrderByAttributeToSourceMap="
        + metricOrderByAttributeToSourceMap
        + ", filter="
        + filter
        + ", sourceToFilterExpressionMap="
        + sourceToFilterExpressionMap
        + ", sourceToFilterAttributeMap="
        + sourceToFilterAttributeMap
        + ", filterAttributeToSourceMap="
        + filterAttributeToSourceMap
        + ", groupBys="
        + groupBys
        + ", sourceToGroupByExpressionMap="
        + sourceToGroupByExpressionMap
        + ", sourceToGroupByAttributeMap="
        + sourceToGroupByAttributeMap
        + ", groupByAttributeToSourceMap="
        + groupByAttributeToSourceMap
        + '}';
  }
}
