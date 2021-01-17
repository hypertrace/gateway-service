package org.hypertrace.gateway.service.entity.query;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.OrderByUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.ValueCase;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context object constructed from the input query that is used at different stages in the query
 * execution
 */
public class ExecutionContext {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

  /** Following fields are immutable and set in the constructor * */
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;

  private final EntitiesRequest entitiesRequest;
  private final EntitiesRequestContext entitiesRequestContext;

  //selections
  private ImmutableMap<String, List<Expression>> sourceToSelectionExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToSelectionAttributeMap;

  private ImmutableMap<String, List<Expression>> sourceToMetricExpressionMap;
  private ImmutableMap<String, List<TimeAggregation>> sourceToTimeAggregationMap;

  // order bys
  private ImmutableMap<String, List<OrderByExpression>> sourceToSelectionOrderByExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToSelectionOrderByAttributeMap;

  private ImmutableMap<String, List<OrderByExpression>> sourceToMetricOrderByExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToMetricOrderByAttributeMap;

  // filters
  private ImmutableMap<String, List<Expression>> sourceToFilterExpressionMap;
  private ImmutableMap<String, Set<String>> sourceToFilterAttributeMap;
  private ImmutableMap<String, Set<String>> filterAttributeToSourceMap;

  /** Following fields are mutable and updated during the ExecutionTree building phase * */
  private final Set<String> pendingSelectionSources = new HashSet<>();

  private final Set<String> pendingMetricAggregationSources = new HashSet<>();
  private final Set<String> pendingTimeAggregationSources = new HashSet<>();
  private final Set<String> pendingSelectionSourcesForOrderBy = new HashSet<>();
  private final Set<String> pendingMetricAggregationSourcesForOrderBy = new HashSet<>();
  private boolean sortAndPaginationNodeAdded = false;

  // map of filter, selections (attribute, metrics, aggregations), order by attributes to source map
  private final Map<String, Set<String>> allAttributesToSourcesMap = new HashMap<>();

  private ExecutionContext(
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      EntitiesRequest entitiesRequest,
      EntitiesRequestContext entitiesRequestContext) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    this.entitiesRequest = entitiesRequest;
    this.entitiesRequestContext = entitiesRequestContext;
    buildSourceToExpressionMaps();
    buildSourceToFilterExpressionMaps();
    buildSourceToOrderByExpressionMaps();
  }

  public static ExecutionContext from(
      AttributeMetadataProvider metadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      EntitiesRequest entitiesRequest,
      EntitiesRequestContext entitiesRequestContext) {
    return new ExecutionContext(metadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
  }

  public String getTenantId() {
    return entitiesRequestContext.getTenantId();
  }

  public String getTimestampAttributeId() {
    return entitiesRequestContext.getTimestampAttributeId();
  }

  public EntitiesRequest getEntitiesRequest() {
    return entitiesRequest;
  }

  public AttributeMetadataProvider getAttributeMetadataProvider() {
    return attributeMetadataProvider;
  }

  public Map<String, List<Expression>> getSourceToSelectionExpressionMap() {
    return sourceToSelectionExpressionMap;
  }

  public Map<String, Set<String>> getSourceToSelectionAttributeMap() {
    return sourceToSelectionAttributeMap;
  }

  public Map<String, List<Expression>> getSourceToMetricExpressionMap() {
    return sourceToMetricExpressionMap;
  }

  public Map<String, List<TimeAggregation>> getSourceToTimeAggregationMap() {
    return sourceToTimeAggregationMap;
  }

  public Map<String, List<OrderByExpression>> getSourceToSelectionOrderByExpressionMap() {
    return sourceToSelectionOrderByExpressionMap;
  }

  public Map<String, Set<String>> getSourceToSelectionOrderByAttributeMap() {
    return sourceToSelectionOrderByAttributeMap;
  }

  public Map<String, List<OrderByExpression>> getSourceToMetricOrderByExpressionMap() {
    return sourceToMetricOrderByExpressionMap;
  }

  public Map<String, Set<String>> getSourceToMetricOrderByAttributeMap() {
    return sourceToMetricOrderByAttributeMap;
  }

  public Map<String, String> getRequestHeaders() {
    return entitiesRequestContext.getHeaders();
  }

  public EntitiesRequestContext getEntitiesRequestContext() {
    return this.entitiesRequestContext;
  }

  public Set<String> getPendingSelectionSources() {
    return pendingSelectionSources;
  }

  public Set<String> getPendingMetricAggregationSources() {
    return pendingMetricAggregationSources;
  }

  public Set<String> getPendingTimeAggregationSources() {
    return pendingTimeAggregationSources;
  }

  public Set<String> getPendingSelectionSourcesForOrderBy() {
    return pendingSelectionSourcesForOrderBy;
  }

  public Set<String> getPendingMetricAggregationSourcesForOrderBy() {
    return pendingMetricAggregationSourcesForOrderBy;
  }

  public boolean isSortAndPaginationNodeAdded() {
    return sortAndPaginationNodeAdded;
  }

  public void setSortAndPaginationNodeAdded(boolean sortAndPaginationNodeAdded) {
    this.sortAndPaginationNodeAdded = sortAndPaginationNodeAdded;
  }

  public List<Expression> getEntityIdExpressions() {
    List<String> entityIdAttributeNames =
        AttributeMetadataUtil.getIdAttributeIds(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            this.entitiesRequestContext,
            entitiesRequest.getEntityType());
    return IntStream.range(0, entityIdAttributeNames.size())
        .mapToObj(
            value ->
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(entityIdAttributeNames.get(value))
                            .setAlias("entityId" + value)
                            .build())
                    .build())
        .collect(Collectors.toList());
  }

  public void removePendingSelectionSource(String source) {
    pendingSelectionSources.remove(source);
  }

  public void removePendingMetricAggregationSources(String source) {
    pendingMetricAggregationSources.remove(source);
  }

  public void removePendingSelectionSourceForOrderBy(String source) {
    pendingSelectionSourcesForOrderBy.remove(source);
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

  public Map<String, Set<String>> getAllAttributesToSourcesMap() {
    return allAttributesToSourcesMap;
  }

  private void buildSourceToExpressionMaps() {
    Map<ValueCase, List<Expression>> selectionExprTypeToExprMap =
        entitiesRequest.getSelectionList().stream()
            .collect(Collectors.groupingBy(Expression::getValueCase, Collectors.toList()));
    sourceToSelectionExpressionMap =
        getDataSourceToExpressionMap(selectionExprTypeToExprMap.get(ValueCase.COLUMNIDENTIFIER));
    sourceToSelectionAttributeMap = buildSourceToAttributesMap(sourceToSelectionExpressionMap);

    sourceToMetricExpressionMap =
        getDataSourceToExpressionMap(selectionExprTypeToExprMap.get(ValueCase.FUNCTION));
    sourceToTimeAggregationMap =
        getDataSourceToTimeAggregation(entitiesRequest.getTimeAggregationList());
    pendingSelectionSources.addAll(sourceToSelectionExpressionMap.keySet());
    pendingMetricAggregationSources.addAll(sourceToMetricExpressionMap.keySet());
    pendingTimeAggregationSources.addAll(sourceToTimeAggregationMap.keySet());
  }

  private void buildSourceToFilterExpressionMaps() {
    sourceToFilterExpressionMap = getSourceToFilterExpressionMap(entitiesRequest.getFilter());
    sourceToFilterAttributeMap = buildSourceToAttributesMap(sourceToFilterExpressionMap);
    filterAttributeToSourceMap =
        ImmutableMap.<String, Set<String>>builder()
            .putAll(ExpressionReader.buildAttributeToSourcesMap(sourceToFilterAttributeMap))
            .build();
  }

  private void buildSourceToOrderByExpressionMaps() {
    // Ensure that the OrderByExpression function alias matches that of a column in the selection or
    // TimeAggregation, since the OrderByComparator uses the alias to match the column name in the
    // QueryService results
    List<OrderByExpression> orderByExpressions =
        OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
            entitiesRequest.getOrderByList(),
            entitiesRequest.getSelectionList(),
            entitiesRequest.getTimeAggregationList());
    Map<ValueCase, List<OrderByExpression>> orderByExpressionTypeToExpressionMap =
        orderByExpressions.stream()
            .collect(
                Collectors.groupingBy(
                    orderByExpression -> orderByExpression.getExpression().getValueCase(),
                    Collectors.toList()));
    sourceToSelectionOrderByExpressionMap =
        getDataSourceToOrderByExpressionMap(
            orderByExpressionTypeToExpressionMap.getOrDefault(ValueCase.COLUMNIDENTIFIER, Collections.emptyList()));
    sourceToSelectionOrderByAttributeMap =
        buildSourceToAttributesMap(
            convertOrderByExpressionToExpression(sourceToSelectionOrderByExpressionMap));

    sourceToMetricOrderByExpressionMap =
        getDataSourceToOrderByExpressionMap(
            orderByExpressionTypeToExpressionMap.getOrDefault(ValueCase.FUNCTION, Collections.emptyList()));
    sourceToMetricOrderByAttributeMap =
        buildSourceToAttributesMap(
            convertOrderByExpressionToExpression(sourceToMetricOrderByExpressionMap));
    pendingSelectionSourcesForOrderBy.addAll(sourceToSelectionOrderByExpressionMap.keySet());
    pendingMetricAggregationSourcesForOrderBy.addAll(sourceToMetricOrderByExpressionMap.keySet());
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
              (childFilter) ->
                  getSourceToFilterExpressionMap(childFilter, sourceToFilterExpressionMap));
    }
  }

  private ImmutableMap<String, List<Expression>> getDataSourceToExpressionMap(
      List<Expression> expressions) {
    if (expressions == null || expressions.isEmpty()) {
      return ImmutableMap.of();
    }
    Map<String, List<Expression>> sourceToExpressionMap = new HashMap<>();
    Map<String, AttributeMetadata> attrNameToMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            this.entitiesRequestContext, entitiesRequest.getEntityType());
    for (Expression expression : expressions) {
      Set<String> columnNames = ExpressionReader.extractColumns(expression);
      Set<AttributeSource> sources =
          Arrays.stream(AttributeSource.values()).collect(Collectors.toSet());
      for (String columnName : columnNames) {
        List<AttributeSource> sourcesList = attrNameToMetadataMap.get(columnName).getSourcesList();
        sources.retainAll(sourcesList);
        allAttributesToSourcesMap
            .computeIfAbsent(columnName, v -> new HashSet<>())
            .addAll(sourcesList.stream().map(Enum::name).collect(Collectors.toList()));
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
                                .map(ExpressionReader::extractColumns)
                                .flatMap(Collection::stream)
                                .collect(Collectors.toSet()))))
        .build();
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

  @Override
  public String toString() {
    return "ExecutionContext{" +
        "attributeMetadataProvider=" + attributeMetadataProvider +
        ", entityIdColumnsConfigs=" + entityIdColumnsConfigs +
        ", entitiesRequest=" + entitiesRequest +
        ", entitiesRequestContext=" + entitiesRequestContext +
        ", sourceToSelectionExpressionMap=" + sourceToSelectionExpressionMap +
        ", sourceToSelectionAttributeMap=" + sourceToSelectionAttributeMap +
        ", sourceToMetricExpressionMap=" + sourceToMetricExpressionMap +
        ", sourceToTimeAggregationMap=" + sourceToTimeAggregationMap +
        ", sourceToSelectionOrderByExpressionMap=" + sourceToSelectionOrderByExpressionMap +
        ", sourceToSelectionOrderByAttributeMap=" + sourceToSelectionOrderByAttributeMap +
        ", sourceToMetricOrderByExpressionMap=" + sourceToMetricOrderByExpressionMap +
        ", sourceToMetricOrderByAttributeMap=" + sourceToMetricOrderByAttributeMap +
        ", sourceToFilterExpressionMap=" + sourceToFilterExpressionMap +
        ", sourceToFilterAttributeMap=" + sourceToFilterAttributeMap +
        ", filterAttributeToSourceMap=" + filterAttributeToSourceMap +
        ", pendingSelectionSources=" + pendingSelectionSources +
        ", pendingMetricAggregationSources=" + pendingMetricAggregationSources +
        ", pendingTimeAggregationSources=" + pendingTimeAggregationSources +
        ", pendingSelectionSourcesForOrderBy=" + pendingSelectionSourcesForOrderBy +
        ", pendingMetricAggregationSourcesForOrderBy=" + pendingMetricAggregationSourcesForOrderBy +
        ", sortAndPaginationNodeAdded=" + sortAndPaginationNodeAdded +
        ", allAttributesToSourcesMap=" + allAttributesToSourcesMap +
        '}';
  }
}
