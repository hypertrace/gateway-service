package org.hypertrace.gateway.service.entity.query;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.OrderByUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
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

  private final EntitiesRequest entitiesRequest;
  private final EntitiesRequestContext entitiesRequestContext;
  private ImmutableMap<String, List<Expression>> sourceToSelectionExpressionMap;
  private ImmutableMap<String, List<Expression>> sourceToMetricExpressionMap;
  private ImmutableMap<String, List<TimeAggregation>> sourceToTimeAggregationMap;
  private ImmutableMap<String, List<OrderByExpression>> sourceToOrderByExpressionMap;
  private ImmutableMap<String, List<Expression>> sourceToFilterExpressionMap;

  /** Following fields are mutable and updated during the ExecutionTree building phase * */
  private final Set<String> pendingSelectionSources = new HashSet<>();

  private final Set<String> pendingMetricAggregationSources = new HashSet<>();
  private final Set<String> pendingTimeAggregationSources = new HashSet<>();
  private final Set<String> pendingSelectionSourcesForOrderBy = new HashSet<>();
  private final Set<String> pendingMetricAggregationSourcesForOrderBy = new HashSet<>();
  private boolean sortAndPaginationNodeAdded = false;

  private final Map<String, Set<AttributeSource>> attributeToSourcesMap = new HashMap<>();

  /** Following fields set during the query execution phase * */
  // Total number of entities. Set during the execution before pagination
  private int total;

  private ExecutionContext(
      AttributeMetadataProvider attributeMetadataProvider,
      EntitiesRequest entitiesRequest,
      EntitiesRequestContext entitiesRequestContext) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entitiesRequest = entitiesRequest;
    this.entitiesRequestContext = entitiesRequestContext;
    buildSourceToExpressionMaps();
  }

  public static ExecutionContext from(
      AttributeMetadataProvider metadataProvider,
      EntitiesRequest entitiesRequest,
      EntitiesRequestContext entitiesRequestContext) {
    return new ExecutionContext(metadataProvider, entitiesRequest, entitiesRequestContext);
  }

  public String getTenantId() {
    return entitiesRequestContext.getTenantId();
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

  public Map<String, List<Expression>> getSourceToMetricExpressionMap() {
    return sourceToMetricExpressionMap;
  }

  public Map<String, List<TimeAggregation>> getSourceToTimeAggregationMap() {
    return sourceToTimeAggregationMap;
  }

  public Map<String, List<OrderByExpression>> getSourceToOrderByExpressionMap() {
    return sourceToOrderByExpressionMap;
  }

  public Map<String, String> getRequestHeaders() {
    return entitiesRequestContext.getHeaders();
  }

  public EntitiesRequestContext getEntitiesRequestContext() {
    return this.entitiesRequestContext;
  }

  public int getTotal() {
    return total;
  }

  public void setTotal(int total) {
    this.total = total;
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

  public Map<String, Set<AttributeSource>> getAttributeToSourcesMap() {
    return attributeToSourcesMap;
  }

  private void buildSourceToExpressionMaps() {
    Map<ValueCase, List<Expression>> selectionExprTypeToExprMap =
        entitiesRequest.getSelectionList().stream()
            .collect(Collectors.groupingBy(Expression::getValueCase, Collectors.toList()));
    sourceToSelectionExpressionMap =
        getDataSourceToExpressionMap(selectionExprTypeToExprMap.get(ValueCase.COLUMNIDENTIFIER));
    sourceToMetricExpressionMap =
        getDataSourceToExpressionMap(selectionExprTypeToExprMap.get(ValueCase.FUNCTION));
    sourceToTimeAggregationMap =
        getDataSourceToTimeAggregation(entitiesRequest.getTimeAggregationList());
    sourceToOrderByExpressionMap = getDataSourceToOrderByExpressionMap(entitiesRequest);
    sourceToFilterExpressionMap = getSourceToFilterExpressionMap(entitiesRequest.getFilter());
    pendingSelectionSources.addAll(sourceToSelectionExpressionMap.keySet());
    pendingMetricAggregationSources.addAll(sourceToMetricExpressionMap.keySet());
    pendingTimeAggregationSources.addAll(sourceToTimeAggregationMap.keySet());
  }

  private ImmutableMap<String, List<OrderByExpression>> getDataSourceToOrderByExpressionMap(
      EntitiesRequest entitiesRequest) {
    // Ensure that the OrderByExpression function alias matches that of a column in the selection or
    // TimeAggregation
    // since the OrderByComparator uses the alias to match the column name in the QueryService
    // results.
    List<OrderByExpression> orderByExpressions =
        OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
            entitiesRequest.getOrderByList(),
            entitiesRequest.getSelectionList(),
            entitiesRequest.getTimeAggregationList());

    Map<String, List<OrderByExpression>> result = new HashMap<>();
    for (OrderByExpression orderByExpression : orderByExpressions) {
      Expression expression = orderByExpression.getExpression();
      Map<String, List<Expression>> map =
          getDataSourceToExpressionMap(Collections.singletonList(expression));
      // There should only be one element in the map.
      result
          .computeIfAbsent(map.keySet().iterator().next(), k -> new ArrayList<>())
          .add(orderByExpression);
      if (expression.getValueCase().equals(ValueCase.COLUMNIDENTIFIER)) {
        pendingSelectionSourcesForOrderBy.addAll(map.keySet());
      }
      if (expression.getValueCase().equals(ValueCase.FUNCTION)) {
        pendingMetricAggregationSourcesForOrderBy.addAll(map.keySet());
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

  private void getSourceToFilterExpressionMap(Filter filter,
                                              Map<String, List<Expression>> sourceToExpressionMap) {
    if (Filter.getDefaultInstance().equals(filter)) {
      return;
    }

    if (filter.hasLhs()) {
      // Assuming RHS never has columnar expressions.
      Expression lhs = filter.getLhs();
      Map<String, List<Expression>> map = getDataSourceToExpressionMap(List.of(lhs));
      sourceToExpressionMap.putAll(map);
    }

    if (filter.getChildFilterCount() > 0) {
      filter.getChildFilterList().forEach((childFilter) ->
          getSourceToFilterExpressionMap(childFilter, sourceToExpressionMap));
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
            this.entitiesRequestContext, AttributeScope.valueOf(entitiesRequest.getEntityType()));
    for (Expression expression : expressions) {
      Set<String> columnNames = new HashSet<>();
      extractColumn(columnNames, expression);
      Set<AttributeSource> sources =
          Arrays.stream(AttributeSource.values()).collect(Collectors.toSet());
      for (String columnName : columnNames) {
        List<AttributeSource> sourcesList = attrNameToMetadataMap.get(columnName).getSourcesList();
        sources.retainAll(sourcesList);
        attributeToSourcesMap.computeIfAbsent(columnName, v -> new HashSet<>()).addAll(sourcesList);
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

  private void extractColumn(Set<String> columns, Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        String columnName = expression.getColumnIdentifier().getColumnName();
        columns.add(columnName);
        break;
      case FUNCTION:
        for (Expression exp : expression.getFunction().getArgumentsList()) {
          extractColumn(columns, exp);
        }
        break;
      case ORDERBY:
        extractColumn(columns, expression.getOrderBy().getExpression());
      case LITERAL:
      case VALUE_NOT_SET:
        break;
    }
  }

  @Override
  public String toString() {
    return "ExecutionContext{"
        + "attributeMetadataProvider="
        + attributeMetadataProvider
        + ", entitiesRequest="
        + entitiesRequest
        + ", sourceToSelectionExpressionMap="
        + sourceToSelectionExpressionMap
        + ", sourceToMetricExpressionMap="
        + sourceToMetricExpressionMap
        + ", sourceToTimeAggregationMap="
        + sourceToTimeAggregationMap
        + ", sourceToOrderByExpressionMap="
        + sourceToOrderByExpressionMap
        + ", pendingSelectionSources="
        + pendingSelectionSources
        + ", pendingMetricAggregationSources="
        + pendingMetricAggregationSources
        + ", pendingTimeAggregationSources="
        + pendingTimeAggregationSources
        + ", pendingSelectionSourcesForOrderBy="
        + pendingSelectionSourcesForOrderBy
        + ", pendingMetricAggregationSourcesForOrderBy="
        + pendingMetricAggregationSourcesForOrderBy
        + ", sortAndPaginationNodeAdded="
        + sortAndPaginationNodeAdded
        + ", total="
        + total
        + '}';
  }
}
