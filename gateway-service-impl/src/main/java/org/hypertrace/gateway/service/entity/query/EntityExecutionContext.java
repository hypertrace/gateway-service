package org.hypertrace.gateway.service.entity.query;

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Expression.Builder;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;

/**
 * Context object constructed from the input query that is used at different stages in the query
 * execution
 */
public class EntityExecutionContext {
  /** Following fields are immutable and set in the constructor * */
  private final AttributeMetadataProvider attributeMetadataProvider;

  private final EntityIdColumnsConfigs entityIdColumnsConfigs;
  private final EntitiesRequestContext entitiesRequestContext;
  private final EntitiesRequest entitiesRequest;

  private final ExpressionContext expressionContext;

  /** Following fields are mutable and updated during the ExecutionTree building phase * */
  private final Set<String> pendingSelectionSources = new HashSet<>();

  private final Set<String> pendingMetricAggregationSources = new HashSet<>();
  private final Set<String> pendingTimeAggregationSources = new HashSet<>();
  private final Set<String> pendingSelectionSourcesForOrderBy = new HashSet<>();
  private final Set<String> pendingMetricAggregationSourcesForOrderBy = new HashSet<>();
  private boolean sortAndPaginationNodeAdded = false;

  public EntityExecutionContext(
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      EntitiesRequestContext entitiesRequestContext,
      EntitiesRequest entitiesRequest) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    this.entitiesRequest = entitiesRequest;
    this.entitiesRequestContext = entitiesRequestContext;

    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            this.entitiesRequestContext, entitiesRequestContext.getEntityType());
    this.expressionContext =
        new ExpressionContext(
            attributeMetadataMap,
            entitiesRequest.getFilter(),
            entitiesRequest.getSelectionList(),
            entitiesRequest.getTimeAggregationList(),
            entitiesRequest.getOrderByList(),
            // entities request does not have group by
            Collections.emptyList());

    buildSelectionPendingSources();
    buildOrderByPendingSources();
  }

  public String getTenantId() {
    return entitiesRequestContext.getTenantId();
  }

  public String getTimestampAttributeId() {
    return entitiesRequestContext.getTimestampAttributeId();
  }

  public AttributeMetadataProvider getAttributeMetadataProvider() {
    return attributeMetadataProvider;
  }

  public Map<String, String> getRequestHeaders() {
    return entitiesRequestContext.getHeaders();
  }

  public EntitiesRequest getEntitiesRequest() {
    return entitiesRequest;
  }

  public EntitiesRequestContext getEntitiesRequestContext() {
    return this.entitiesRequestContext;
  }

  public ExpressionContext getExpressionContext() {
    return this.expressionContext;
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
            entitiesRequestContext.getEntityType());
    return IntStream.range(0, entityIdAttributeNames.size())
        .mapToObj(
            value ->
                QueryExpressionUtil.buildAttributeExpression(
                    entityIdAttributeNames.get(value), "entityId" + value))
        .map(Builder::build)
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

  public void removeSelectionAttributes(String source, Set<String> attributes) {
    Map<String, List<Expression>> sourceToSelectionExpressionMap =
        expressionContext.getSourceToSelectionExpressionMap();
    if (!sourceToSelectionExpressionMap.containsKey(source)) {
      return;
    }

    Predicate<Expression> retainExpressionPredicate =
        expression ->
            Sets.intersection(ExpressionReader.extractAttributeIds(expression), attributes)
                .isEmpty();
    List<Expression> expressions =
        sourceToSelectionExpressionMap.get(source).stream()
            .filter(retainExpressionPredicate)
            .collect(Collectors.toUnmodifiableList());

    Map<String, List<Expression>> sourceToRetainedSelectionExpressionMap =
        sourceToSelectionExpressionMap.entrySet().stream()
            .map(
                entry ->
                    entry.getKey().equals(source) ? Map.entry(entry.getKey(), expressions) : entry)
            .filter(entry -> !entry.getValue().isEmpty())
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));

    expressionContext.setSourceToSelectionExpressionMap(sourceToRetainedSelectionExpressionMap);
    expressionContext.setSourceToSelectionAttributeMap(sourceToRetainedSelectionExpressionMap);
  }

  private void buildSelectionPendingSources() {
    pendingSelectionSources.addAll(expressionContext.getSourceToSelectionExpressionMap().keySet());
    pendingMetricAggregationSources.addAll(
        expressionContext.getSourceToMetricAggregationExpressionMap().keySet());
    pendingTimeAggregationSources.addAll(
        expressionContext.getSourceToTimeAggregationMap().keySet());
  }

  private void buildOrderByPendingSources() {
    pendingSelectionSourcesForOrderBy.addAll(
        expressionContext.getSourceToSelectionOrderByExpressionMap().keySet());
    pendingMetricAggregationSourcesForOrderBy.addAll(
        expressionContext.getSourceToMetricOrderByExpressionMap().keySet());
  }

  @Override
  public String toString() {
    return "EntityExecutionContext{"
        + "attributeMetadataProvider="
        + attributeMetadataProvider
        + ", entityIdColumnsConfigs="
        + entityIdColumnsConfigs
        + ", entitiesRequest="
        + entitiesRequest
        + ", entitiesRequestContext="
        + entitiesRequestContext
        + ", expressionContext="
        + expressionContext
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
        + '}';
  }
}
