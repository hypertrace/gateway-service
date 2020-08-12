package org.hypertrace.gateway.service.entity.query.visitor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.IEntityFetcher;
import org.hypertrace.gateway.service.common.util.DataCollectionUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.EntityKeyEntityBuilderEntryComparator;
import org.hypertrace.gateway.service.entity.EntityQueryHandlerRegistry;
import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.SelectionAndFilterNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
import org.hypertrace.gateway.service.entity.query.TotalFetcherNode;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Visitor that executes each QueryNode in the execution tree.
 */
public class ExecutionVisitor implements Visitor<EntityFetcherResponse> {

  private final EntityQueryHandlerRegistry queryHandlerRegistry;
  private final ExecutionContext executionContext;
  private Logger LOG = LoggerFactory.getLogger(ExecutionVisitor.class);

  public ExecutionVisitor(ExecutionContext executionContext,
      EntityQueryHandlerRegistry queryHandlerRegistry) {
    this.executionContext = executionContext;
    this.queryHandlerRegistry = queryHandlerRegistry;
  }

  @VisibleForTesting
  protected static EntityFetcherResponse intersect(List<EntityFetcherResponse> builders) {
    return new EntityFetcherResponse(
        builders.stream()
            .map(EntityFetcherResponse::getEntityKeyBuilderMap)
            .reduce(
                (map1, map2) ->
                    Maps.difference(map1, map2).entriesDiffering().entrySet().stream()
                        .collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                stringValueDifferenceEntry -> {
                                  MapDifference.ValueDifference<Entity.Builder> diff =
                                      stringValueDifferenceEntry.getValue();
                                  return diff.leftValue().mergeFrom(diff.rightValue().build());
                                })))
            .orElse(Collections.emptyMap()));
  }

  @VisibleForTesting
  protected static EntityFetcherResponse union(List<EntityFetcherResponse> builders) {
    return new EntityFetcherResponse(
        builders.stream()
            .map(EntityFetcherResponse::getEntityKeyBuilderMap)
            .reduce(
                new LinkedHashMap<>(),
                (map1, map2) -> {
                  Map<EntityKey, Builder> newMap = new LinkedHashMap<>(map1);
                  map2.forEach(
                      (key, value) ->
                          newMap.merge(key, value, (v1, v2) -> v1.mergeFrom(v2.build())));
                  return newMap;
                }));
  }

  @Override
  public EntityFetcherResponse visit(DataFetcherNode dataFetcherNode) {
    String source = dataFetcherNode.getSource();
    EntitiesRequest request =
        EntitiesRequest.newBuilder(executionContext.getEntitiesRequest())
            .clearSelection()
            .clearFilter()
            .addAllSelection(
                executionContext
                    .getSourceToSelectionExpressionMap()
                    .getOrDefault(source, executionContext.getEntityIdExpressions()))
            .setFilter(dataFetcherNode.getFilter())
            .build();
    IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(source);
    EntitiesRequestContext context =
        new EntitiesRequestContext(
            executionContext.getTenantId(),
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getEntityType(),
            executionContext.getRequestHeaders());
    return entityFetcher.getEntities(context, request);
  }

  @Override
  public EntityFetcherResponse visit(AndNode andNode) {
    return intersect(
        andNode.getChildNodes().parallelStream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.toList()));
  }

  @Override
  public EntityFetcherResponse visit(OrNode orNode) {
    return union(
        orNode.getChildNodes().parallelStream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.toList()));
  }

  @Override
  public EntityFetcherResponse visit(SelectionNode selectionNode) {
    EntityFetcherResponse result = selectionNode.getChildNode().acceptVisitor(this);

    // If the result was empty when the filter is non-empty, it means no entities matched the filter
    // and hence no need to do any more follow up calls.
    if (result.isEmpty()
        && !Filter.getDefaultInstance().equals(executionContext.getEntitiesRequest().getFilter())) {
      LOG.debug("No results matched the filter so not fetching aggregate/timeseries metrics.");
      return result;
    }

    // Construct the filter from the child nodes result
    Filter filter = constructFilterFromChildNodesResult(result);
    // Select attributes, metric aggregations and time-series data from corresponding sources
    List<EntityFetcherResponse> resultMapList = new ArrayList<>();
    // if data are coming from multiple sources, then, get entities and aggregated metrics
    // needs to be separated
    resultMapList.addAll(selectionNode.getAttrSelectionSources().parallelStream()
        .map(
            source -> {
              EntitiesRequest request =
                  EntitiesRequest.newBuilder(executionContext.getEntitiesRequest())
                      .clearSelection()
                      .clearFilter()
                      .addAllSelection(
                          executionContext.getSourceToSelectionExpressionMap().get(source))
                      .setFilter(filter)
                      .build();
              IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(source);
              EntitiesRequestContext context =
                  new EntitiesRequestContext(
                      executionContext.getTenantId(),
                      request.getStartTimeMillis(),
                      request.getEndTimeMillis(),
                      request.getEntityType(),
                      executionContext.getRequestHeaders());
              return entityFetcher.getEntities(context, request);
            })
        .collect(Collectors.toList()));
    resultMapList.addAll(
        selectionNode.getAggMetricSelectionSources().parallelStream()
            .map(
                source -> {
                  EntitiesRequest request =
                      EntitiesRequest.newBuilder(executionContext.getEntitiesRequest())
                          .clearSelection()
                          .clearFilter()
                          .addAllSelection(
                              executionContext.getSourceToMetricExpressionMap().get(source))
                          .setFilter(filter)
                          .build();
                  IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(source);
                  EntitiesRequestContext context =
                      new EntitiesRequestContext(
                          executionContext.getTenantId(),
                          request.getStartTimeMillis(),
                          request.getEndTimeMillis(),
                          request.getEntityType(),
                          executionContext.getRequestHeaders());
                  return entityFetcher.getAggregatedMetrics(context, request);
                })
            .collect(Collectors.toList()));
    resultMapList.addAll(
        selectionNode.getTimeSeriesSelectionSources().parallelStream()
            .map(
                source -> {
                  EntitiesRequest request =
                      EntitiesRequest.newBuilder(executionContext.getEntitiesRequest())
                          .clearTimeAggregation()
                          .clearFilter()
                          .addAllTimeAggregation(
                              executionContext.getSourceToTimeAggregationMap().get(source))
                          .setFilter(filter)
                          .build();
                  IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(source);
                  EntitiesRequestContext requestContext =
                      new EntitiesRequestContext(
                          executionContext.getTenantId(),
                          request.getStartTimeMillis(),
                          request.getEndTimeMillis(),
                          request.getEntityType(),
                          executionContext.getRequestHeaders());
                  return entityFetcher.getTimeAggregatedMetrics(requestContext, request);
                })
            .collect(Collectors.toList()));
    return resultMapList.stream().reduce(result, (r1, r2) -> union(Arrays.asList(r1, r2)));
  }

  Filter constructFilterFromChildNodesResult(EntityFetcherResponse result) {
    if (result.isEmpty()) {
      return Filter.getDefaultInstance();
    }

    List<Expression> entityIdExpressionList = executionContext.getEntityIdExpressions();
    if (entityIdExpressionList.size() == 1) {
      Expression entityIdExpression = entityIdExpressionList.get(0);
      Set<String> entityIdValues =
          result.getEntityKeyBuilderMap().keySet().stream()
              .map(entityKey -> entityKey.getAttributes().get(0))
              .collect(Collectors.toSet());
      return Filter.newBuilder()
          .setLhs(entityIdExpression)
          .setOperator(Operator.IN)
          .setRhs(
              Expression.newBuilder()
                  .setLiteral(
                      LiteralConstant.newBuilder()
                          .setValue(
                              Value.newBuilder()
                                  .addAllStringArray(entityIdValues)
                                  .setValueType(ValueType.STRING_ARRAY))))
          .build();
    } else {
      return Filter.newBuilder()
          .setOperator(Operator.OR)
          .addAllChildFilter(
              result.getEntityKeyBuilderMap().keySet().stream()
                  .map(
                      entityKey ->
                          Filter.newBuilder()
                              .setOperator(Operator.AND)
                              .addAllChildFilter(
                                  IntStream.range(0, entityIdExpressionList.size())
                                      .mapToObj(
                                          value ->
                                              Filter.newBuilder()
                                                  .setOperator(Operator.EQ)
                                                  .setLhs(entityIdExpressionList.get(value))
                                                  .setRhs(
                                                      QueryExpressionUtil.getLiteralExpression(
                                                          entityKey.getAttributes().get(value)))
                                                  .build())
                                      .collect(Collectors.toList()))
                              .build())
                  .collect(Collectors.toList()))
          .build();
    }
  }

  @Override
  public EntityFetcherResponse visit(SortAndPaginateNode sortAndPaginateNode) {
    EntityFetcherResponse result = sortAndPaginateNode.getChildNode().acceptVisitor(this);
    // Set the total entities count in the execution context before pagination
    executionContext.setTotal(result.size());

    // Create a list from elements of HashMap
    List<Map.Entry<EntityKey, Builder>> list =
        new LinkedList<>(result.getEntityKeyBuilderMap().entrySet());

    // Sort the list
    List<Map.Entry<EntityKey, Entity.Builder>> sortedList =
        DataCollectionUtil.limitAndSort(
            list.stream(),
            sortAndPaginateNode.getLimit(),
            sortAndPaginateNode.getOffset(),
            sortAndPaginateNode.getOrderByExpressionList().size(),
            new EntityKeyEntityBuilderEntryComparator(
                sortAndPaginateNode.getOrderByExpressionList()));

    // put data from sorted list to a linked hashmap
    Map<EntityKey, Builder> linkedHashMap = new LinkedHashMap<>();
    sortedList.forEach(entry -> linkedHashMap.put(entry.getKey(), entry.getValue()));
    return new EntityFetcherResponse(linkedHashMap);
  }

  @Override
  public EntityFetcherResponse visit(NoOpNode noOpNode) {
    return new EntityFetcherResponse();
  }

  @Override
  public EntityFetcherResponse visit(SelectionAndFilterNode selectionAndFilterNode) {
    List<EntityFetcherResponse> resultMapList = new ArrayList<>();
    EntitiesRequest request =
        EntitiesRequest.newBuilder(executionContext.getEntitiesRequest())
            .setOffset(selectionAndFilterNode.getOffset())
            .setLimit(selectionAndFilterNode.getLimit())
            .build();
    EntitiesRequestContext context =
        new EntitiesRequestContext(
            executionContext.getTenantId(),
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getEntityType(),
            executionContext.getRequestHeaders());

    String source = selectionAndFilterNode.getSource();
    if (source.equals("QS")) {
      // TODO: Make these queries parallel
      IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(source);
      // Entities Attributes and aggregations request
      resultMapList.add(entityFetcher.getEntitiesAndAggregatedMetrics(context, request));
      // Time Series request
      resultMapList.add(entityFetcher.getTimeAggregatedMetrics(context, request));
      return resultMapList.stream().reduce(new EntityFetcherResponse(), (r1, r2) -> union(Arrays.asList(r1, r2)));
    } else { // EDS - can only get Entities, not Entity aggregations or time aggregations
      IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(source);
      return entityFetcher.getEntities(context, request);
    }
  }

  @Override
  public EntityFetcherResponse visit(PaginateOnlyNode paginateOnlyNode) {
    EntityFetcherResponse result = paginateOnlyNode.getChildNode().acceptVisitor(this);

    // Create a list from elements of HashMap
    List<Map.Entry<EntityKey, Builder>> list =
        new LinkedList<>(result.getEntityKeyBuilderMap().entrySet());

    // Sort the list
    List<Map.Entry<EntityKey, Entity.Builder>> sortedList =
        DataCollectionUtil.paginateAndLimit(
            list.stream(),
            paginateOnlyNode.getLimit(),
            paginateOnlyNode.getOffset());

    // put data from sorted list to a linked hashmap
    Map<EntityKey, Builder> linkedHashMap = new LinkedHashMap<>();
    sortedList.forEach(entry -> linkedHashMap.put(entry.getKey(), entry.getValue()));
    return new EntityFetcherResponse(linkedHashMap);
  }

  @Override
  public EntityFetcherResponse visit(TotalFetcherNode totalFetcherNode) {
    // TODO: Make parallel
    EntityFetcherResponse result = totalFetcherNode.getChildNode().acceptVisitor(this);

    IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(totalFetcherNode.getSource());
    executionContext.setTotal(entityFetcher.getTotalEntities(executionContext.getEntitiesRequestContext(), executionContext.getEntitiesRequest()));

    return result;
  }
}
