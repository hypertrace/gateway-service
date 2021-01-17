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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.EntityResponse;
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
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
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
public class ExecutionVisitor implements Visitor<EntityResponse> {

  private static final int THREAD_COUNT = 20;
  private static final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

  private final EntityQueryHandlerRegistry queryHandlerRegistry;
  private final ExecutionContext executionContext;
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionVisitor.class);

  public ExecutionVisitor(ExecutionContext executionContext,
                          EntityQueryHandlerRegistry queryHandlerRegistry) {
    this.executionContext = executionContext;
    this.queryHandlerRegistry = queryHandlerRegistry;
  }

  private static EntityFetcherResponse intersectEntities(List<EntityFetcherResponse> builders) {
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
  protected static EntityResponse intersect(List<EntityResponse> entityResponses) {
    EntityFetcherResponse entityFetcherResponse =
        intersectEntities(
            entityResponses.parallelStream()
                .map(EntityResponse::getEntityFetcherResponse)
                .collect(Collectors.toList()));
    Set<EntityKey> entityKeys =
        entityResponses.parallelStream()
            .map(EntityResponse::getEntityKeys)
            .reduce(Sets::intersection)
            .orElse(Collections.emptySet());

    return new EntityResponse(entityFetcherResponse, entityKeys);
  }

  private static EntityFetcherResponse unionEntities(List<EntityFetcherResponse> builders) {
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

  @VisibleForTesting
  protected static EntityResponse union(List<EntityResponse> entityResponses) {
    EntityFetcherResponse entityFetcherResponse =
        unionEntities(
            entityResponses.parallelStream()
                .map(EntityResponse::getEntityFetcherResponse)
                .collect(Collectors.toList()));
    Set<EntityKey> entityKeys =
        entityResponses.parallelStream()
            .map(EntityResponse::getEntityKeys)
            .reduce(Sets::union)
            .orElse(Collections.emptySet());

    return new EntityResponse(entityFetcherResponse, entityKeys);
  }

  @Override
  public EntityResponse visit(DataFetcherNode dataFetcherNode) {
    String source = dataFetcherNode.getSource();
    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();
    EntitiesRequestContext context =
        new EntitiesRequestContext(
            executionContext.getTenantId(),
            entitiesRequest.getStartTimeMillis(),
            entitiesRequest.getEndTimeMillis(),
            entitiesRequest.getEntityType(),
            executionContext.getTimestampAttributeId(),
            executionContext.getRequestHeaders());

    EntitiesRequest.Builder requestBuilder =
        EntitiesRequest.newBuilder(entitiesRequest)
            .clearSelection()
            .clearTimeAggregation()
            .clearFilter()
            .clearOrderBy()
            .clearLimit()
            .clearOffset()
            .addAllSelection(
                executionContext
                    .getSourceToSelectionExpressionMap()
                    .getOrDefault(source, executionContext.getEntityIdExpressions()))
            .setFilter(dataFetcherNode.getFilter());

    if (dataFetcherNode.getLimit() != null) {
      requestBuilder.setLimit(dataFetcherNode.getLimit());
    }

    if (dataFetcherNode.getOffset() != null) {
      requestBuilder.setOffset(dataFetcherNode.getOffset());
    }

    if (!dataFetcherNode.getOrderByExpressionList().isEmpty()) {
      requestBuilder.addAllOrderBy(dataFetcherNode.getOrderByExpressionList());
    }

    EntitiesRequest request = requestBuilder.build();
    IEntityFetcher entityFetcher = queryHandlerRegistry.getEntityFetcher(source);

    // if the data fetcher node is fetching paginated records, the total number of entities has to
    // be fetched separately
    if (dataFetcherNode.isPaginated()) {
      EntitiesRequest totalEntitiesRequest =
          EntitiesRequest.newBuilder(executionContext.getEntitiesRequest())
              .clearSelection()
              .clearTimeAggregation()
              .clearOrderBy()
              .clearLimit()
              .setOffset(0)
              .setFilter(dataFetcherNode.getFilter())
              .build();

      return new EntityResponse(
          entityFetcher.getEntities(context, request),
          entityFetcher
              .getEntities(context, totalEntitiesRequest)
              .getEntityKeyBuilderMap()
              .keySet());
    } else {
      // if the data fetcher node is not paginating, the total number of entities is equal to number
      // of records fetched
      EntityFetcherResponse response = entityFetcher.getEntities(context, request);
      return new EntityResponse(response, response.getEntityKeyBuilderMap().keySet());
    }
  }

  @Override
  public EntityResponse visit(AndNode andNode) {
    return intersect(
        andNode.getChildNodes().parallelStream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.toList()));
  }

  @Override
  public EntityResponse visit(OrNode orNode) {
    return union(
        orNode.getChildNodes().parallelStream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.toList()));
  }

  @Override
  public EntityResponse visit(SelectionNode selectionNode) {
    EntityResponse childNodeResponse = selectionNode.getChildNode().acceptVisitor(this);

    EntityFetcherResponse childEntityFetcherResponse = childNodeResponse.getEntityFetcherResponse();

    // If the result was empty when the filter is non-empty, it means no entities matched the filter
    // and hence no need to do any more follow up calls.
    if (childEntityFetcherResponse.isEmpty()
        && !Filter.getDefaultInstance().equals(executionContext.getEntitiesRequest().getFilter())) {
      LOG.debug("No results matched the filter so not fetching aggregate/timeseries metrics.");
      return childNodeResponse;
    }

    // Construct the filter from the child nodes result
    final Filter filter = constructFilterFromChildNodesResult(childEntityFetcherResponse);

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
                      .clearTimeAggregation()
                      .clearFilter()
                      // TODO: Should we push order by, limit and offet down to the data source?
                      // If we want to push the order by down, we would also have to divide order by into
                      // sourceToOrderBySelectionExpressionMap, sourceToOrderByMetricExpressionMap, sourceToOrderByTimeAggregationMap
                      .clearOrderBy()
                      .clearLimit()
                      .clearOffset()
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
                      executionContext.getTimestampAttributeId(),
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
                          .clearTimeAggregation()
                          .clearFilter()
                          .clearOrderBy()
                          .clearOffset()
                          .clearLimit()
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
                          executionContext.getTimestampAttributeId(),
                          executionContext.getRequestHeaders());
                  return entityFetcher.getEntities(context, request);
                })
            .collect(Collectors.toList()));
    resultMapList.addAll(
        selectionNode.getTimeSeriesSelectionSources().parallelStream()
            .map(
                source -> {
                  EntitiesRequest request =
                      EntitiesRequest.newBuilder(executionContext.getEntitiesRequest())
                          .clearSelection()
                          .clearTimeAggregation()
                          .clearFilter()
                          .clearOrderBy()
                          .clearOffset()
                          .clearLimit()
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
                          executionContext.getTimestampAttributeId(),
                          executionContext.getRequestHeaders());
                  return entityFetcher.getTimeAggregatedMetrics(requestContext, request);
                })
            .collect(Collectors.toList()));

    EntityFetcherResponse response =
        resultMapList.stream()
            .reduce(childEntityFetcherResponse, (r1, r2) -> unionEntities(Arrays.asList(r1, r2)));

    if (!childEntityFetcherResponse.isEmpty()) {
      // if the child fetcher response is non empty, the total set of entity keys
      // has already been fetched by node below it.
      // Could be DataFetcherNode or a child SelectionNode
      return new EntityResponse(response, childNodeResponse.getEntityKeys());
    } else {
      // if the child fetcher response is empty, the total set of entity keys
      // is equal to the response fetched by the current SelectionNode
      return new EntityResponse(response, response.getEntityKeyBuilderMap().keySet());
    }
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
  public EntityResponse visit(SortAndPaginateNode sortAndPaginateNode) {
    EntityResponse childNodeResponse = sortAndPaginateNode.getChildNode().acceptVisitor(this);

    // Create a list from elements of HashMap
    List<Map.Entry<EntityKey, Builder>> list =
        new LinkedList<>(
            childNodeResponse.getEntityFetcherResponse().getEntityKeyBuilderMap().entrySet());

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
    return new EntityResponse(
        new EntityFetcherResponse(linkedHashMap), childNodeResponse.getEntityKeys());
  }

  @Override
  public EntityResponse visit(NoOpNode noOpNode) {
    return new EntityResponse();
  }

  @Override
  public EntityResponse visit(PaginateOnlyNode paginateOnlyNode) {
    EntityResponse childNodeResponse = paginateOnlyNode.getChildNode().acceptVisitor(this);

    // Create a list from elements of HashMap
    List<Map.Entry<EntityKey, Builder>> list =
        new LinkedList<>(
            childNodeResponse.getEntityFetcherResponse().getEntityKeyBuilderMap().entrySet());

    // Sort the list
    List<Map.Entry<EntityKey, Entity.Builder>> sortedList =
        DataCollectionUtil.paginateAndLimit(
            list.stream(),
            paginateOnlyNode.getLimit(),
            paginateOnlyNode.getOffset());

    // put data from sorted list to a linked hashmap
    Map<EntityKey, Builder> linkedHashMap = new LinkedHashMap<>();
    sortedList.forEach(entry -> linkedHashMap.put(entry.getKey(), entry.getValue()));

    return new EntityResponse(
        new EntityFetcherResponse(linkedHashMap), childNodeResponse.getEntityKeys());
  }
}
