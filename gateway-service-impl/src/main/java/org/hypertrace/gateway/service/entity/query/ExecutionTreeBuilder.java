package org.hypertrace.gateway.service.entity.query;

import static org.hypertrace.core.attribute.service.v1.AttributeSource.EDS;
import static org.hypertrace.core.attribute.service.v1.AttributeSource.QS;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.TimeRangeFilterUtil;
import org.hypertrace.gateway.service.entity.query.visitor.ExecutionContextBuilderVisitor;
import org.hypertrace.gateway.service.entity.query.visitor.FilterOptimizingVisitor;
import org.hypertrace.gateway.service.entity.query.visitor.PrintVisitor;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class used to build the execution tree. */
public class ExecutionTreeBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionTreeBuilder.class);

  private final Map<String, AttributeMetadata> attributeMetadataMap;
  private final EntityExecutionContext executionContext;
  private final Set<String> sourceSetsIfFilterAndOrderByAreFromSameSourceSets;

  public ExecutionTreeBuilder(EntityExecutionContext executionContext) {
    this.executionContext = executionContext;
    this.attributeMetadataMap =
        executionContext
            .getAttributeMetadataProvider()
            .getAttributesMetadata(
                executionContext.getEntitiesRequestContext(),
                executionContext.getEntitiesRequest().getEntityType());

    this.sourceSetsIfFilterAndOrderByAreFromSameSourceSets =
        ExpressionContext.getSourceSetsIfFilterAndOrderByAreFromSameSourceSets(
            executionContext.getExpressionContext());
  }

  /**
   * Builds the complete execution tree from the Filter tree.
   *
   * <p>It contains steps in addition to the filter tree like Sorting/Pagination and Selecting
   * aggregated metrics and timeseries data after the basic filtering steps
   *
   * @return the root node of the execution tree
   */
  public QueryNode build() {
    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();

    // EDS source has all the entities (live + non live). In order to fetch all the non live
    // entities, along with live entities,
    // the query needs to be anchored around EDS.
    // Hence, EDS is treated as a DataFetcherNode, so that first all the entities are fetched from
    // EDS, irrespective of the time range. And, then the remaining data can be fetched from other
    // sources

    // If there is a filter on any other data source than EDS, then fetching all the entities
    // (live + non live) does not make sense, since filters on any other data source will anyways
    // filter out the "non live" entities
    boolean areFiltersOnlyOnEds =
        ExpressionContext.areFiltersOnlyOnCurrentDataSource(
            executionContext.getExpressionContext(), EDS.name());
    if (entitiesRequest.getIncludeNonLiveEntities() && areFiltersOnlyOnEds) {
      ExecutionTreeUtils.removeDuplicateSelectionAttributes(executionContext, EDS.name());

      QueryNode rootNode = new DataFetcherNode(EDS.name(), entitiesRequest.getFilter());
      // if the filter by and order by are from the same source, pagination can be pushed down to
      // EDS
      if (sourceSetsIfFilterAndOrderByAreFromSameSourceSets.contains(EDS.name())) {
        rootNode =
            new DataFetcherNode(
                EDS.name(),
                entitiesRequest.getFilter(),
                entitiesRequest.getLimit(),
                entitiesRequest.getOffset(),
                entitiesRequest.getOrderByList(),
                entitiesRequest.getFetchTotal());
        executionContext.setSortAndPaginationNodeAdded(true);
      }

      rootNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));

      QueryNode executionTree = buildExecutionTree(executionContext, rootNode);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Execution Tree:{}", executionTree.acceptVisitor(new PrintVisitor()));
      }

      return executionTree;
    }

    // If all the attributes, filters, order by and sort are requested from a single source, there
    // can be source specification
    // optimization where all projections, filters, order by, sort and limit can be pushed down to
    // the data store
    Optional<String> singleSourceForAllAttributes =
        ExecutionTreeUtils.getValidSingleSource(executionContext);
    if (singleSourceForAllAttributes.isPresent()) {
      String source = singleSourceForAllAttributes.get();
      QueryNode rootNode = buildExecutionTreeForSameSourceFilterAndSelection(source);

      rootNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));
      QueryNode executionTree = buildExecutionTree(executionContext, rootNode);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Execution Tree:{}", executionTree.acceptVisitor(new PrintVisitor()));
      }

      return executionTree;
    }

    ExecutionTreeUtils.removeDuplicateSelectionAttributes(executionContext, QS.name());

    QueryNode filterTree = buildFilterTree(executionContext, entitiesRequest.getFilter());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filter Tree:{}", filterTree.acceptVisitor(new PrintVisitor()));
    }

    // if the filters and order by are on QS, then the DataFetcherNode(s) would have been created
    // for QS, with limit and offset pushed down to QS
    // But, due to a Pinot limitation of not supporting offset when group by is specified, we need
    // to paginate the data in memory by adding a paginate only node
    if (sourceSetsIfFilterAndOrderByAreFromSameSourceSets.contains(QS.name())) {
      filterTree = createPaginateOnlyNode(filterTree, entitiesRequest);
    }

    filterTree.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));

    if (LOG.isDebugEnabled()) {
      LOG.debug("ExecutionContext: {}", executionContext);
    }

    /**
     * {@link FilterOptimizingVisitor} is needed to merge filters corresponding to the same source
     * into one {@link DataFetcherNode}, instead of having multiple {@link DataFetcherNode}s for
     * each filter
     */
    QueryNode optimizedFilterTree = filterTree.acceptVisitor(new FilterOptimizingVisitor());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Optimized Filter Tree:{}", optimizedFilterTree.acceptVisitor(new PrintVisitor()));
    }

    QueryNode executionTree = buildExecutionTree(executionContext, optimizedFilterTree);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Execution Tree:{}", executionTree.acceptVisitor(new PrintVisitor()));
    }

    return executionTree;
  }

  private QueryNode buildExecutionTreeForSameSourceFilterAndSelection(String source) {
    if (source.equals(QS.name())) {
      return buildExecutionTreeForQsFilterAndSelection();
    } else if (source.equals(EDS.name())) {
      return buildExecutionTreeForEdsFilterAndSelection();
    } else {
      throw new UnsupportedOperationException(
          "Unknown Entities data source. No fetcher for this source.");
    }
  }

  private QueryNode buildExecutionTreeForQsFilterAndSelection() {
    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();
    QueryNode rootNode = createQsDataFetcherNodeWithLimitAndOffset(entitiesRequest);
    rootNode = createPaginateOnlyNode(rootNode, entitiesRequest);
    executionContext.setSortAndPaginationNodeAdded(true);

    return rootNode;
  }

  private QueryNode buildExecutionTreeForEdsFilterAndSelection() {
    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();
    Filter filter = entitiesRequest.getFilter();
    int selectionLimit = entitiesRequest.getLimit();
    int selectionOffset = entitiesRequest.getOffset();
    List<OrderByExpression> orderBys = entitiesRequest.getOrderByList();
    boolean canFetchTotal = entitiesRequest.getFetchTotal();

    QueryNode rootNode =
        new DataFetcherNode(
            EDS.name(), filter, selectionLimit, selectionOffset, orderBys, canFetchTotal);
    executionContext.setSortAndPaginationNodeAdded(true);
    return rootNode;
  }

  @VisibleForTesting
  QueryNode buildExecutionTree(EntityExecutionContext executionContext, QueryNode filterTree) {
    QueryNode rootNode = filterTree;
    // Select attributes from sources in order by but not part of the filter tree
    Set<String> attrSourcesForOrderBy = executionContext.getPendingSelectionSourcesForOrderBy();
    if (!attrSourcesForOrderBy.isEmpty()) {
      rootNode =
          new SelectionNode.Builder(filterTree)
              .setAttrSelectionSources(attrSourcesForOrderBy)
              .build();
      attrSourcesForOrderBy.forEach(executionContext::removePendingSelectionSource);
    }

    // Select agg attributes from sources in order by
    Set<String> metricSourcesForOrderBy =
        executionContext.getPendingMetricAggregationSourcesForOrderBy();
    if (!metricSourcesForOrderBy.isEmpty()) {
      rootNode =
          new SelectionNode.Builder(rootNode)
              .setAggMetricSelectionSources(metricSourcesForOrderBy)
              .build();
      metricSourcesForOrderBy.forEach(executionContext::removePendingMetricAggregationSources);
    }

    // Try adding SortAndPaginateNode
    rootNode = checkAndAddSortAndPaginationNode(rootNode, executionContext);

    // Fetch all other attributes, metric agg and time series data
    if (!executionContext.getPendingSelectionSources().isEmpty()) {
      rootNode =
          new SelectionNode.Builder(rootNode)
              .setAttrSelectionSources(executionContext.getPendingSelectionSources())
              .build();
      // Handle case where there is no order by but pagination still needs to be done
      rootNode = checkAndAddSortAndPaginationNode(rootNode, executionContext);
    }

    if (!executionContext.getPendingMetricAggregationSources().isEmpty()) {
      rootNode =
          new SelectionNode.Builder(rootNode)
              .setAggMetricSelectionSources(executionContext.getPendingMetricAggregationSources())
              .build();
      rootNode = checkAndAddSortAndPaginationNode(rootNode, executionContext);
    }

    if (!executionContext.getPendingTimeAggregationSources().isEmpty()) {
      rootNode =
          new SelectionNode.Builder(rootNode)
              .setTimeSeriesSelectionSources(executionContext.getPendingTimeAggregationSources())
              .build();
      rootNode = checkAndAddSortAndPaginationNode(rootNode, executionContext);
    }

    return rootNode;
  }

  @VisibleForTesting
  QueryNode buildFilterTree(EntityExecutionContext context, Filter filter) {
    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();
    // Convert the time range into a filter and set it on the request so that all downstream
    // components needn't treat it specially
    Filter timeRangeFilter =
        TimeRangeFilterUtil.addTimeRangeFilter(
            context.getTimestampAttributeId(),
            filter,
            entitiesRequest.getStartTimeMillis(),
            entitiesRequest.getEndTimeMillis());

    return buildFilterTree(entitiesRequest, timeRangeFilter);
  }

  @VisibleForTesting
  QueryNode buildFilterTree(EntitiesRequest entitiesRequest, Filter filter) {
    if (filter.equals(Filter.getDefaultInstance())) {
      return new NoOpNode();
    }
    Operator operator = filter.getOperator();
    if (operator == Operator.AND) {
      return new AndNode(
          filter.getChildFilterList().stream()
              .map(childFilter -> buildFilterTree(entitiesRequest, childFilter))
              .collect(Collectors.toList()));
    } else if (operator == Operator.OR) {
      return new OrNode(
          filter.getChildFilterList().stream()
              .map(childFilter -> buildFilterTree(entitiesRequest, childFilter))
              .collect(Collectors.toList()));
    } else {
      List<AttributeSource> sources =
          attributeMetadataMap
              .get(
                  ExpressionReader.getAttributeIdFromAttributeSelection(filter.getLhs())
                      .orElseThrow())
              .getSourcesList();

      // if the filter by and order by are from QS, pagination can be pushed down to QS

      // There will always be a DataFetcherNode for QS, because the results are always fetched
      // within a time range. Hence, we can only push pagination down to QS and not any other
      // sources, since we will always have a time range filter on QS
      if (sourceSetsIfFilterAndOrderByAreFromSameSourceSets.contains(QS.name())) {
        executionContext.setSortAndPaginationNodeAdded(true);
        return createQsDataFetcherNodeWithLimitAndOffset(entitiesRequest);
      }

      return new DataFetcherNode(sources.contains(QS) ? QS.name() : sources.get(0).name(), filter);
    }
  }

  private QueryNode checkAndAddSortAndPaginationNode(
      QueryNode childNode, EntityExecutionContext executionContext) {
    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();
    // If sort/pagination node is already added or if the child is a NoOp don't add it
    if (executionContext.isSortAndPaginationNodeAdded() || childNode instanceof NoOpNode) {
      return childNode;
    }
    // Add ordering and pagination node
    List<OrderByExpression> selectionOrderByExpressions =
        executionContext
            .getExpressionContext()
            .getSourceToSelectionOrderByExpressionMap()
            .values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    List<OrderByExpression> metricOrderByExpressions =
        executionContext
            .getExpressionContext()
            .getSourceToMetricOrderByExpressionMap()
            .values()
            .stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    List<OrderByExpression> orderByExpressions =
        Stream.of(selectionOrderByExpressions, metricOrderByExpressions)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    if (orderByExpressions.isEmpty() && entitiesRequest.getLimit() == 0) {
      return childNode;
    }

    executionContext.setSortAndPaginationNodeAdded(true);
    return new SortAndPaginateNode(
        childNode, entitiesRequest.getLimit(), entitiesRequest.getOffset(), orderByExpressions);
  }

  private QueryNode createQsDataFetcherNodeWithLimitAndOffset(EntitiesRequest entitiesRequest) {
    Filter filter = entitiesRequest.getFilter();
    int selectionLimit = entitiesRequest.getLimit();
    int selectionOffset = entitiesRequest.getOffset();
    List<OrderByExpression> orderBys = entitiesRequest.getOrderByList();
    boolean canFetchTotal = entitiesRequest.getFetchTotal();

    // query-service/Pinot does not support offset when group by is specified. Since we will be
    // grouping by at least the entity id, we will compute the non zero pagination ourselves. This
    // means that we need to request for offset + limit rows so that we can paginate appropriately.
    // Pinot will do the ordering for us.
    // https://github.com/apache/incubator-pinot/issues/111#issuecomment-214810551
    if (selectionOffset > 0) {
      selectionLimit = selectionOffset + selectionLimit;
      selectionOffset = 0;
    }

    return new DataFetcherNode(
        QS.name(), filter, selectionLimit, selectionOffset, orderBys, canFetchTotal);
  }

  private QueryNode createPaginateOnlyNode(QueryNode queryNode, EntitiesRequest entitiesRequest) {
    return new PaginateOnlyNode(queryNode, entitiesRequest.getLimit(), entitiesRequest.getOffset());
  }
}
