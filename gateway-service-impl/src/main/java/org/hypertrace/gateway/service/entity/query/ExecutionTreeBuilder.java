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
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.util.TimeRangeFilterUtil;
import org.hypertrace.gateway.service.entity.query.visitor.ExecutionContextBuilderVisitor;
import org.hypertrace.gateway.service.entity.query.visitor.OptimizingVisitor;
import org.hypertrace.gateway.service.entity.query.visitor.PrintVisitor;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class used to build the execution tree.
 */
public class ExecutionTreeBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionTreeBuilder.class);

  private final Map<String, AttributeMetadata> attributeMetadataMap;
  private final ExecutionContext executionContext;

  public ExecutionTreeBuilder(ExecutionContext executionContext) {
    this.executionContext = executionContext;
    this.attributeMetadataMap =
        executionContext
            .getAttributeMetadataProvider()
            .getAttributesMetadata(
                executionContext.getEntitiesRequestContext(),
                executionContext.getEntitiesRequest().getEntityType());
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
    // All expressions' attributes from the same source. Will only need one downstream query.
    Optional<String> singleSourceForAllAttributes = ExecutionTreeUtils.getSingleSourceForAllAttributes(executionContext);

    EntitiesRequest entitiesRequest = executionContext.getEntitiesRequest();

    // TODO: If there is a filter on a data source, other than EDS, then the flag is a no-op
    if (entitiesRequest.getIncludeResultsOutsideTimeRange()) {
      QueryNode rootNode = new DataFetcherNode(EDS.name(), entitiesRequest.getFilter());
      executionContext.removePendingSelectionSource(EDS.name());
      executionContext.removePendingSelectionSourceForOrderBy(EDS.name());

      return buildExecutionTree(executionContext, rootNode);
    }

    if (singleSourceForAllAttributes.isPresent()) {
      String source = singleSourceForAllAttributes.get();
      QueryNode selectionAndFilterNode = buildExecutionTreeForSameSourceFilterAndSelection(source);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Execution Tree:{}", selectionAndFilterNode.acceptVisitor(new PrintVisitor()));
      }

      return selectionAndFilterNode;
    }

    QueryNode filterTree = buildFilterTree(executionContext, entitiesRequest.getFilter());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Filter Tree:{}", filterTree.acceptVisitor(new PrintVisitor()));
    }

    filterTree.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));
    if (LOG.isDebugEnabled()) {
      LOG.debug("ExecutionContext: {}", executionContext);
    }

    QueryNode optimizedFilterTree = filterTree.acceptVisitor(new OptimizingVisitor());
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
      return buildExecutionTreeForQsFilterAndSelection(source);
    } else if (source.equals(EDS.name())) {
      return buildExecutionTreeForEdsFilterAndSelection(source);
    } else {
      throw new UnsupportedOperationException("Unknown Entities data source. No fetcher for this source.");
    }
  }

  private QueryNode buildExecutionTreeForQsFilterAndSelection(String source) {
    int selectionLimit = executionContext.getEntitiesRequest().getLimit();
    int selectionOffset = executionContext.getEntitiesRequest().getOffset();

    // query-service/Pinot does not support offset when group by is specified. Since we will be
    // grouping by at least the entity id, we will compute the non zero pagination ourselves. This
    // means that we need to request for offset + limit rows so that we can paginate appropriately.
    // Pinot will do the ordering for us.
    if (selectionOffset > 0) {
      selectionLimit = selectionOffset + selectionLimit;
      selectionOffset = 0;
    }

    QueryNode rootNode = new SelectionAndFilterNode(source, selectionLimit, selectionOffset);
    if (executionContext.getEntitiesRequest().getOffset() > 0) {
      rootNode = new PaginateOnlyNode(
          rootNode,
          executionContext.getEntitiesRequest().getLimit(),
          executionContext.getEntitiesRequest().getOffset()
      );
    }

    // If the request has an EntityId EQ filter then there's no need for the 2nd request to get the
    // total entities. So no need to set the TotalFetcherNode
    if (ExecutionTreeUtils.hasEntityIdEqualsFilter(executionContext)) {
      executionContext.setTotal(1);
    } else {
      rootNode = new TotalFetcherNode(rootNode, source);
    }

    return rootNode;
  }

  private QueryNode buildExecutionTreeForEdsFilterAndSelection(String source) {
    int selectionLimit = executionContext.getEntitiesRequest().getLimit();
    int selectionOffset = executionContext.getEntitiesRequest().getOffset();
    List<OrderByExpression> orderBys = executionContext.getEntitiesRequest().getOrderByList();

    QueryNode rootNode = new SelectionAndFilterNode(source, selectionLimit, selectionOffset);
    // EDS does not seem to do orderby, limiting and offsetting. We will have to do it ourselves.
    rootNode = new SortAndPaginateNode(rootNode, selectionLimit, selectionOffset, orderBys);

    return rootNode;
  }

  @VisibleForTesting
  QueryNode buildExecutionTree(ExecutionContext executionContext, QueryNode filterTree) {
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
  QueryNode buildFilterTree(ExecutionContext context, Filter filter) {
    // Convert the time range into a filter and set it on the request so that all downstream
    // components needn't treat it specially
    Filter timeRangeFilter =
        TimeRangeFilterUtil.addTimeRangeFilter(
            context.getTimestampAttributeId(),
            filter,
            context.getEntitiesRequest().getStartTimeMillis(),
            context.getEntitiesRequest().getEndTimeMillis());

    return buildFilterTree(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(timeRangeFilter)
            .addChildFilter(filter)
            .build());
  }

  @VisibleForTesting
  QueryNode buildFilterTree(Filter filter) {
    if (filter.equals(Filter.getDefaultInstance())) {
      return new NoOpNode();
    }
    Operator operator = filter.getOperator();
    if (operator == Operator.AND) {
      return new AndNode(
          filter.getChildFilterList().stream()
              .map(this::buildFilterTree)
              .collect(Collectors.toList()));
    } else if (operator == Operator.OR) {
      return new OrNode(
          filter.getChildFilterList().stream()
              .map(this::buildFilterTree)
              .collect(Collectors.toList()));
    } else {
      List<AttributeSource> sources =
          attributeMetadataMap
              .get(filter.getLhs().getColumnIdentifier().getColumnName())
              .getSourcesList();
      return new DataFetcherNode(sources.contains(QS) ? QS.name() : sources.get(0).name(), filter);
    }
  }

  private QueryNode checkAndAddSortAndPaginationNode(
      QueryNode childNode, ExecutionContext executionContext) {
    // If sort/pagination node is already added or if the child is a NoOp don't add it
    if (executionContext.isSortAndPaginationNodeAdded() || childNode instanceof NoOpNode) {
      return childNode;
    }
    // Add ordering and pagination node
    List<OrderByExpression> orderByExpressions =
        executionContext.getSourceToOrderByExpressionMap().values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    if (orderByExpressions.isEmpty() && executionContext.getEntitiesRequest().getLimit() == 0) {
      return childNode;
    }

    executionContext.setSortAndPaginationNodeAdded(true);
    return new SortAndPaginateNode(
        childNode,
        executionContext.getEntitiesRequest().getLimit(),
        executionContext.getEntitiesRequest().getOffset(),
        orderByExpressions);
  }
}
