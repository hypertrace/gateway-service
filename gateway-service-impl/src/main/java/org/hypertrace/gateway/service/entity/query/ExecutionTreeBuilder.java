package org.hypertrace.gateway.service.entity.query;

import static org.hypertrace.core.attribute.service.v1.AttributeSource.QS;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.entity.query.visitor.ExecutionContextBuilderVisitor;
import org.hypertrace.gateway.service.entity.query.visitor.OptimizingVisitor;
import org.hypertrace.gateway.service.entity.query.visitor.PrintVisitor;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class used to build the execution tree.
 */
public class ExecutionTreeBuilder {

  private static Logger LOG = LoggerFactory.getLogger(ExecutionTreeBuilder.class);

  private final Map<String, AttributeMetadata> attributeMetadataMap;
  private final ExecutionContext executionContext;

  public ExecutionTreeBuilder(ExecutionContext executionContext) {
    this.executionContext = executionContext;
    this.attributeMetadataMap =
        executionContext
            .getAttributeMetadataProvider()
            .getAttributesMetadata(
                executionContext.getEntitiesRequestContext(),
                AttributeScope.valueOf(executionContext.getEntitiesRequest().getEntityType()));
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
    QueryNode filterTree = buildFilterTree(executionContext.getEntitiesRequest().getFilter());
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

    if (isSingleSourceAndSame(executionContext.getPendingSelectionSources(), executionContext.getPendingMetricAggregationSources())) {
      rootNode =
          new SelectionNode.Builder(rootNode)
              .setAttrSelectionSources(executionContext.getPendingSelectionSources())
              .setAggMetricSelectionSources(executionContext.getPendingMetricAggregationSources())
              .build();
      // Handle case where there is no order by but pagination still needs to be done
      rootNode = checkAndAddSortAndPaginationNode(rootNode, executionContext);
    } else {
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
    executionContext.setSortAndPaginationNodeAdded(true);
    return new SortAndPaginateNode(
        childNode,
        executionContext.getEntitiesRequest().getLimit(),
        executionContext.getEntitiesRequest().getOffset(),
        orderByExpressions);
  }

  boolean isSingleSourceAndSame(Set<String> firstSource, Set<String> secondSource) {
    return firstSource.size() == 1 &&
        firstSource.equals(secondSource);
  }
}
