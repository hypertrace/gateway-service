package org.hypertrace.gateway.service.entity.query.visitor;

import org.hypertrace.gateway.service.common.util.TimeRangeFilterUtil;
import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.EntityExecutionContext;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.QueryNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
import org.hypertrace.gateway.service.v1.common.Filter;

/** Visitor that stores the filter tree per source in the query */
public class SourceFilterVisitor implements Visitor<QueryNode> {

  private final EntityExecutionContext executionContext;

  public SourceFilterVisitor(EntityExecutionContext executionContext) {
    this.executionContext = executionContext;
  }

  @Override
  public QueryNode visit(DataFetcherNode dataFetcherNode) {
    Filter timeRangeFilter =
        TimeRangeFilterUtil.addTimeRangeFilter(
            executionContext.getTimestampAttributeId(),
            Filter.newBuilder().build(),
            executionContext.getEntitiesRequest().getStartTimeMillis(),
            executionContext.getEntitiesRequest().getEndTimeMillis());
    // we don't need to store timestamp filter
    if (!timeRangeFilter.equals(dataFetcherNode.getFilter())) {
      // if it is not a time-range filter, then we don't need to store it
      executionContext
          .getExpressionContext()
          .putSourceToFilterMap(dataFetcherNode.getSource(), dataFetcherNode.getFilter());
    }
    return dataFetcherNode;
  }

  @Override
  public QueryNode visit(AndNode andNode) {
    andNode.getChildNodes().forEach(childNode -> childNode.acceptVisitor(this));
    return andNode;
  }

  @Override
  public QueryNode visit(OrNode orNode) {
    orNode.getChildNodes().forEach(childNode -> childNode.acceptVisitor(this));
    return orNode;
  }

  @Override
  public QueryNode visit(SelectionNode selectionNode) {
    selectionNode.getChildNode().acceptVisitor(this);
    return selectionNode;
  }

  @Override
  public QueryNode visit(SortAndPaginateNode sortAndPaginateNode) {
    sortAndPaginateNode.getChildNode().acceptVisitor(this);
    return sortAndPaginateNode;
  }

  @Override
  public QueryNode visit(NoOpNode noOpNode) {
    return noOpNode;
  }

  @Override
  public QueryNode visit(PaginateOnlyNode paginateOnlyNode) {
    paginateOnlyNode.getChildNode().acceptVisitor(this);
    return paginateOnlyNode;
  }
}
