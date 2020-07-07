package org.hypertrace.gateway.service.entity.query.visitor;

import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;

/**
 * Visitor for capturing the different sources corresponding to the expressions in the filter tree
 */
public class ExecutionContextBuilderVisitor implements Visitor<Void> {

  private ExecutionContext executionContext;

  public ExecutionContextBuilderVisitor(ExecutionContext executionContext) {
    this.executionContext = executionContext;
  }

  @Override
  public Void visit(DataFetcherNode dataFetcherNode) {
    executionContext.removePendingSelectionSource(dataFetcherNode.getSource());
    executionContext.removePendingSelectionSourceForOrderBy(dataFetcherNode.getSource());
    return null;
  }

  @Override
  public Void visit(AndNode andNode) {
    andNode.getChildNodes().forEach(n -> n.acceptVisitor(this));
    return null;
  }

  @Override
  public Void visit(OrNode orNode) {
    orNode.getChildNodes().forEach(n -> n.acceptVisitor(this));
    return null;
  }

  @Override
  public Void visit(SelectionNode selectionNode) {
    return null;
  }

  @Override
  public Void visit(SortAndPaginateNode sortAndPaginateNode) {
    return null;
  }

  @Override
  public Void visit(NoOpNode noOpNode) {
    return null;
  }
}
