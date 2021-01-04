package org.hypertrace.gateway.service.entity.query.visitor;

import java.util.stream.Collectors;
import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;

/**
 * Visitor that prints debug information of every QueryNode. Used primarily for logging and
 * debugging purpose
 */
public class PrintVisitor implements Visitor<String> {

  @Override
  public String visit(DataFetcherNode dataFetcherNode) {
    return "(" + dataFetcherNode.toString() + ")\n";
  }

  @Override
  public String visit(AndNode andNode) {
    return "("
        + andNode.getChildNodes().stream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.joining(" AND "))
        + ")\n";
  }

  @Override
  public String visit(OrNode orNode) {
    return "("
        + orNode.getChildNodes().stream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.joining(" OR "))
        + ")\n";
  }

  @Override
  public String visit(SelectionNode selectionNode) {
    return "SELECT("
        + selectionNode
        + ") --> \n"
        + selectionNode.getChildNode().acceptVisitor(this);
  }

  @Override
  public String visit(SortAndPaginateNode sortAndPaginateNode) {
    return "SORT_AND_PAGINATION("
        + sortAndPaginateNode
        + ") --> \n"
        + sortAndPaginateNode.getChildNode().acceptVisitor(this);
  }

  @Override
  public String visit(NoOpNode noOpNode) {
    return noOpNode.toString();
  }

  @Override
  public String visit(PaginateOnlyNode paginateOnlyNode) {
    return "PAGINATE_ONLY("
        + paginateOnlyNode
        + ") --> \n"
        + paginateOnlyNode.getChildNode().acceptVisitor(this);
  }
}
