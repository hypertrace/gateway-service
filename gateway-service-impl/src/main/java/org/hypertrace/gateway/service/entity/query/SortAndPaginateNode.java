package org.hypertrace.gateway.service.entity.query;

import java.util.List;
import org.hypertrace.gateway.service.entity.query.visitor.Visitor;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;

/**
 * Node that takes care of ordering the result set (if applicable) and also paginating the results
 */
public class SortAndPaginateNode implements QueryNode {
  private final int limit;
  private final int offset;
  private final List<OrderByExpression> orderByExpressionList;
  private final QueryNode childNode;

  public SortAndPaginateNode(
      QueryNode childNode, int limit, int offset, List<OrderByExpression> orderByExpressionList) {
    this.limit = limit;
    this.offset = offset;
    this.orderByExpressionList = orderByExpressionList;
    this.childNode = childNode;
  }

  public QueryNode getChildNode() {
    return childNode;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public List<OrderByExpression> getOrderByExpressionList() {
    return orderByExpressionList;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "SortAndPaginateNode{"
        + "limit="
        + limit
        + ", offset="
        + offset
        + ", orderByExpressionList="
        + orderByExpressionList
        + ", childNode="
        + childNode
        + '}';
  }
}
