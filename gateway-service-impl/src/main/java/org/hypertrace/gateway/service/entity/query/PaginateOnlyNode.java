package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

public class PaginateOnlyNode implements QueryNode {
  private final int limit;
  private final int offset;
  private final QueryNode childNode;

  public PaginateOnlyNode(QueryNode childNode, int limit, int offset) {
    this.limit = limit;
    this.offset = offset;
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

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "PAGINATE_ONLY_NODE{"
        + "limit="
        + limit
        + ", offset="
        + offset
        + '}';
  }
}
