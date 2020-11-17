package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

public class SelectionAndFilterNode implements QueryNode {

  private final String source;
  private final int limit;
  private final int offset;

  public SelectionAndFilterNode(String source, int limit, int offset) {
    this.source = source;
    this.limit = limit;
    this.offset = offset;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  public String getSource() {
    return source;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "SelectionAndFilterNode{"
        + "source="
        + source
        + ", limit="
        + limit
        + ", offset="
        + offset
        + '}';
  }
}
