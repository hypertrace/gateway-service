package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

public class TotalFetcherNode implements QueryNode {
  private final QueryNode childNode;
  private final String source;

  public TotalFetcherNode(QueryNode childNode, String source) {
    this.childNode = childNode;
    this.source = source;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "TotalFetcherNode{}";
  }

  public QueryNode getChildNode() {
    return childNode;
  }

  public String getSource() {
    return source;
  }
}
