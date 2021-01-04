package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;
import org.hypertrace.gateway.service.v1.common.Filter;

public class TotalFetcherNode implements QueryNode {
  private final QueryNode childNode;
  private final String source;
  private final Filter filter;

  public TotalFetcherNode(QueryNode childNode, String source, Filter filter) {
    this.childNode = childNode;
    this.source = source;
    this.filter = filter;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  public QueryNode getChildNode() {
    return childNode;
  }

  public String getSource() {
    return source;
  }

  public Filter getFilter() {
    return filter;
  }

  @Override
  public String toString() {
    return "TotalFetcherNode{" +
        "childNode=" + childNode +
        ", source='" + source + '\'' +
        ", filter=" + filter +
        '}';
  }
}
