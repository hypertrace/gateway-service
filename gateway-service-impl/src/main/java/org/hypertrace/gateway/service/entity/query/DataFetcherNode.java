package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;
import org.hypertrace.gateway.service.v1.common.Filter;

/**
 * Node in the execution tree that applies the encapsulated filters and fetches attributes
 * corresponding to a specific source
 */
public class DataFetcherNode implements QueryNode {

  private final String source;
  private final Filter filter;

  public DataFetcherNode(String source, Filter filter) {
    this.source = source;
    this.filter = filter;
  }

  public String getSource() {
    return source;
  }

  public Filter getFilter() {
    return filter;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "DataFetcherNode{" + "source='" + source + '\'' + ", filter=" + filter + '}';
  }
}
