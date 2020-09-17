package org.hypertrace.gateway.service.entity.query;

import java.util.List;
import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

/**
 * Node corresponding to the AND condition in the query filter. Applies an AND between all the child
 * nodes
 */
public class AndNode implements QueryNode {
  private final List<QueryNode> childNodes;

  public AndNode(List<QueryNode> childNodes) {
    this.childNodes = childNodes;
  }

  public List<QueryNode> getChildNodes() {
    return childNodes;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "AndNode{" + "childNodes=" + childNodes + '}';
  }
}
