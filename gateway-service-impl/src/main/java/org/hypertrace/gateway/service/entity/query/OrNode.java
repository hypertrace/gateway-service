package org.hypertrace.gateway.service.entity.query;

import java.util.List;
import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

/**
 * Node corresponding to the OR condition in the query filter. Applies an OR between all the child
 * nodes
 */
public class OrNode implements QueryNode {
  private final List<QueryNode> childNodes;

  public OrNode(List<QueryNode> childNodes) {
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
    return "OrNode{" + "childNodes=" + childNodes + '}';
  }
}
