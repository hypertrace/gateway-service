package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

public class NoOpNode implements QueryNode {

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "NoOpNode";
  }
}
