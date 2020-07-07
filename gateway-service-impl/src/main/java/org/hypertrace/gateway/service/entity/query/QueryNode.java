package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

/** A node in the query execution tree */
public interface QueryNode {

  <R> R acceptVisitor(Visitor<R> v);
}
