package org.hypertrace.gateway.service.entity.query;

import java.util.List;
import org.hypertrace.gateway.service.entity.query.visitor.Visitor;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;

public class SelectionAndFilterNode implements QueryNode {

  private final String source;
  private final int limit;
  private final int offset;
  private final List<OrderByExpression> orderBys;

  public SelectionAndFilterNode(String source, int limit, int offset, List<OrderByExpression> orderBys) {
    this.source = source;
    this.limit = limit;
    this.offset = offset;
    this.orderBys = orderBys;
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

  public List<OrderByExpression> getOrderBys() {
    return orderBys;
  }

  @Override
  public String toString() {
    return "SelectionAndFilterNode{"
        + "limit="
        + limit
        + ", offset="
        + offset
        + ", orderBys="
        + orderBys
        + '}';
  }
}
