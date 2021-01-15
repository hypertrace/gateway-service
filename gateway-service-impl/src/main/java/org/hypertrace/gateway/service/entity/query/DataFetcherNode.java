package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.gateway.service.entity.query.visitor.Visitor;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;

import java.util.Collections;
import java.util.List;

/**
 * Node in the execution tree that applies the encapsulated filters and fetches attributes
 * corresponding to a specific source
 *
 * Also, applies limit, offset and orderBys, if present
 */
public class DataFetcherNode implements QueryNode {

  private final String source;
  private final Filter filter;
  private Integer limit;
  private Integer offset;
  private List<OrderByExpression> orderByExpressionList = Collections.emptyList();

  private final boolean isPaginated;

  public DataFetcherNode(String source, Filter filter) {
    this.source = source;
    this.filter = filter;
    this.isPaginated = false;
  }

  public DataFetcherNode(
      String source,
      Filter filter,
      Integer limit,
      Integer offset,
      List<OrderByExpression> orderByExpressionList) {
    this.source = source;
    this.filter = filter;
    this.limit = limit;
    this.offset = offset;
    this.orderByExpressionList = orderByExpressionList;
    this.isPaginated = limit != null && offset != null;
  }

  public String getSource() {
    return source;
  }

  public Filter getFilter() {
    return filter;
  }

  public Integer getLimit() {
    return limit;
  }

  public Integer getOffset() {
    return offset;
  }

  public List<OrderByExpression> getOrderByExpressionList() {
    return orderByExpressionList;
  }

  public boolean isPaginated() {
    return isPaginated;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "DataFetcherNode{" +
        "source='" + source + '\'' +
        ", filter=" + filter +
        ", limit=" + limit +
        ", offset=" + offset +
        ", orderByExpressionList=" + orderByExpressionList +
        ", isPaginated=" + isPaginated +
        '}';
  }
}
