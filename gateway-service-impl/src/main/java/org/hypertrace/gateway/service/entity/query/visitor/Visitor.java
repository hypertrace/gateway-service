package org.hypertrace.gateway.service.entity.query.visitor;

import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.QueryNode;
import org.hypertrace.gateway.service.entity.query.SelectionAndFilterNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
import org.hypertrace.gateway.service.entity.query.TotalFetcherNode;

/**
 * Visitor interface for visiting every type of {@link QueryNode}
 *
 * @param <R>
 */
public interface Visitor<R> {
  R visit(DataFetcherNode dataFetcherNode);

  R visit(AndNode andNode);

  R visit(OrNode orNode);

  R visit(SelectionNode selectionNode);

  R visit(SortAndPaginateNode sortAndPaginateNode);

  R visit(NoOpNode noOpNode);

  R visit(SelectionAndFilterNode selectionAndFilterNode);

  R visit(PaginateOnlyNode paginateOnlyNode);

  R visit(TotalFetcherNode totalFetcherNode);
}
