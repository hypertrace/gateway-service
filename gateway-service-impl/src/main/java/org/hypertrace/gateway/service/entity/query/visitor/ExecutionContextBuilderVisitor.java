package org.hypertrace.gateway.service.entity.query.visitor;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
import org.hypertrace.gateway.service.entity.query.TotalFetcherNode;

import java.util.Collections;
import java.util.Set;

/**
 * Visitor for capturing the different sources corresponding to the expressions in the filter tree
 *
 * Returns set of sources for which attributes have already been fetched
 */
public class ExecutionContextBuilderVisitor implements Visitor<Set<String>> {

  private final ExecutionContext executionContext;

  public ExecutionContextBuilderVisitor(ExecutionContext executionContext) {
    this.executionContext = executionContext;
  }

  @Override
  public Set<String> visit(DataFetcherNode dataFetcherNode) {
    executionContext.removePendingSelectionSource(dataFetcherNode.getSource());
    executionContext.removePendingSelectionSourceForOrderBy(dataFetcherNode.getSource());
    return Set.of(dataFetcherNode.getSource());
  }

  @Override
  public Set<String> visit(AndNode andNode) {
    // We just want to return all the sources that have been used to fetch the attributes
    // from DataFetcherNode. Hence, union of all the source sets
    return andNode.getChildNodes().stream()
        .map(n -> n.acceptVisitor(this))
        .reduce(Sets::union)
        .orElse(Collections.emptySet());
  }

  @Override
  public Set<String> visit(OrNode orNode) {
    // We just want to return all the sources that have been used to fetch the attributes
    // from DataFetcherNode. Hence, union of all the source sets
    return orNode.getChildNodes().stream()
        .map(n -> n.acceptVisitor(this))
        .reduce(Sets::union)
        .orElse(Collections.emptySet());
  }

  @Override
  public Set<String> visit(SelectionNode selectionNode) {
    Set<String> childAttributeSelectionSources = selectionNode.getChildNode().acceptVisitor(this);
    Set<String> attributeSelectionSources = selectionNode.getAttrSelectionSources();

    // return all the set of sources for which the attributes have been fetched
    return Sets.newHashSet(
        Iterables.concat(childAttributeSelectionSources, attributeSelectionSources));
  }

  @Override
  public Set<String> visit(SortAndPaginateNode sortAndPaginateNode) {
    return sortAndPaginateNode.getChildNode().acceptVisitor(this);
  }

  @Override
  public Set<String> visit(NoOpNode noOpNode) {
    return null;
  }

  @Override
  public Set<String> visit(PaginateOnlyNode paginateOnlyNode) {
    return paginateOnlyNode.getChildNode().acceptVisitor(this);
  }

  @Override
  public Set<String> visit(TotalFetcherNode totalFetcherNode) {
    return totalFetcherNode.getChildNode().acceptVisitor(this);
  }
}
