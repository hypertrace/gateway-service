package org.hypertrace.gateway.service.entity.query.visitor;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.ExecutionTreeUtils;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
import org.hypertrace.gateway.service.entity.query.TotalFetcherNode;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class AttributeSelectionOptimizingVisitor implements Visitor<Set<String>> {

  private final ExecutionContext executionContext;

  public AttributeSelectionOptimizingVisitor(ExecutionContext executionContext) {
    this.executionContext = executionContext;
  }

  @Override
  public Set<String> visit(DataFetcherNode dataFetcherNode) {
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

  /**
   * Removes the attribute selection source from SelectionNode, if the attribute has already been
   * requested from a different source
   *
   * Example: select api.id, api.name
   *
   * api.id -> ["QS", "EDS"] api.name -> ["QS", EDS"]
   *
   * This would lead to the following execution tree DataFetcherNode("QS") ->
   * SelectionNode("EDS")
   *
   * Since, DataFetcherNode will fetch api.id, api.name from QS, there is no point fetching
   * the same set of attributes from EDS
   *
   * Algorithm:
   * - Collect all the sources from child query nodes,
   * which denotes the set of sources for which the attributes have been fetched
   * - Generate a set of the attributes fetched from the above collected sources
   *
   * The above steps will give us a set of attributes(say A) that have already been
   * fetched below in the execution tree
   *
   * - Get the set of attribute selection sources from the current selection node
   * - for each attribute selection source S, get all the selection attributes for the source
   * - if the selection attributes for that source are already present in the set A,
   * then this source is redundant to fetch the attributes
   * - then remove the attribute selection S from the current selection node
   */
  @Override
  public Set<String> visit(SelectionNode selectionNode) {
    // set of sources from which the attributes have already been fetched
    Set<String> childAttributeSelectionSources = selectionNode.getChildNode().acceptVisitor(this);

    Set<String> attributeSelectionSources = selectionNode.getAttrSelectionSources();
    if (attributeSelectionSources.isEmpty()) {
      return childAttributeSelectionSources;
    }

    // map of selection attributes to sources map
    Map<String, Set<String>> attributeSelectionToSourceMap =
        ExecutionTreeUtils.buildAttributeToSourcesMap(
            executionContext.getSourceToSelectionExpressionMap());

    // set of attributes which were fetched from child attribute sources
    Set<String> attributesFromChildAttributeSources =
        attributeSelectionToSourceMap.entrySet().stream()
            .filter(
                entry ->
                    !Sets.intersection(entry.getValue(), childAttributeSelectionSources).isEmpty())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

    // reverse of attributeSelectionToSourceMap
    // map of source to attribute selection map
    Map<String, Set<String>> sourceToAttributeSelectionMap =
        ExecutionTreeUtils.buildSourceToAttributesMap(
            executionContext.getSourceToSelectionExpressionMap());

    Set<String> retainedAttributeSelectionSources = new HashSet<>();
    for (String source: attributeSelectionSources) {
      Set<String> selectionAttributesFromSource = sourceToAttributeSelectionMap.get(source);
      // if all the attributes from the selection source have already been fetched,
      // remove the source from selection node, so that it does not fetch the same
      // set of attributes again
      if (!attributesFromChildAttributeSources.containsAll(selectionAttributesFromSource)) {
        retainedAttributeSelectionSources.add(source);
      }
    }

    selectionNode.setAttrSelectionSources(retainedAttributeSelectionSources);

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
    return Collections.emptySet();
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
