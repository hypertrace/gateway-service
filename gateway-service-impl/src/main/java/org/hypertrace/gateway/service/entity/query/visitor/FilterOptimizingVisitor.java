package org.hypertrace.gateway.service.entity.query.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.OrNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.QueryNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;

/**
 * Visitor that optimizes the basic Filter Tree constructed from the Filter condition in the query
 *
 * <p>If there are multiple filter conditions for the same source, this visitor tries to optimize
 * the queries being made to the respective sources
 */
public class FilterOptimizingVisitor implements Visitor<QueryNode> {

  @Override
  public QueryNode visit(DataFetcherNode dataFetcherNode) {
    return dataFetcherNode;
  }

  @Override
  public QueryNode visit(AndNode andNode) {
    // First optimize all child nodes
    List<QueryNode> optimizedChildNodes =
        andNode.getChildNodes().stream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.toList());
    // Group the child nodes by their sources
    Map<String, List<QueryNode>> sourceToTreeNodeListMap = groupNodesBySource(optimizedChildNodes);
    List<QueryNode> andNodeList = sourceToTreeNodeListMap.remove(AndNode.class.getSimpleName());
    if (andNodeList != null) {
      andNodeList.forEach(
          treeNode -> {
            AndNode and = (AndNode) treeNode;
            Map<String, List<QueryNode>> childAndNodeMap = groupNodesBySource(and.getChildNodes());
            childAndNodeMap.forEach(
                (k, v) ->
                    sourceToTreeNodeListMap.merge(
                        k,
                        v,
                        (v1, v2) ->
                            Stream.concat(v1.stream(), v2.stream()).collect(Collectors.toList())));
          });
    }
    // Convert the List of QueryNode(s) for each group to a single QueryNode
    Map<String, QueryNode> sourceToTreeNodeMap =
        mergeQueryNodesBySource(sourceToTreeNodeListMap, Operator.AND);
    if (sourceToTreeNodeMap.size() == 1) {
      return sourceToTreeNodeMap.values().stream().findFirst().get();
    } else {
      return new AndNode(new ArrayList<>(sourceToTreeNodeMap.values()));
    }
  }

  @Override
  public QueryNode visit(OrNode orNode) {
    List<QueryNode> optimizedChildNodes =
        orNode.getChildNodes().stream()
            .map(n -> n.acceptVisitor(this))
            .collect(Collectors.toList());
    Map<String, List<QueryNode>> sourceToTreeNodeListMap = groupNodesBySource(optimizedChildNodes);
    List<QueryNode> orNodeList = sourceToTreeNodeListMap.remove(OrNode.class.getSimpleName());
    if (orNodeList != null) {
      orNodeList.forEach(
          treeNode -> {
            OrNode or = (OrNode) treeNode;
            Map<String, List<QueryNode>> childAndNodeMap = groupNodesBySource(or.getChildNodes());
            childAndNodeMap.forEach(
                (k, v) ->
                    sourceToTreeNodeListMap.merge(
                        k,
                        v,
                        (v1, v2) ->
                            Stream.concat(v1.stream(), v2.stream()).collect(Collectors.toList())));
          });
    }
    Map<String, QueryNode> sourceToTreeNodeMap =
        mergeQueryNodesBySource(sourceToTreeNodeListMap, Operator.OR);
    if (sourceToTreeNodeMap.size() == 1) {
      return sourceToTreeNodeMap.values().stream().findFirst().get();
    } else {
      return new OrNode(new ArrayList<>(sourceToTreeNodeMap.values()));
    }
  }

  private Map<String, List<QueryNode>> groupNodesBySource(List<QueryNode> nodes) {
    return nodes.stream()
        .collect(
            Collectors.groupingBy(
                treeNode1 -> {
                  if (treeNode1 instanceof DataFetcherNode) {
                    return ((DataFetcherNode) treeNode1).getSource();
                  } else {
                    return treeNode1.getClass().getSimpleName();
                  }
                }));
  }

  @Override
  public QueryNode visit(SelectionNode selectionNode) {
    QueryNode childNode = selectionNode.getChildNode().acceptVisitor(this);
    return new SelectionNode.Builder(childNode)
        .setTimeSeriesSelectionSources(selectionNode.getTimeSeriesSelectionSources())
        .setAggMetricSelectionSources(selectionNode.getAggMetricSelectionSources())
        .setAttrSelectionSources(selectionNode.getAttrSelectionSources())
        .build();
  }

  @Override
  public QueryNode visit(SortAndPaginateNode sortAndPaginateNode) {
    QueryNode childNode = sortAndPaginateNode.getChildNode().acceptVisitor(this);
    return new SortAndPaginateNode(
        childNode,
        sortAndPaginateNode.getLimit(),
        sortAndPaginateNode.getOffset(),
        sortAndPaginateNode.getOrderByExpressionList());
  }

  private Map<String, QueryNode> mergeQueryNodesBySource(
      Map<String, List<QueryNode>> sourceToTreeNodeListMap, Operator operator) {
    return sourceToTreeNodeListMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                stringListEntry -> {
                  if (stringListEntry.getKey().equals(AndNode.class.getSimpleName())) {
                    List<QueryNode> queryNodeList =
                        stringListEntry.getValue().stream()
                            .flatMap(treeNode -> ((AndNode) treeNode).getChildNodes().stream())
                            .collect(Collectors.toList());
                    return new AndNode(queryNodeList);
                  } else if (stringListEntry.getKey().equals(OrNode.class.getSimpleName())) {
                    List<QueryNode> queryNodeList =
                        stringListEntry.getValue().stream()
                            .flatMap(treeNode -> ((OrNode) treeNode).getChildNodes().stream())
                            .collect(Collectors.toList());
                    return new OrNode(queryNodeList);
                  } else {
                    List<Filter> filterList =
                        stringListEntry.getValue().stream()
                            .map(treeNode -> ((DataFetcherNode) treeNode).getFilter())
                            .filter(Objects::nonNull)
                            .filter(f -> !f.equals(Filter.getDefaultInstance()))
                            .collect(Collectors.toList());
                    Filter filter = Filter.getDefaultInstance();
                    if (filterList.size() == 1) {
                      filter = filterList.get(0);
                    } else if (filterList.size() > 1) {
                      filter =
                          Filter.newBuilder()
                              .setOperator(operator)
                              .addAllChildFilter(filterList)
                              .build();
                    }
                    // There should always be at least one entry
                    if (!stringListEntry.getValue().isEmpty()) {
                      String source = stringListEntry.getKey();
                      DataFetcherNode dataFetcherNode = (DataFetcherNode) stringListEntry.getValue().get(0);
                      return
                          new DataFetcherNode(
                              source,
                              filter,
                              dataFetcherNode.getLimit(),
                              dataFetcherNode.getOffset(),
                              dataFetcherNode.getOrderByExpressionList());
                    } else {
                      return new NoOpNode();
                    }
                  }
                }));
  }

  @Override
  public QueryNode visit(NoOpNode noOpNode) {
    return noOpNode;
  }

  @Override
  public QueryNode visit(PaginateOnlyNode paginateOnlyNode) {
    QueryNode childNode = paginateOnlyNode.getChildNode().acceptVisitor(this);
    return new PaginateOnlyNode(
        childNode, paginateOnlyNode.getLimit(), paginateOnlyNode.getOffset());
  }
}
