package org.hypertrace.gateway.service.entity.query.visitor;

import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildOrderByExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateEQFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.QueryNode;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class FilterOptimizingVisitorTest {
  @Test
  public void testPaginateOnlyNode() {
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", Filter.getDefaultInstance());
    PaginateOnlyNode paginateOnlyNode = new PaginateOnlyNode(dataFetcherNode, 10, 10);
    FilterOptimizingVisitor filterOptimizingVisitor = new FilterOptimizingVisitor();
    PaginateOnlyNode visitedPaginatedOnlyNode = (PaginateOnlyNode) filterOptimizingVisitor.visit(paginateOnlyNode);
    assertEquals(paginateOnlyNode.getChildNode(), visitedPaginatedOnlyNode.getChildNode());
    assertEquals(paginateOnlyNode.getLimit(), visitedPaginatedOnlyNode.getLimit());
    assertEquals(paginateOnlyNode.getOffset(), visitedPaginatedOnlyNode.getOffset());
  }

  @Test
  public void testAndNodes_dataFetcherPaginationNodes() {
    Filter filter1 = generateEQFilter("API.name", "apiName1");
    Filter filter2 = generateEQFilter("API.id", "apiId1");
    int limit = 10;
    int offset = 5;
    List<OrderByExpression> orderByExpressions = Collections.singletonList(buildOrderByExpression("API.id"));
    DataFetcherNode dataFetcherNode1 =
        new DataFetcherNode("QS", filter1, limit, offset, orderByExpressions);
    DataFetcherNode dataFetcherNode2 =
        new DataFetcherNode("QS", filter2, limit, offset, orderByExpressions);
    AndNode andNode = new AndNode(List.of(dataFetcherNode1, dataFetcherNode2));
    QueryNode queryNode = andNode.acceptVisitor(new FilterOptimizingVisitor());

    DataFetcherNode mergedDataFetcherNode = (DataFetcherNode) queryNode;
    assertEquals("QS", mergedDataFetcherNode.getSource());
    assertEquals(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .build(),
        mergedDataFetcherNode.getFilter());
    assertEquals(limit, mergedDataFetcherNode.getLimit());
    assertEquals(offset, mergedDataFetcherNode.getOffset());
    assertEquals(orderByExpressions, mergedDataFetcherNode.getOrderByExpressionList());
  }

  @Test
  public void testAndNodes_dataFetcherNonPaginationNodes() {
    Filter filter1 = generateEQFilter("API.name", "apiName1");
    Filter filter2 = generateEQFilter("API.id", "apiId1");
    DataFetcherNode dataFetcherNode1 = new DataFetcherNode("QS", filter1);
    DataFetcherNode dataFetcherNode2 = new DataFetcherNode("QS", filter2);
    AndNode andNode = new AndNode(List.of(dataFetcherNode1, dataFetcherNode2));
    QueryNode queryNode = andNode.acceptVisitor(new FilterOptimizingVisitor());

    DataFetcherNode mergedDataFetcherNode = (DataFetcherNode) queryNode;
    assertEquals("QS", mergedDataFetcherNode.getSource());
    assertEquals(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(filter2)
            .build(),
        mergedDataFetcherNode.getFilter());
    assertNull(mergedDataFetcherNode.getLimit());
    assertNull(mergedDataFetcherNode.getOffset());
    assertTrue(mergedDataFetcherNode.getOrderByExpressionList().isEmpty());
  }
}
