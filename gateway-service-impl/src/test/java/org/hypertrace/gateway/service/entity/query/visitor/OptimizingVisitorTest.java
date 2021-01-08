package org.hypertrace.gateway.service.entity.query.visitor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.TotalFetcherNode;
import org.junit.jupiter.api.Test;

public class OptimizingVisitorTest {
  @Test
  public void testPaginateOnlyNode() {
    PaginateOnlyNode paginateOnlyNode = mock(PaginateOnlyNode.class);
    FilterOptimizingVisitor filterOptimizingVisitor = new FilterOptimizingVisitor();
    assertEquals(paginateOnlyNode, filterOptimizingVisitor.visit(paginateOnlyNode));
  }

  @Test
  public void testTotalFetcherNode() {
    TotalFetcherNode totalFetcherNode = mock(TotalFetcherNode.class);
    FilterOptimizingVisitor filterOptimizingVisitor = new FilterOptimizingVisitor();
    assertEquals(totalFetcherNode, filterOptimizingVisitor.visit(totalFetcherNode));
  }
}
