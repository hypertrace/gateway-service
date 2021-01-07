package org.hypertrace.gateway.service.entity.query.visitor;

import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExecutionContextBuilderVisitorTest {
  private ExecutionContext executionContext;

  @BeforeEach
  public void setup() {
    this.executionContext = mock(ExecutionContext.class);
  }

  @Test
  public void shouldReturnSourcesForFetchedAttributes_DataFetcherNode() {
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);
    Set<String> attributeSources = dataFetcherNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));
    verify(executionContext).removePendingSelectionSource("QS");
    verify(executionContext).removePendingSelectionSourceForOrderBy("QS");
    assertEquals(Set.of("QS"), attributeSources);
  }


  @Test
  public void shouldReturnSourcesForFetchedAttributes_AndNode() {
    //              SelectionNode("EDS")
    //                     |
    //                  AndNode
    //             /                \
    // DataFetcherNode("QS1")   DataFetcherNode("QS2")

    DataFetcherNode dataFetcherNode1 = new DataFetcherNode("QS1", null);
    DataFetcherNode dataFetcherNode2 = new DataFetcherNode("QS2", null);
    AndNode andNode = new AndNode(List.of(dataFetcherNode1, dataFetcherNode2));
    SelectionNode selectionNode =
        new SelectionNode.Builder(andNode).setAttrSelectionSources(Set.of("EDS")).build();
    Set<String> attributeSources =
        selectionNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));
    assertEquals(Set.of("QS1", "QS2", "EDS"), attributeSources);
  }
}
