package org.hypertrace.gateway.service.entity.query.visitor;

import org.hypertrace.gateway.service.entity.query.AndNode;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AttributeSelectionOptimizingVisitorTest {
  private ExecutionContext executionContext;

  @BeforeEach
  public void setup() {
    this.executionContext = mock(ExecutionContext.class);
  }

  @Test
  public void shouldRemove_attributeSelectionSource() {
    // DataFetcherNode("QS") -> SelectionNode(["EDS"])
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);
    SelectionNode selectionNode =
        new SelectionNode.Builder(dataFetcherNode).setAttrSelectionSources(Set.of("EDS")).build();

    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS", "EDS"]
    when(executionContext.getSourceToSelectionExpressionMap())
        .thenReturn(
            Map.of(
                "QS",
                List.of(
                    createExpressionFromColumnName("API.id"),
                    createExpressionFromColumnName("API.name")),
                "EDS",
                List.of(
                    createExpressionFromColumnName("API.id"),
                    createExpressionFromColumnName("API.name"))));
    Set<String> attributeSources =
        selectionNode.acceptVisitor(new AttributeSelectionOptimizingVisitor(executionContext));
    assertEquals(Set.of("QS", "EDS"), attributeSources);
    assertTrue(selectionNode.getAttrSelectionSources().isEmpty());
  }

  @Test
  public void shouldNotRemove_attributeSelectionSource() {
    // DataFetcherNode("QS") -> SelectionNode(["EDS"])
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);
    SelectionNode selectionNode =
        new SelectionNode.Builder(dataFetcherNode).setAttrSelectionSources(Set.of("EDS")).build();

    // API.id -> ["QS", "EDS"]
    // API.name -> ["EDS"]
    when(executionContext.getSourceToSelectionExpressionMap())
        .thenReturn(
            Map.of(
                "QS",
                List.of(createExpressionFromColumnName("API.id")),
                "EDS",
                List.of(
                    createExpressionFromColumnName("API.id"),
                    createExpressionFromColumnName("API.name"))));
    Set<String> attributeSources =
        selectionNode.acceptVisitor(new AttributeSelectionOptimizingVisitor(executionContext));
    assertEquals(Set.of("QS", "EDS"), attributeSources);
    assertEquals(Set.of("EDS"), selectionNode.getAttrSelectionSources());
  }

  @Test
  public void shouldOnlyRemove_attributeSelectionSource_fetchedBefore() {
    // DataFetcherNode("QS") -> SelectionNode(["EDS", "AS"])
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);
    SelectionNode selectionNode =
        new SelectionNode.Builder(dataFetcherNode)
            .setAttrSelectionSources(Set.of("EDS", "AS"))
            .build();

    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS", "EDS"]
    // API.status -> ["AS"]
    when(executionContext.getSourceToSelectionExpressionMap())
        .thenReturn(
            Map.of(
                "QS",
                List.of(
                    createExpressionFromColumnName("API.id"),
                    createExpressionFromColumnName("API.name")),
                "EDS",
                List.of(
                    createExpressionFromColumnName("API.id"),
                    createExpressionFromColumnName("API.name")),
                "AS",
                List.of(createExpressionFromColumnName("API.status"))));
    Set<String> attributeSources =
        selectionNode.acceptVisitor(new AttributeSelectionOptimizingVisitor(executionContext));
    assertEquals(Set.of("AS", "QS", "EDS"), attributeSources);
    assertEquals(Set.of("AS"), selectionNode.getAttrSelectionSources());
  }

  @Test
  public void shouldRemove_attributeSelectionSourcesWithAndNode() {
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

    // API.id -> ["QS1", "EDS"]
    // API.name -> ["QS2", "EDS"]
    when(executionContext.getSourceToSelectionExpressionMap())
        .thenReturn(
            Map.of(
                "QS1",
                List.of(createExpressionFromColumnName("API.id")),
                "QS2",
                List.of(createExpressionFromColumnName("API.name")),
                "EDS",
                List.of(
                    createExpressionFromColumnName("API.id"),
                    createExpressionFromColumnName("API.name"))));
    Set<String> attributeSources =
        selectionNode.acceptVisitor(new AttributeSelectionOptimizingVisitor(executionContext));
    assertEquals(Set.of("QS1", "QS2", "EDS"), attributeSources);
    assertTrue(selectionNode.getAttrSelectionSources().isEmpty());
  }

  private Expression createExpressionFromColumnName(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }
}
