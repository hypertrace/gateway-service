package org.hypertrace.gateway.service.entity.query.visitor;

import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutionContextBuilderVisitorTest {
  private ExecutionContext executionContext;

  @BeforeEach
  public void setup() {
    this.executionContext = mock(ExecutionContext.class);
  }

  @Test
  public void shouldRemoveAllPendingSourcesForFetchedAttributes() {
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);
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

    when(executionContext.getSourceToSelectionAttributeMap())
        .thenReturn(
            Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id", "API.name")));

    when(executionContext.getPendingSelectionSources()).thenReturn(Set.of("EDS"));
    when(executionContext.getPendingSelectionSourcesForOrderBy())
        .thenReturn(Collections.emptySet());

    dataFetcherNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));
    verify(executionContext).removePendingSelectionSource("QS");
    verify(executionContext).removePendingSelectionSourceForOrderBy("QS");
    verify(executionContext).removePendingSelectionSource("EDS");
    verify(executionContext, never()).removePendingSelectionSourceForOrderBy("EDS");
  }

  @Test
  public void shouldKeepPendingSourcesForNonFetchedAttributes() {
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);

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

    when(executionContext.getSourceToSelectionAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.id"), "EDS", Set.of("API.id", "API.name")));

    when(executionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.id"), "EDS", Set.of("API.name")));

    when(executionContext.getPendingSelectionSources()).thenReturn(Set.of("EDS"));
    when(executionContext.getPendingSelectionSourcesForOrderBy()).thenReturn(Set.of("EDS"));

    dataFetcherNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));

    verify(executionContext).removePendingSelectionSource("QS");
    verify(executionContext).removePendingSelectionSourceForOrderBy("QS");
    verify(executionContext, never()).removePendingSelectionSource("EDS");
    verify(executionContext, never()).removePendingSelectionSourceForOrderBy("EDS");
  }

  @Test
  public void shouldKeepPendingSelectionSourcesForOrderBy() {
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);

    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS", "EDS"]
    when(executionContext.getSourceToSelectionAttributeMap())
        .thenReturn(
            Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id", "API.name")));

    when(executionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.id"), "EDS", Set.of("API.status")));

    when(executionContext.getPendingSelectionSources()).thenReturn(Set.of("EDS"));
    when(executionContext.getPendingSelectionSourcesForOrderBy()).thenReturn(Set.of("EDS"));

    dataFetcherNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));

    verify(executionContext).removePendingSelectionSource("QS");
    verify(executionContext).removePendingSelectionSourceForOrderBy("QS");
    verify(executionContext).removePendingSelectionSource("EDS");
    verify(executionContext, never()).removePendingSelectionSourceForOrderBy("EDS");
  }

  @Test
  public void shouldKeepPendingExtraSourcesForNonFetchedAttributes() {
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", null);

    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS", "EDS"]
    // API.status -> ["AS"]
    when(executionContext.getSourceToSelectionAttributeMap())
        .thenReturn(
            Map.of(
                "QS",
                Set.of("API.id", "API.name"),
                "EDS",
                Set.of("API.id", "API.name"),
                "AS",
                Set.of("API.status")));

    when(executionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Map.of("EDS", Set.of("API.name"), "AS", Set.of("API.status")));

    when(executionContext.getPendingSelectionSources()).thenReturn(Set.of("EDS", "AS"));
    when(executionContext.getPendingSelectionSourcesForOrderBy()).thenReturn(Set.of("EDS", "AS"));

    dataFetcherNode.acceptVisitor(new ExecutionContextBuilderVisitor(executionContext));

    verify(executionContext).removePendingSelectionSource("QS");
    verify(executionContext).removePendingSelectionSourceForOrderBy("QS");
    verify(executionContext).removePendingSelectionSource("EDS");
    verify(executionContext).removePendingSelectionSourceForOrderBy("EDS");
    verify(executionContext, never()).removePendingSelectionSourceForOrderBy("AS");
    verify(executionContext, never()).removePendingSelectionSourceForOrderBy("AS");
  }

  private Expression createExpressionFromColumnName(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }

  private OrderByExpression createOrderByExpressionFromColumnName(String columnName) {
    return OrderByExpression.newBuilder()
        .setExpression(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(columnName).build())
                .build())
        .build();
  }
}
