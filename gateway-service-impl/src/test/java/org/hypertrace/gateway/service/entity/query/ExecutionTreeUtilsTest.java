package org.hypertrace.gateway.service.entity.query;

import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.junit.jupiter.api.Test;

class ExecutionTreeUtilsTest {

  @Test
  void removeDuplicateSelectionAttributes_invalidSource() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    when(expressionContext.getSourceToSelectionAttributeMap()).thenReturn(Collections.emptyMap());
    EntityExecutionContext executionContext = mock(EntityExecutionContext.class);
    when(executionContext.getExpressionContext()).thenReturn(expressionContext);

    ExecutionTreeUtils.removeDuplicateSelectionAttributes(executionContext, "INVALID");
    verify(executionContext, never()).removeSelectionAttributes(eq("INVALID"), anySet());
  }

  @Test
  void removeDuplicateSelectionAttributes() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // API.id -> ["QS"]
    // API.name -> ["QS", "EDS"]
    // API.status -> ["EDS", "AS"]
    // API.latency -> ["QS", "AS"]
    when(expressionContext.getSourceToSelectionAttributeMap())
        .thenReturn(
            Map.of(
                "QS",
                Set.of("API.id", "API.name", "API.latency"),
                "EDS",
                Set.of("API.name", "API.status"),
                "AS",
                Set.of("API.status", "API.latency")));
    EntityExecutionContext executionContext = mock(EntityExecutionContext.class);
    when(executionContext.getExpressionContext()).thenReturn(expressionContext);

    ExecutionTreeUtils.removeDuplicateSelectionAttributes(executionContext, "QS");
    verify(executionContext).removeSelectionAttributes("EDS", Set.of("API.name"));
    verify(executionContext).removeSelectionAttributes("AS", Set.of("API.latency"));
  }

  private ExpressionContext getMockExpressionContext(
      Map<String, List<Expression>> sourceToSelectionExpressionMap,
      Map<String, List<Expression>> sourceToMetricExpressionMap,
      Map<String, List<TimeAggregation>> sourceToTimeAggregationMap,
      Map<String, List<OrderByExpression>> sourceToSelectionOrderByExpressionMap,
      Map<String, List<OrderByExpression>> sourceToMetricOrderByExpressionMap,
      Map<String, List<Expression>> sourceToFilterExpressionMap,
      Map<String, Set<String>> attributeToSourcesMap) {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(sourceToSelectionExpressionMap);
    when(expressionContext.getSourceToMetricExpressionMap())
        .thenReturn(sourceToMetricExpressionMap);
    when(expressionContext.getSourceToTimeAggregationMap()).thenReturn(sourceToTimeAggregationMap);
    when(expressionContext.getSourceToSelectionOrderByExpressionMap())
        .thenReturn(sourceToSelectionOrderByExpressionMap);
    when(expressionContext.getSourceToMetricOrderByExpressionMap())
        .thenReturn(sourceToMetricOrderByExpressionMap);
    when(expressionContext.getSourceToFilterExpressionMap())
        .thenReturn(sourceToFilterExpressionMap);
    when(expressionContext.getAllAttributesToSourcesMap()).thenReturn(attributeToSourcesMap);

    return expressionContext;
  }

  // We don't care about the values. Just they keySets.
  private <T> Map<String, List<T>> createSourceToExpressionsMap(List<String> sourceKeys) {
    return sourceKeys.stream().collect(Collectors.toUnmodifiableMap(s -> s, s -> List.of()));
  }
}
