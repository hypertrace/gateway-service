package org.hypertrace.gateway.service.entity.query;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExecutionTreeUtilsTest {
  @Test
  public void testGetSingleSourceForAllAttributes_allSourceExpressionMapKeySetsHaveOneSource() {
    // All source expression map keysets have one source.
    ExecutionContext executionContext = getMockExecutionContext(
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        Map.of(
            "API.id", Set.of(AttributeSource.QS),
            "API.name", Set.of(AttributeSource.QS),
            "API.duration", Set.of(AttributeSource.QS),
            "API.startTime", Set.of(AttributeSource.QS)
            )
    );

    Assertions.assertEquals(Optional.of("QS"), ExecutionTreeUtils.getSingleSourceForAllAttributes(executionContext));
  }

  @Test
  public void testGetSingleSourceForAllAttributes_someSourceExpressionMapKeySetsHaveMultipleSources() {
    ExecutionContext executionContext = getMockExecutionContext(
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS", "EDS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        Map.of(
            "API.id", Set.of(AttributeSource.QS),
            "API.name", Set.of(AttributeSource.QS),
            "API.duration", Set.of(AttributeSource.QS, AttributeSource.EDS),
            "API.startTime", Set.of(AttributeSource.QS)
        )
    );

    Assertions.assertEquals(Optional.of("QS"), ExecutionTreeUtils.getSingleSourceForAllAttributes(executionContext));
  }

  @Test
  public void testGetSingleSourceForAllAttributes_someSourceExpressionMapKeySetsHaveDifferentSources() {
    ExecutionContext executionContext = getMockExecutionContext(
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("EDS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        Map.of(
            "API.id", Set.of(AttributeSource.QS),
            "API.name", Set.of(AttributeSource.QS),
            "API.duration", Set.of(AttributeSource.EDS),
            "API.startTime", Set.of(AttributeSource.QS)
        )
    );

    Assertions.assertEquals(Optional.empty(), ExecutionTreeUtils.getSingleSourceForAllAttributes(executionContext));
  }

  @Test
  public void testGetSingleSourceForAllAttributes_someSourceExpressionMapKeySetsAreEmpty() {
    // All source expression map keysets have one source but dont have sources defined.
    ExecutionContext executionContext = getMockExecutionContext(
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of()),
        createSourceToExpressionsMap(List.of("QS")),
        createSourceToExpressionsMap(List.of()),
        Map.of(
            "API.id", Set.of(AttributeSource.QS),
            "API.name", Set.of(AttributeSource.QS),
            "API.duration", Set.of(),
            "API.startTime", Set.of(AttributeSource.QS)
        )
    );

    Assertions.assertEquals(Optional.of("QS"), ExecutionTreeUtils.getSingleSourceForAllAttributes(executionContext));
  }

  // This scenario is not possible but for academic purposes let's test it.
  @Test
  public void testGetSingleSourceForAllAttributes_allEmptySourceExpressionMapKeySetsAndAttributes() {
    ExecutionContext executionContext = getMockExecutionContext(
        createSourceToExpressionsMap(List.of()),
        createSourceToExpressionsMap(List.of()),
        createSourceToExpressionsMap(List.of()),
        createSourceToExpressionsMap(List.of()),
        createSourceToExpressionsMap(List.of()),
        Map.of(
            "API.id", Set.of(),
            "API.name", Set.of(),
            "API.duration", Set.of(),
            "API.startTime", Set.of()
        )
    );

    Assertions.assertEquals(Optional.empty(), ExecutionTreeUtils.getSingleSourceForAllAttributes(executionContext));
  }

  private ExecutionContext getMockExecutionContext(Map<String, List<Expression>> sourceToSelectionExpressionMap,
                                                   Map<String, List<Expression>> sourceToMetricExpressionMap,
                                                   Map<String, List<TimeAggregation>> sourceToTimeAggregationMap,
                                                   Map<String, List<OrderByExpression>> sourceToOrderByExpressionMap,
                                                   Map<String, List<Expression>> sourceToFilterExpressionMap,
                                                   Map<String, Set<AttributeSource>> attributeToSourcesMap
                                                   ) {
    ExecutionContext executionContext = mock(ExecutionContext.class);

    when(executionContext.getSourceToSelectionExpressionMap()).thenReturn(sourceToSelectionExpressionMap);
    when(executionContext.getSourceToMetricExpressionMap()).thenReturn(sourceToMetricExpressionMap);
    when(executionContext.getSourceToTimeAggregationMap()).thenReturn(sourceToTimeAggregationMap);
    when(executionContext.getSourceToOrderByExpressionMap()).thenReturn(sourceToOrderByExpressionMap);
    when(executionContext.getSourceToFilterExpressionMap()).thenReturn(sourceToFilterExpressionMap);

    when(executionContext.getAttributeToSourcesMap()).thenReturn(attributeToSourcesMap);

    return executionContext;
  }

  // We don't care about the values. Just they keySets.
  private <T> Map<String, List<T>> createSourceToExpressionsMap(List<String> sourceKeys) {
    return sourceKeys.stream().collect(Collectors.toUnmodifiableMap(s -> s, s ->List.of()));
  }
}
