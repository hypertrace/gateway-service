package org.hypertrace.gateway.service.entity.query;

import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateAndOrNotFilter;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateEQFilter;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateFilter;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
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

  @Test
  public void testHasEntityIdEqualsFilterEmptyFilterOrEmptyEntityIdExpressions() {
    List<Expression> entityIdExpressions = List.of(buildExpression("SERVICE.id"));
    EntitiesRequest entitiesRequest = EntitiesRequest.newBuilder().build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest,false);

    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            Filter.newBuilder()
                .setOperator(Operator.AND)
        )
        .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest,false);

    // 1 level AND but empty entityIdExpressions
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter("SERVICE.id", "deliciouscookies")
            )
        )
        .build();

    executeHasEntityIdEqualsFilterTest(List.of(), entitiesRequest, false);
  }

  @Test
  public void testHasEntityIdEqualsFilterServiceId() {
    List<Expression> entityIdExpressions = List.of(buildExpression("SERVICE.id"));

    // 1 level AND
    EntitiesRequest entitiesRequest = EntitiesRequest.newBuilder()
            .setFilter(
                generateAndOrNotFilter(
                    Operator.AND,
                    generateEQFilter("SERVICE.id", "deliciouscookies")
                    )
            )
            .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, true);

    // 3 levels AND
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.AND,
                generateAndOrNotFilter(
                    Operator.AND,
                    generateAndOrNotFilter(
                        Operator.AND,
                        generateEQFilter("SERVICE.id", "deliciouscookies")
                    )
               )
            )
        )
        .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, true);

    // Simple EQ filter
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateEQFilter("SERVICE.id", "deliciouscookies")
        )
        .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, true);

    // Simple EQ filter not on Service Id
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateEQFilter("SERVICE.name", "deliciouscookies")
        )
        .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, false);

    // AND with a extra filter on the name. Should return true.
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(Operator.AND,
                generateEQFilter("SERVICE.name", "deliciouscookies"),
                generateAndOrNotFilter(
                    Operator.AND,
                    generateEQFilter("SERVICE.id", "deliciouscookies")
                )
            )
        )
        .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, true);

    // IN clause filter
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.AND,
                generateFilter(
                    Operator.IN,
                    "SERVICE.id",
                    Value.newBuilder()
                        .setValueType(ValueType.STRING_ARRAY)
                        .addAllStringArray(List.of("deliciouscookies", "tickets"))
                        .build()
                )
            )
        )
        .build();
    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, false);

    // Contains OR
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(Operator.OR,
                generateEQFilter("SERVICE.name", "deliciouscookies"),
                generateAndOrNotFilter(
                    Operator.AND,
                    generateEQFilter("SERVICE.id", "deliciouscookies")
                )
            )
        )
        .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, false);

    // Contains NOT
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(Operator.AND,
                generateEQFilter("SERVICE.name", "deliciouscookies"),
                generateAndOrNotFilter(
                    Operator.NOT,
                    generateEQFilter("SERVICE.id", "deliciouscookies")
                )
            )
        )
        .build();

    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, false);
  }

  @Test
  public void testHasEntityIdEqualsFilterMultipleEntityIdExpressions() {
    List<Expression> entityIdExpressions = List.of(
        buildExpression("SERVICE.idAttr1"),
        buildExpression("SERVICE.idAttr2")
    );

    // 1 level AND
    EntitiesRequest entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter("SERVICE.idAttr1", "deliciouscookies"),
                generateEQFilter("SERVICE.idAttr2", "tickets")
            )
        )
        .build();
    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, true);

    // Contains OR
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.OR,
                generateAndOrNotFilter(
                    Operator.AND,
                    generateEQFilter("SERVICE.idAttr1", "deliciouscookies"),
                    generateEQFilter("SERVICE.idAttr2", "tickets")
                )
            )
        )
        .build();
    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, false);

    // 2 level AND
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter("SERVICE.name", "deliciouscookies"),
                generateAndOrNotFilter(
                    Operator.AND,
                    generateEQFilter("SERVICE.idAttr1", "deliciouscookies"),
                    generateEQFilter("SERVICE.idAttr2", "tickets")
                )
            )
        )
        .build();
    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, true);

    // 3 level AND
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter("SERVICE.name", "deliciouscookies"),
                generateAndOrNotFilter(
                    Operator.AND,
                    generateAndOrNotFilter(
                        Operator.AND,
                        generateEQFilter("SERVICE.idAttr1", "deliciouscookies"),
                        generateEQFilter("SERVICE.idAttr2", "tickets")
                    )
                )
            )
        )
        .build();
    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, true);

    // 3 level AND missing EQ on one of the entityId expressions
    entitiesRequest = EntitiesRequest.newBuilder()
        .setFilter(
            generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter("SERVICE.name", "deliciouscookies"),
                generateAndOrNotFilter(
                    Operator.AND,
                    generateAndOrNotFilter(
                        Operator.AND,
                        generateEQFilter("SERVICE.idAttr1", "deliciouscookies"),
                        generateEQFilter("SERVICE.idAttr3", "tickets")
                    )
                )
            )
        )
        .build();
    executeHasEntityIdEqualsFilterTest(entityIdExpressions, entitiesRequest, false);
  }

  private void executeHasEntityIdEqualsFilterTest(List<Expression> entityIdExpressions,
                                                  EntitiesRequest entitiesRequest,
                                                  boolean expectedResult){
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getEntityIdExpressions()).thenReturn(entityIdExpressions);

    Assertions.assertEquals(
        expectedResult,
        ExecutionTreeUtils.hasEntityIdEqualsFilter(executionContext));
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
    return sourceKeys.stream().collect(Collectors.toUnmodifiableMap(s -> s, s -> List.of()));
  }
}
