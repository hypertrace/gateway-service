package org.hypertrace.gateway.service.common;

import static org.hypertrace.core.attribute.service.v1.AttributeSource.EDS;
import static org.hypertrace.core.attribute.service.v1.AttributeSource.QS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ExpressionContextTest {
  @Test
  void testGetSingleSourceForAllAttributes_allSourceExpressionMapKeySetsHaveOneSource() {
    // All source expression map keysets have one source.
    ExpressionContext expressionContext =
        getMockExpressionContext(
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")));

    assertEquals(
        Optional.of("QS"), ExpressionContext.getSingleSourceForAllAttributes(expressionContext));
  }

  @Test
  void testGetSingleSourceForAllAttributes_someSourceExpressionMapKeySetsHaveMultipleSources() {
    ExpressionContext expressionContext =
        getMockExpressionContext(
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS", "EDS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")));

    assertEquals(
        Optional.of("QS"), ExpressionContext.getSingleSourceForAllAttributes(expressionContext));
  }

  @Test
  void testGetSingleSourceForAllAttributes_someSourceExpressionMapKeySetsHaveDifferentSources() {
    ExpressionContext expressionContext =
        getMockExpressionContext(
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("EDS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")));

    assertEquals(
        Optional.empty(), ExpressionContext.getSingleSourceForAllAttributes(expressionContext));
  }

  @Test
  void testGetSingleSourceForAllAttributes_someSourceExpressionMapKeySetsAreEmpty() {
    // All source expression map keysets have one source but dont have sources defined.
    ExpressionContext expressionContext =
        getMockExpressionContext(
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of()),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of("QS")),
            createSourceToExpressionsMap(List.of()));

    assertEquals(
        Optional.of("QS"), ExpressionContext.getSingleSourceForAllAttributes(expressionContext));
  }

  // This scenario is not possible but for academic purposes let's test it.
  @Test
  void testGetSingleSourceForAllAttributes_allEmptySourceExpressionMapKeySetsAndAttributes() {
    ExpressionContext expressionContext =
        getMockExpressionContext(
            createSourceToExpressionsMap(List.of()),
            createSourceToExpressionsMap(List.of()),
            createSourceToExpressionsMap(List.of()),
            createSourceToExpressionsMap(List.of()),
            createSourceToExpressionsMap(List.of()),
            createSourceToExpressionsMap(List.of()));

    assertEquals(
        Optional.empty(), ExpressionContext.getSingleSourceForAllAttributes(expressionContext));
  }

  @Test
  void test_filtersAndOrderByFromSameSingleSourceSet() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS"]
    when(expressionContext.getSourceToFilterAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id")));

    // order bys
    // API.status -> ["QS"]
    when(expressionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.status")));
    when(expressionContext.getSourceToMetricOrderByAttributeMap())
        .thenReturn(Collections.emptyMap());

    Set<String> sourceSets =
        ExpressionContext.getSourceSetsIfFilterAndOrderByAreFromSameSourceSets(expressionContext);
    assertEquals(1, sourceSets.size());
    assertTrue(sourceSets.contains("QS"));
  }

  @Test
  void test_filtersAndOrderByFromSameMultipleSourceSets() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS", "EDS"]
    when(expressionContext.getSourceToFilterAttributeMap())
        .thenReturn(
            Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id", "API.name")));

    // order bys
    // API.status -> ["QS", "EDS"]
    // API.latency -> ["QS", "EDS"]
    when(expressionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.status", "API.latency")));
    when(expressionContext.getSourceToMetricOrderByAttributeMap())
        .thenReturn(Map.of("EDS", Set.of("API.status", "API.latency")));

    Set<String> sourceSets =
        ExpressionContext.getSourceSetsIfFilterAndOrderByAreFromSameSourceSets(expressionContext);
    assertEquals(2, sourceSets.size());
    assertTrue(sourceSets.contains("QS"));
    assertTrue(sourceSets.contains("EDS"));
  }

  @Test
  void test_filtersAndOrderByFromEmptyOrderBy() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS", "EDS"]
    when(expressionContext.getSourceToFilterAttributeMap())
        .thenReturn(
            Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id", "API.name")));
    when(expressionContext.getFilterAttributeToSourceMap())
        .thenReturn(Map.of("API.id", Set.of("QS", "EDS"), "API.name", Set.of("QS", "EDS")));

    // order bys
    when(expressionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Collections.emptyMap());
    when(expressionContext.getSourceToMetricOrderByAttributeMap())
        .thenReturn(Collections.emptyMap());

    Set<String> sourceSets =
        ExpressionContext.getSourceSetsIfFilterAndOrderByAreFromSameSourceSets(expressionContext);
    assertEquals(2, sourceSets.size());
    assertTrue(sourceSets.contains("QS"));
    assertTrue(sourceSets.contains("EDS"));
  }

  @Test
  void test_filtersAndOrderByFromEmptyFilters() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    when(expressionContext.getSourceToFilterAttributeMap()).thenReturn(Collections.emptyMap());

    // order bys
    // API.status -> ["QS", "EDS"]
    // API.latency -> ["QS"]
    when(expressionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.status", "API.latency")));
    when(expressionContext.getSourceToMetricOrderByAttributeMap())
        .thenReturn(Map.of("EDS", Set.of("API.status")));

    Set<String> sourceSets =
        ExpressionContext.getSourceSetsIfFilterAndOrderByAreFromSameSourceSets(expressionContext);
    assertEquals(1, sourceSets.size());
    assertTrue(sourceSets.contains("QS"));
  }

  @Test
  void test_filtersAndOrderByFromDifferentSourceSets() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS"]
    when(expressionContext.getSourceToFilterAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id")));
    when(expressionContext.getFilterAttributeToSourceMap())
        .thenReturn(Map.of("API.id", Set.of("QS", "EDS"), "API.name", Set.of("QS")));

    // order bys
    // API.status -> ["QS", "EDS"]
    // API.latency -> ["EDS"]
    when(expressionContext.getSourceToSelectionOrderByAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.status")));
    when(expressionContext.getSourceToMetricOrderByAttributeMap())
        .thenReturn(Map.of("EDS", Set.of("API.status", "API.latency")));

    Set<String> sourceSets =
        ExpressionContext.getSourceSetsIfFilterAndOrderByAreFromSameSourceSets(expressionContext);
    assertTrue(sourceSets.isEmpty());
  }

  @Test
  @DisplayName(
      "filters are applied on a single data source, for filters check on other data source")
  void test_areFiltersOnCurrentDataSource_onlyPresentOnTheCurrentDataSource() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS"]
    when(expressionContext.getSourceToFilterAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id")));
    when(expressionContext.getFilterAttributeToSourceMap())
        .thenReturn(Map.of("API.id", Set.of("QS", "EDS"), "API.name", Set.of("QS")));

    assertTrue(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "QS"));
    assertFalse(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "EDS"));
  }

  @Test
  @DisplayName(
      "are filters applied on a single data source, if attributes are on multiple data sources")
  void test_areFiltersOnCurrentDataSource_multipleSourcesForAttributes() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    // API.id -> ["QS", "EDS"]
    // API.name -> ["QS", "EDS"]
    when(expressionContext.getSourceToFilterAttributeMap())
        .thenReturn(
            Map.of("QS", Set.of("API.id", "API.name"), "EDS", Set.of("API.id", "API.name")));
    when(expressionContext.getFilterAttributeToSourceMap())
        .thenReturn(Map.of("API.id", Set.of("QS", "EDS"), "API.name", Set.of("QS", "EDS")));

    assertTrue(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "QS"));
    assertTrue(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "EDS"));
  }

  @Test
  @DisplayName(
      "are filters applied on a single data source, if attributes are on different data sources")
  void test_areFiltersOnCurrentDataSource_attributesOnDifferentSources() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    // API.id -> ["QS"]
    // API.name -> ["EDS"]
    when(expressionContext.getSourceToFilterAttributeMap())
        .thenReturn(Map.of("QS", Set.of("API.id"), "EDS", Set.of("API.name")));
    when(expressionContext.getFilterAttributeToSourceMap())
        .thenReturn(Map.of("API.id", Set.of("QS"), "API.name", Set.of("EDS")));

    assertFalse(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "QS"));
    assertFalse(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "EDS"));
  }

  @Test
  @DisplayName("are filters applied on a single data source, if no filters")
  void test_areFiltersOnCurrentDataSource_noFilters() {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    // filters
    when(expressionContext.getSourceToFilterAttributeMap()).thenReturn(Collections.emptyMap());

    assertTrue(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "QS"));
    assertTrue(ExpressionContext.areFiltersOnlyOnCurrentDataSource(expressionContext, "EDS"));
  }

  private ExpressionContext getMockExpressionContext(
      Map<String, List<Expression>> sourceToSelectionExpressionMap,
      Map<String, List<Expression>> sourceToMetricExpressionMap,
      Map<String, List<TimeAggregation>> sourceToTimeAggregationMap,
      Map<String, List<OrderByExpression>> sourceToSelectionOrderByExpressionMap,
      Map<String, List<OrderByExpression>> sourceToMetricOrderByExpressionMap,
      Map<String, List<Expression>> sourceToFilterExpressionMap) {
    ExpressionContext expressionContext = mock(ExpressionContext.class);
    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(sourceToSelectionExpressionMap);
    when(expressionContext.getSourceToMetricAggregationExpressionMap())
        .thenReturn(sourceToMetricExpressionMap);
    when(expressionContext.getSourceToTimeAggregationMap()).thenReturn(sourceToTimeAggregationMap);
    when(expressionContext.getSourceToSelectionOrderByExpressionMap())
        .thenReturn(sourceToSelectionOrderByExpressionMap);
    when(expressionContext.getSourceToMetricOrderByExpressionMap())
        .thenReturn(sourceToMetricOrderByExpressionMap);
    when(expressionContext.getSourceToFilterExpressionMap())
        .thenReturn(sourceToFilterExpressionMap);

    return expressionContext;
  }

  // We don't care about the values. Just they keySets.
  private <T> Map<String, List<T>> createSourceToExpressionsMap(List<String> sourceKeys) {
    return sourceKeys.stream().collect(Collectors.toUnmodifiableMap(s -> s, s -> List.of()));
  }
}
