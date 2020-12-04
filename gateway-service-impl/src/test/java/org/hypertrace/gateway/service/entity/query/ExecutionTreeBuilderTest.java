package org.hypertrace.gateway.service.entity.query;

import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildAggregateExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildOrderByExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildTimeAggregation;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateAndOrNotFilter;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateEQFilter;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateFilter;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.getTimeRangeFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.entity.query.visitor.OptimizingVisitor;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class ExecutionTreeBuilderTest {

  private static final String TENANT_ID = "tenant1";

  private static final String API_API_ID_ATTR = "API.apiId";
  private static final String API_NAME_ATTR = "API.name";
  private static final String API_TYPE_ATTR = "API.apiType";
  private static final String API_PATTERN_ATTR = "API.urlPattern";
  private static final String API_START_TIME_ATTR = "API.startTime";
  private static final String API_END_TIME_ATTR = "API.endTime";
  private static final String API_NUM_CALLS_ATTR = "API.numCalls";
  private static final String API_STATE_ATTR = "API.state";
  private static final String API_DISCOVERY_STATE = "API.apiDiscoveryState";
  private static final String API_ID_ATTR = "API.id";

  private static final Map<String, AttributeMetadata> attributeSources =
      new HashMap<>() {
        {
          put(
              API_API_ID_ATTR,
              buildAttributeMetadataForSources(API_API_ID_ATTR, AttributeScope.API.name(), "apiId", List.of(AttributeSource.EDS)));
          put(
              API_PATTERN_ATTR,
              buildAttributeMetadataForSources(API_PATTERN_ATTR, AttributeScope.API.name(), "urlPattern", List.of(AttributeSource.EDS)));
          put(
              API_NAME_ATTR,
              buildAttributeMetadataForSources(API_NAME_ATTR, AttributeScope.API.name(), "name", List.of(AttributeSource.EDS)));
          put(
              API_TYPE_ATTR,
              buildAttributeMetadataForSources(API_TYPE_ATTR, AttributeScope.API.name(), "apiType", List.of(AttributeSource.EDS)));
          put(
              API_START_TIME_ATTR,
              buildAttributeMetadataForSources(API_START_TIME_ATTR, AttributeScope.API.name(), "startTime", List.of(AttributeSource.QS)));
          put(
              API_END_TIME_ATTR,
              buildAttributeMetadataForSources(API_END_TIME_ATTR, AttributeScope.API.name(), "endTime", List.of(AttributeSource.QS)));
          put(
              API_NUM_CALLS_ATTR,
              buildAttributeMetadataForSources(API_NUM_CALLS_ATTR, AttributeScope.API.name(), "numCalls", List.of(AttributeSource.QS)));
          put(
              API_STATE_ATTR,
              buildAttributeMetadataForSources(API_STATE_ATTR, AttributeScope.API.name(), "state", List.of(AttributeSource.QS)));
          put(
              API_DISCOVERY_STATE,
              buildAttributeMetadataForSources(API_DISCOVERY_STATE, AttributeScope.API.name(), "apiDiscoveryState", List.of(AttributeSource.EDS, AttributeSource.QS)));
          put(
              API_ID_ATTR,
              buildAttributeMetadataForSources(API_ID_ATTR, AttributeScope.API.name(), "id", List.of(AttributeSource.EDS, AttributeSource.QS)));
        }
      };

  @Mock private AttributeMetadataProvider attributeMetadataProvider;
  @Mock private EntityIdColumnsConfigs entityIdColumnsConfigs;

  private static AttributeMetadata buildAttributeMetadataForSources(String attributeId,
                                                                    String scope,
                                                                    String key,
                                                                    List<AttributeSource> sources) {
    return AttributeMetadata.newBuilder()
        .setId(attributeId)
        .setScopeString(scope)
        .setKey(key)
        .addAllSources(sources)
        .build();
  }

  @BeforeEach
  public void setup() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);
    when(attributeMetadataProvider.getAttributesMetadata(
        any(RequestContext.class),
        eq(AttributeScope.API.name()))
    ).thenReturn(attributeSources);

    attributeSources.forEach((attributeId, attribute) ->
        when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class),
            eq(attribute.getScopeString()),
            eq(attribute.getKey()))
        ).thenReturn(Optional.of(attribute)));
  }

  private ExecutionTreeBuilder getExecutionTreeBuilderForOptimizedFilterTests() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000;
    EntitiesRequest entitiesRequest = EntitiesRequest.newBuilder()
        .setStartTimeMillis(startTime).setEndTimeMillis(endTime)
        .setEntityType(AttributeScope.API.name())
        .setFilter(getTimeRangeFilter("API.startTime", startTime, endTime))
        .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, startTime, endTime, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    return new ExecutionTreeBuilder(executionContext);
  }

  @Test
  public void testOptimizedFilterTreeBuilderSimpleFilter() {
    ExecutionTreeBuilder executionTreeBuilder = getExecutionTreeBuilderForOptimizedFilterTests();
    Filter filter = generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString());
    QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
    QueryNode optimizedQueryNode = queryNode.acceptVisitor(new OptimizingVisitor());
    assertNotNull(optimizedQueryNode);
    assertTrue(optimizedQueryNode instanceof DataFetcherNode);
    assertEquals(((DataFetcherNode) optimizedQueryNode).getFilter(), filter);
  }

  @Test
  public void testOptimizedFilterTreeBuilderAndOrFilterSingleDataSource() {
    ExecutionTreeBuilder executionTreeBuilder = getExecutionTreeBuilderForOptimizedFilterTests();
    {
      Filter filter =
          generateAndOrNotFilter(
              Operator.AND,
              generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString()),
              generateEQFilter(API_NAME_ATTR, "/login"),
              generateEQFilter(API_TYPE_ATTR, "http"));
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof AndNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof DataFetcherNode);
      assertEquals(filter, ((DataFetcherNode) optimizedNode).getFilter());
    }

    {
      Filter filter =
          generateAndOrNotFilter(
              Operator.OR,
              generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString()),
              generateEQFilter(API_NAME_ATTR, "/login"),
              generateEQFilter(API_TYPE_ATTR, "http"));
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof OrNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof DataFetcherNode);
      assertEquals(filter, ((DataFetcherNode) optimizedNode).getFilter());
    }
  }

  @Test
  public void testOptimizedFilterTreeBuilderAndOrFilterMultiDataSource() {
    ExecutionTreeBuilder executionTreeBuilder = getExecutionTreeBuilderForOptimizedFilterTests();
    Filter apiIdFilter = generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString());
    Filter apiNameFilter = generateEQFilter(API_NAME_ATTR, "/login");
    Filter startTimeFilter =
        generateFilter(
            Operator.GE,
            API_START_TIME_ATTR,
            Value.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setValueType(ValueType.TIMESTAMP)
                .build());
    {
      Filter filter =
          generateAndOrNotFilter(Operator.AND, apiIdFilter, apiNameFilter, startTimeFilter);
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof AndNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof AndNode);
      List<QueryNode> queryNodeList = ((AndNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      queryNodeList.forEach(tn -> assertTrue(tn instanceof DataFetcherNode));
      List<Filter> filterList =
          queryNodeList.stream()
              .map(tn -> ((DataFetcherNode) tn).getFilter())
              .collect(Collectors.toList());
      assertTrue(
          filterList.containsAll(
              Arrays.asList(
                  generateAndOrNotFilter(Operator.AND, apiIdFilter, apiNameFilter), startTimeFilter)));
    }

    {
      Filter filter = generateAndOrNotFilter(Operator.OR, apiIdFilter, apiNameFilter, startTimeFilter);
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof OrNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof OrNode);
      List<QueryNode> queryNodeList = ((OrNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      queryNodeList.forEach(tn -> assertTrue(tn instanceof DataFetcherNode));
      List<Filter> filterList =
          queryNodeList.stream()
              .map(tn -> ((DataFetcherNode) tn).getFilter())
              .collect(Collectors.toList());
      assertTrue(
          filterList.containsAll(
              Arrays.asList(
                  generateAndOrNotFilter(Operator.OR, apiIdFilter, apiNameFilter), startTimeFilter)));
    }
  }

  @Test
  public void testOptimizedFilterTreeBuilderNestedAndFilter() {
    ExecutionTreeBuilder executionTreeBuilder = getExecutionTreeBuilderForOptimizedFilterTests();
    Filter apiIdFilter = generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString());
    Filter apiNameFilter = generateEQFilter(API_NAME_ATTR, "/login");
    Filter apiPatternFilter = generateEQFilter(API_PATTERN_ATTR, "/login");
    Filter startTimeFilter =
        generateFilter(
            Operator.GE,
            API_START_TIME_ATTR,
            Value.newBuilder()
                .setTimestamp(System.currentTimeMillis() - 5 * 60 * 1000)
                .setValueType(ValueType.TIMESTAMP)
                .build());
    Filter endTimeFilter =
        generateFilter(
            Operator.LE,
            API_END_TIME_ATTR,
            Value.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setValueType(ValueType.TIMESTAMP)
                .build());

    {
      Filter level2Filter = generateAndOrNotFilter(Operator.AND, apiIdFilter, startTimeFilter);
      Filter filter = generateAndOrNotFilter(Operator.AND, level2Filter, apiNameFilter);
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof AndNode);
      List<QueryNode> queryNodeList = ((AndNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      queryNodeList.forEach(tn -> assertTrue(tn instanceof DataFetcherNode));
      List<Filter> filterList =
          queryNodeList.stream()
              .map(tn -> ((DataFetcherNode) tn).getFilter())
              .collect(Collectors.toList());
      assertTrue(
          filterList.containsAll(
              Arrays.asList(
                  generateAndOrNotFilter(Operator.AND, apiNameFilter, apiIdFilter), startTimeFilter)));
    }

    {
      Filter level2Filter = generateAndOrNotFilter(Operator.AND, apiIdFilter, apiNameFilter);
      Filter filter = generateAndOrNotFilter(Operator.AND, level2Filter, startTimeFilter);
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof AndNode);
      List<QueryNode> queryNodeList = ((AndNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      queryNodeList.forEach(tn -> assertTrue(tn instanceof DataFetcherNode));
      List<Filter> filterList =
          queryNodeList.stream()
              .map(tn -> ((DataFetcherNode) tn).getFilter())
              .collect(Collectors.toList());
      assertTrue(
          filterList.containsAll(
              Arrays.asList(
                  generateAndOrNotFilter(Operator.AND, apiIdFilter, apiNameFilter), startTimeFilter)));
    }

    {
      Filter level3Filter = generateAndOrNotFilter(Operator.AND, endTimeFilter, apiPatternFilter);
      Filter level2Filter =
          generateAndOrNotFilter(Operator.AND, apiIdFilter, startTimeFilter, level3Filter);
      Filter filter = generateAndOrNotFilter(Operator.AND, level2Filter, apiNameFilter);
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof AndNode);
      List<QueryNode> queryNodeList = ((AndNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      queryNodeList.forEach(tn -> assertTrue(tn instanceof DataFetcherNode));
    }
  }

  @Test
  public void testOptimizedFilterTreeBuilderNestedAndOrFilter() {
    ExecutionTreeBuilder executionTreeBuilder = getExecutionTreeBuilderForOptimizedFilterTests();
    Filter apiIdFilter = generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString());
    Filter apiNameFilter = generateEQFilter(API_NAME_ATTR, "/login");
    Filter startTimeFilter =
        generateFilter(
            Operator.GE,
            API_START_TIME_ATTR,
            Value.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setValueType(ValueType.TIMESTAMP)
                .build());

    {
      Filter level2Filter = generateAndOrNotFilter(Operator.AND, apiIdFilter, apiNameFilter);
      Filter filter = generateAndOrNotFilter(Operator.OR, level2Filter, startTimeFilter);
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof OrNode);
      List<QueryNode> queryNodeList = ((OrNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      queryNodeList.forEach(tn -> assertTrue(tn instanceof DataFetcherNode));
      List<Filter> filterList =
          queryNodeList.stream()
              .map(tn -> ((DataFetcherNode) tn).getFilter())
              .collect(Collectors.toList());
      assertTrue(
          filterList.containsAll(
              Arrays.asList(
                  generateAndOrNotFilter(Operator.AND, apiIdFilter, apiNameFilter), startTimeFilter)));
    }

    {
      Filter level2Filter = generateAndOrNotFilter(Operator.AND, apiIdFilter, startTimeFilter);
      Filter filter = generateAndOrNotFilter(Operator.OR, level2Filter, apiNameFilter);
      QueryNode queryNode = executionTreeBuilder.buildFilterTree(filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new OptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof OrNode);
      List<QueryNode> queryNodeList = ((OrNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      QueryNode firstNode = queryNodeList.get(0);
      assertTrue(firstNode instanceof DataFetcherNode);
      assertEquals(apiNameFilter, ((DataFetcherNode) firstNode).getFilter());
      QueryNode secondNode = queryNodeList.get(1);
      assertTrue(secondNode instanceof AndNode);
      List<QueryNode> childNodes = ((AndNode) secondNode).getChildNodes();
      assertEquals(2, childNodes.size());
      assertTrue(
          childNodes.stream()
              .map(node -> ((DataFetcherNode) node).getFilter())
              .collect(Collectors.toList())
              .containsAll(Arrays.asList(apiIdFilter, startTimeFilter)));
    }
  }

  @Test
  public void testExecutionTreeBuilderWithSelectFilterOrderPagination() {
    OrderByExpression orderByExpression = buildOrderByExpression(API_API_ID_ATTR);
    {
      EntitiesRequest entitiesRequest =
          EntitiesRequest.newBuilder()
              .setEntityType(AttributeScope.API.name())
              .addSelection(buildExpression(API_NAME_ATTR))
              .setFilter(generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString()))
              .addOrderBy(orderByExpression)
              .setLimit(10)
              .setOffset(20)
              .build();
      EntitiesRequestContext entitiesRequestContext =
          new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
      ExecutionContext executionContext =
          ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode executionTree = executionTreeBuilder.build();
      assertNotNull(executionTree);
      assertTrue(executionTree instanceof SortAndPaginateNode);
      assertEquals(10, ((SortAndPaginateNode)executionTree).getLimit());
      assertEquals(20, ((SortAndPaginateNode)executionTree).getOffset());
      assertEquals(List.of(orderByExpression), ((SortAndPaginateNode)executionTree).getOrderByExpressionList());

      QueryNode selectionAndFilterNode = ((SortAndPaginateNode)executionTree).getChildNode();
      assertTrue(selectionAndFilterNode instanceof SelectionAndFilterNode);
      assertEquals(20, ((SelectionAndFilterNode)selectionAndFilterNode).getOffset());
      assertEquals(10, ((SelectionAndFilterNode)selectionAndFilterNode).getLimit());
    }

    {
      long endTime = System.currentTimeMillis();
      long startTime = endTime - 1000;
      Filter apiIdFilter = generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString());
      Filter trFilter = getTimeRangeFilter(API_START_TIME_ATTR, startTime, endTime);
      EntitiesRequest entitiesRequest =
          EntitiesRequest.newBuilder()
              .setEntityType(AttributeScope.API.name())
              .addSelection(buildExpression(API_START_TIME_ATTR))
              .setFilter(Filter.newBuilder().setOperator(Operator.AND).addChildFilter(trFilter).addChildFilter(apiIdFilter))
              .addOrderBy(orderByExpression)
              .setLimit(10)
              .setOffset(0)
              .build();
      EntitiesRequestContext entitiesRequestContext =
          new EntitiesRequestContext(TENANT_ID, startTime, endTime, "API", "API.startTime", new HashMap<>());
      ExecutionContext executionContext =
          ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode executionTree = executionTreeBuilder.build();
      assertNotNull(executionTree);
      assertTrue(executionTree instanceof SortAndPaginateNode);
      assertEquals(10, ((SortAndPaginateNode) executionTree).getLimit());
      QueryNode firstChild = ((SortAndPaginateNode) executionTree).getChildNode();
      assertTrue(firstChild instanceof AndNode);
      List<QueryNode> grandchildren = ((AndNode) firstChild).getChildNodes();
      assertEquals(2, grandchildren.size());
      Set<String> sources = new HashSet<>();
      grandchildren.forEach(c -> {
        assertTrue(c instanceof DataFetcherNode);
        sources.add(((DataFetcherNode) c).getSource());
      });
      assertTrue(sources.contains(AttributeSource.EDS.name()));
      assertTrue(sources.contains(AttributeSource.QS.name()));
    }
  }

  @Test
  public void testExecutionTreeBuilderWithSelectPagination() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_NAME_ATTR))
            .setLimit(10)
            .setOffset(20)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof SortAndPaginateNode);
    assertEquals(10, ((SortAndPaginateNode)executionTree).getLimit());
    assertEquals(20, ((SortAndPaginateNode)executionTree).getOffset());
    assertEquals(List.of(), ((SortAndPaginateNode)executionTree).getOrderByExpressionList());

    QueryNode selectionAndFilterNode = ((SortAndPaginateNode)executionTree).getChildNode();
    assertTrue(selectionAndFilterNode instanceof SelectionAndFilterNode);
    assertEquals(10, ((SelectionAndFilterNode)selectionAndFilterNode).getLimit());
    assertEquals(20, ((SelectionAndFilterNode)selectionAndFilterNode).getOffset());
  }

  @Test
  public void test_build_selectAttributeAndAggregateMetricWithSameSource_shouldCreateSelectionAndFilterNode() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_STATE_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .setLimit(10)
            .setOffset(0)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof TotalFetcherNode);
    assertEquals("QS", ((TotalFetcherNode)executionTree).getSource());

    QueryNode selectionAndFilterNode = ((TotalFetcherNode)executionTree).getChildNode();
    assertTrue(selectionAndFilterNode instanceof SelectionAndFilterNode);
    assertEquals("QS", ((SelectionAndFilterNode)selectionAndFilterNode).getSource());
    assertEquals(0, ((SelectionAndFilterNode)selectionAndFilterNode).getOffset());
    assertEquals(10, ((SelectionAndFilterNode)selectionAndFilterNode).getLimit());
  }

  @Test
  public void test_build_selectAttributesTimeAggregationAndFilterWithSameSource_shouldCreateSelectionAndFilterNode() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_STATE_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .addTimeAggregation(
                buildTimeAggregation(
                    30,
                    API_NUM_CALLS_ATTR,
                    FunctionType.AVG,
                    "AVG_numCalls",
                    List.of()
                )
            )
            .setFilter(generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter(API_STATE_ATTR, "state1"),
                generateFilter(Operator.GE, API_NUM_CALLS_ATTR,
                    Value.newBuilder().
                        setDouble(60)
                        .setValueType(ValueType.DOUBLE)
                        .build()
                )
            ))
            .setLimit(10)
            .setOffset(0)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof TotalFetcherNode);
    assertEquals("QS", ((TotalFetcherNode)executionTree).getSource());

    QueryNode selectionAndFilterNode = ((TotalFetcherNode)executionTree).getChildNode();
    assertTrue(selectionAndFilterNode instanceof SelectionAndFilterNode);
    assertEquals("QS", ((SelectionAndFilterNode)selectionAndFilterNode).getSource());
    assertEquals(0, ((SelectionAndFilterNode)selectionAndFilterNode).getOffset());
    assertEquals(10, ((SelectionAndFilterNode)selectionAndFilterNode).getLimit());
  }

  @Test
  public void test_build_selectAttributesWithEntityIdEqFilter_shouldNotCreateTotalNode() {
    when(entityIdColumnsConfigs.getIdKey("API")).thenReturn(Optional.of("id"));
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_STATE_ATTR))
            .addSelection(buildExpression(API_ID_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .addTimeAggregation(
                buildTimeAggregation(
                    30,
                    API_NUM_CALLS_ATTR,
                    FunctionType.AVG,
                    "AVG_numCalls",
                    List.of()
                )
            )
            .setFilter(generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter(API_ID_ATTR, "apiId1"),
                generateEQFilter(API_STATE_ATTR, "state1"),
                generateFilter(Operator.GE, API_NUM_CALLS_ATTR,
                    Value.newBuilder().
                        setDouble(60)
                        .setValueType(ValueType.DOUBLE)
                        .build()
                )
            ))
            .setLimit(10)
            .setOffset(0)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);

    assertTrue(executionTree instanceof SelectionAndFilterNode);
    assertEquals("QS", ((SelectionAndFilterNode)executionTree).getSource());
    assertEquals(0, ((SelectionAndFilterNode)executionTree).getOffset());
    assertEquals(10, ((SelectionAndFilterNode)executionTree).getLimit());

    // Assert that total is set to 1
    assertEquals(1, executionContext.getTotal());
  }

  @Test
  public void test_build_selectAttributesTimeAggregationFilterAndOrderByWithSameSource_shouldCreateSelectionAndFilterNode() {
    OrderByExpression orderByExpression = buildOrderByExpression(API_STATE_ATTR);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_STATE_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .addTimeAggregation(
                buildTimeAggregation(
                    30,
                    API_NUM_CALLS_ATTR,
                    FunctionType.AVG,
                    "AVG_numCalls",
                    List.of()
                )
            )
            .setFilter(generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter(API_STATE_ATTR, "state1"),
                generateFilter(Operator.GE, API_NUM_CALLS_ATTR,
                    Value.newBuilder().
                        setDouble(60)
                        .setValueType(ValueType.DOUBLE)
                        .build()
                )
            ))
            .addOrderBy(orderByExpression)
            .setLimit(10)
            .setOffset(0)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof TotalFetcherNode);
    assertEquals("QS", ((TotalFetcherNode)executionTree).getSource());

    QueryNode selectionAndFilterNode = ((TotalFetcherNode)executionTree).getChildNode();
    assertTrue(selectionAndFilterNode instanceof SelectionAndFilterNode);
    assertEquals("QS", ((SelectionAndFilterNode)selectionAndFilterNode).getSource());
    assertEquals(0, ((SelectionAndFilterNode)selectionAndFilterNode).getOffset());
    assertEquals(10, ((SelectionAndFilterNode)selectionAndFilterNode).getLimit());
  }

  @Test
  public void test_build_selectAttributesAndFilterWithSameSourceNonZeroOffset_shouldCreateSelectionAndFilterNodeAndPaginateOnlyNode() {
    OrderByExpression orderByExpression = buildOrderByExpression(API_STATE_ATTR);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_STATE_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .setFilter(generateAndOrNotFilter(
                Operator.AND,
                generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"),
                generateFilter(Operator.GE, API_NUM_CALLS_ATTR,
                    Value.newBuilder().
                        setDouble(60)
                        .setValueType(ValueType.DOUBLE)
                        .build()
                )
            ))
            .addOrderBy(orderByExpression)
            .setLimit(10)
            .setOffset(10)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof TotalFetcherNode);
    assertEquals("QS", ((TotalFetcherNode)executionTree).getSource());

    QueryNode paginateOnlyNode = ((TotalFetcherNode)executionTree).getChildNode();
    assertTrue(paginateOnlyNode instanceof PaginateOnlyNode);
    assertEquals(10, ((PaginateOnlyNode)paginateOnlyNode).getOffset());
    assertEquals(10, ((PaginateOnlyNode)paginateOnlyNode).getLimit());

    QueryNode selectAndFilterNode = ((PaginateOnlyNode)paginateOnlyNode).getChildNode();
    assertTrue(selectAndFilterNode instanceof SelectionAndFilterNode);
    assertEquals("QS", ((SelectionAndFilterNode)selectAndFilterNode).getSource());
    assertEquals(0, ((SelectionAndFilterNode)selectAndFilterNode).getOffset());
    assertEquals(20, ((SelectionAndFilterNode)selectAndFilterNode).getLimit());
  }

  @Test
  public void test_build_selectAttributeAndAggregateMetricWithDifferentSource_shouldCreateDifferentNode() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .setFilter(getTimeRangeFilter("API.startTime", System.currentTimeMillis() - 1000, System.currentTimeMillis()))
            .setLimit(10)
            .setOffset(0)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, entitiesRequest.getStartTimeMillis(), entitiesRequest.getEndTimeMillis(),
            "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(((SelectionNode) executionTree).getAggMetricSelectionSources().contains(AttributeSource.QS.name()));
    QueryNode firstChild = ((SelectionNode) executionTree).getChildNode();
    assertTrue(firstChild instanceof SelectionNode);
    assertTrue(((SelectionNode) firstChild).getAttrSelectionSources().contains(AttributeSource.EDS.name()));

    QueryNode secondChild = ((SelectionNode) firstChild).getChildNode();
    assertTrue(secondChild instanceof SortAndPaginateNode);
    assertEquals(entitiesRequest.getLimit(), ((SortAndPaginateNode) secondChild).getLimit());

    QueryNode thirdChild = ((SortAndPaginateNode) secondChild).getChildNode();
    assertTrue(thirdChild instanceof DataFetcherNode);
    assertEquals(AttributeSource.QS.name(), ((DataFetcherNode) thirdChild).getSource());
  }

  @Test
  public void test_build_selectAttributeWithFiltersWithDifferentSource_shouldCreateDifferentNode() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .setFilter(generateEQFilter(API_PATTERN_ATTR, "/login"))
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(((SelectionNode) executionTree).getAggMetricSelectionSources().contains(AttributeSource.QS.name()));
    QueryNode firstChild = ((SelectionNode) executionTree).getChildNode();
    assertTrue(firstChild instanceof DataFetcherNode);
    assertEquals(AttributeSource.EDS.name(), ((DataFetcherNode) firstChild).getSource());
  }
}
