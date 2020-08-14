package org.hypertrace.gateway.service.entity.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.query.visitor.OptimizingVisitor;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ExecutionTreeBuilderTest {

  private static final String TENANT_ID = "tenant1";

  private static final String API_ID_ATTR = "API.apiId";
  private static final String API_NAME_ATTR = "API.name";
  private static final String API_TYPE_ATTR = "API.apiType";
  private static final String API_PATTERN_ATTR = "API.urlPattern";
  private static final String API_START_TIME_ATTR = "API.start_time_millis";
  private static final String API_END_TIME_ATTR = "API.end_time_millis";
  private static final String API_NUM_CALLS_ATTR = "API.numCalls";
  private static final String API_STATE_ATTR = "API.state";
  private static final String API_DISCOVERY_STATE = "API.apiDiscoveryState";

  private static final Map<String, AttributeMetadata> attributeSources =
      new HashMap<>() {
        {
          put(
              API_ID_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.EDS)));
          put(
              API_PATTERN_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.EDS)));
          put(
              API_NAME_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.EDS)));
          put(
              API_TYPE_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.EDS)));
          put(
              API_START_TIME_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.QS)));
          put(
              API_END_TIME_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.QS)));
          put(
              API_NUM_CALLS_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.QS)));
          put(
              API_STATE_ATTR,
              buildAttributeMetadataForSources(List.of(AttributeSource.QS)));
          put(
              API_DISCOVERY_STATE,
              buildAttributeMetadataForSources(List.of(AttributeSource.EDS, AttributeSource.QS)));
        }
      };

  @Mock private AttributeMetadataProvider attributeMetadataProvider;

  private static AttributeMetadata buildAttributeMetadataForSources(List<AttributeSource> sources) {
    return AttributeMetadata.newBuilder().addAllSources(sources).build();
  }

  @BeforeEach
  public void setup() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    Mockito.when(
            attributeMetadataProvider.getAttributesMetadata(
                any(RequestContext.class), Mockito.eq(AttributeScope.API)))
        .thenReturn(attributeSources);
  }

  private ExecutionTreeBuilder getExecutionTreeBuilderForOptimizedFilterTests() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder().setEntityType(AttributeScope.API.name()).build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
    return new ExecutionTreeBuilder(executionContext);
  }

  @Test
  public void testOptimizedFilterTreeBuilderSimpleFilter() {
    ExecutionTreeBuilder executionTreeBuilder = getExecutionTreeBuilderForOptimizedFilterTests();
    Filter filter = generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString());
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
          generateAndOrFilter(
              Operator.AND,
              generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString()),
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
          generateAndOrFilter(
              Operator.OR,
              generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString()),
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
    Filter apiIdFilter = generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString());
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
          generateAndOrFilter(Operator.AND, apiIdFilter, apiNameFilter, startTimeFilter);
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
                  generateAndOrFilter(Operator.AND, apiIdFilter, apiNameFilter), startTimeFilter)));
    }

    {
      Filter filter = generateAndOrFilter(Operator.OR, apiIdFilter, apiNameFilter, startTimeFilter);
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
                  generateAndOrFilter(Operator.OR, apiIdFilter, apiNameFilter), startTimeFilter)));
    }
  }

  @Test
  public void testOptimizedFilterTreeBuilderNestedAndFilter() {
    ExecutionTreeBuilder executionTreeBuilder = getExecutionTreeBuilderForOptimizedFilterTests();
    Filter apiIdFilter = generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString());
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
      Filter level2Filter = generateAndOrFilter(Operator.AND, apiIdFilter, startTimeFilter);
      Filter filter = generateAndOrFilter(Operator.AND, level2Filter, apiNameFilter);
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
                  generateAndOrFilter(Operator.AND, apiNameFilter, apiIdFilter), startTimeFilter)));
    }

    {
      Filter level2Filter = generateAndOrFilter(Operator.AND, apiIdFilter, apiNameFilter);
      Filter filter = generateAndOrFilter(Operator.AND, level2Filter, startTimeFilter);
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
                  generateAndOrFilter(Operator.AND, apiIdFilter, apiNameFilter), startTimeFilter)));
    }

    {
      Filter level3Filter = generateAndOrFilter(Operator.AND, endTimeFilter, apiPatternFilter);
      Filter level2Filter =
          generateAndOrFilter(Operator.AND, apiIdFilter, startTimeFilter, level3Filter);
      Filter filter = generateAndOrFilter(Operator.AND, level2Filter, apiNameFilter);
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
    Filter apiIdFilter = generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString());
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
      Filter level2Filter = generateAndOrFilter(Operator.AND, apiIdFilter, apiNameFilter);
      Filter filter = generateAndOrFilter(Operator.OR, level2Filter, startTimeFilter);
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
                  generateAndOrFilter(Operator.AND, apiIdFilter, apiNameFilter), startTimeFilter)));
    }

    {
      Filter level2Filter = generateAndOrFilter(Operator.AND, apiIdFilter, startTimeFilter);
      Filter filter = generateAndOrFilter(Operator.OR, level2Filter, apiNameFilter);
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
    OrderByExpression orderByExpression = buildOrderByExpression(API_ID_ATTR);
    {
      EntitiesRequest entitiesRequest =
          EntitiesRequest.newBuilder()
              .setEntityType(AttributeScope.API.name())
              .addSelection(buildExpression(API_NAME_ATTR))
              .setFilter(generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString()))
              .addOrderBy(orderByExpression)
              .setLimit(10)
              .setOffset(20)
              .build();
      EntitiesRequestContext entitiesRequestContext =
          new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
      ExecutionContext executionContext =
          ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
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
      EntitiesRequest entitiesRequest =
          EntitiesRequest.newBuilder()
              .setEntityType(AttributeScope.API.name())
              .addSelection(buildExpression(API_START_TIME_ATTR))
              .setFilter(generateEQFilter(API_ID_ATTR, UUID.randomUUID().toString()))
              .addOrderBy(orderByExpression)
              .setLimit(10)
              .setOffset(0)
              .build();
      EntitiesRequestContext entitiesRequestContext =
          new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
      ExecutionContext executionContext =
          ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode executionTree = executionTreeBuilder.build();
      assertNotNull(executionTree);
      assertTrue(executionTree instanceof SelectionNode);
      assertTrue(
          ((SelectionNode) executionTree)
              .getAttrSelectionSources()
              .contains(AttributeSource.QS.name()));
      QueryNode firstChild = ((SelectionNode) executionTree).getChildNode();
      assertTrue(firstChild instanceof SortAndPaginateNode);
      QueryNode grandchild = ((SortAndPaginateNode) firstChild).getChildNode();
      assertEquals(AttributeSource.EDS.name(), ((DataFetcherNode) grandchild).getSource());
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
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
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
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
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
            .setFilter(generateAndOrFilter(
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
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
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
            .setFilter(generateAndOrFilter(
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
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
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
            .setFilter(generateAndOrFilter(
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
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
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
            .setLimit(10)
            .setOffset(0)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0L, 10L, "API", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(attributeMetadataProvider, entitiesRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(
        ((SelectionNode) executionTree)
            .getAggMetricSelectionSources()
            .contains(AttributeSource.QS.name()));
    QueryNode firstChild = ((SelectionNode) executionTree).getChildNode();
    assertTrue(firstChild instanceof SortAndPaginateNode);
    QueryNode secondChild = ((SortAndPaginateNode) firstChild).getChildNode();
    assertTrue(secondChild instanceof SelectionNode);
    assertTrue(
        ((SelectionNode) secondChild)
            .getAttrSelectionSources()
            .contains(AttributeSource.EDS.name()));
  }

  private Filter generateAndOrFilter(Operator operator, Filter... filters) {
    return Filter.newBuilder()
        .setOperator(operator)
        .addAllChildFilter(Arrays.asList(filters))
        .build();
  }

  private Filter generateFilter(Operator operator, String columnName, Value columnValue) {
    return Filter.newBuilder()
        .setOperator(operator)
        .setLhs(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(columnName).build())
                .build())
        .setRhs(
            Expression.newBuilder()
                .setLiteral(LiteralConstant.newBuilder().setValue(columnValue).build())
                .build())
        .build();
  }

  private Filter generateEQFilter(String columnName, String columnValue) {
    return generateFilter(
        Operator.EQ,
        columnName,
        Value.newBuilder().setString(columnValue).setValueType(ValueType.STRING).build());
  }

  private OrderByExpression buildOrderByExpression(String columnName) {
    return OrderByExpression.newBuilder().setExpression(buildExpression(columnName)).build();
  }

  private Expression buildExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName).build())
        .build();
  }

  public Expression buildAggregateExpression(
      String columnName,
      FunctionType function,
      String alias,
      List<Expression> additionalArguments) {
    FunctionExpression.Builder functionBuilder =
        FunctionExpression.newBuilder()
            .setFunction(function)
            .setAlias(alias)
            .addArguments(buildExpression(columnName));
    if (!additionalArguments.isEmpty()) {
      additionalArguments.forEach(functionBuilder::addArguments);
    }
    return Expression.newBuilder().setFunction(functionBuilder).build();
  }

  public TimeAggregation buildTimeAggregation(int period,
                                              String columnName,
                                              FunctionType function,
                                              String alias,
                                              List<Expression> additionalArguments) {
    return TimeAggregation.newBuilder()
        .setPeriod(Period.newBuilder()
            .setValue(period)
            .setUnit(ChronoUnit.SECONDS.name())
        )
        .setAggregation(buildAggregateExpression(columnName, function, alias, additionalArguments))
        .build();
  }
}
