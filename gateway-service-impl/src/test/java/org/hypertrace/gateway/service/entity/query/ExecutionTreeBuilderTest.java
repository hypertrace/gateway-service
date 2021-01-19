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
import java.util.Collections;
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
import org.hypertrace.gateway.service.entity.query.visitor.FilterOptimizingVisitor;
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

  private ExecutionContext getExecutionContextForOptimizedFilterTests(Filter filter) {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000;
    EntitiesRequest entitiesRequest = EntitiesRequest.newBuilder()
        .setStartTimeMillis(startTime).setEndTimeMillis(endTime)
        .setEntityType(AttributeScope.API.name())
        .setFilter(filter)
        .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, startTime, endTime, "API", "API.startTime", new HashMap<>());
    return ExecutionContext.from(attributeMetadataProvider, entityIdColumnsConfigs, entitiesRequest, entitiesRequestContext);
  }

  @Test
  public void testOptimizedFilterTreeBuilderSimpleFilter() {
    Filter filter = generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString());
    ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode queryNode =
        executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
    QueryNode optimizedQueryNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
    assertNotNull(optimizedQueryNode);
    assertTrue(optimizedQueryNode instanceof DataFetcherNode);
    assertEquals(((DataFetcherNode) optimizedQueryNode).getFilter(), filter);
  }

  @Test
  public void testOptimizedFilterTreeBuilderAndOrFilterSingleDataSource() {
    {
      Filter filter =
          generateAndOrNotFilter(
              Operator.AND,
              generateEQFilter(API_API_ID_ATTR, UUID.randomUUID().toString()),
              generateEQFilter(API_NAME_ATTR, "/login"),
              generateEQFilter(API_TYPE_ATTR, "http"));
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof AndNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof OrNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof DataFetcherNode);
      assertEquals(filter, ((DataFetcherNode) optimizedNode).getFilter());
    }
  }

  @Test
  public void testOptimizedFilterTreeBuilderAndOrFilterMultiDataSource() {
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof AndNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      assertTrue(queryNode instanceof OrNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
      assertNotNull(optimizedNode);
      assertTrue(optimizedNode instanceof AndNode);
      List<QueryNode> queryNodeList = ((AndNode) optimizedNode).getChildNodes();
      assertEquals(2, queryNodeList.size());
      queryNodeList.forEach(tn -> assertTrue(tn instanceof DataFetcherNode));
    }
  }

  @Test
  public void testOptimizedFilterTreeBuilderNestedAndOrFilter() {
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
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
      ExecutionContext executionContext = getExecutionContextForOptimizedFilterTests(filter);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode queryNode =
          executionTreeBuilder.buildFilterTree(executionContext.getEntitiesRequest(), filter);
      assertNotNull(queryNode);
      QueryNode optimizedNode = queryNode.acceptVisitor(new FilterOptimizingVisitor());
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
          ExecutionContext.from(
              attributeMetadataProvider,
              entityIdColumnsConfigs,
              entitiesRequest,
              entitiesRequestContext);
      ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
      QueryNode executionTree = executionTreeBuilder.build();
      assertNotNull(executionTree);

      assertTrue(executionTree instanceof DataFetcherNode);
      assertEquals("EDS", ((DataFetcherNode) executionTree).getSource());
      assertEquals(20, ((DataFetcherNode) executionTree).getOffset());
      assertEquals(10, ((DataFetcherNode) executionTree).getLimit());
      assertEquals(
          List.of(orderByExpression), ((DataFetcherNode) executionTree).getOrderByExpressionList());
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
    assertTrue(executionTree instanceof DataFetcherNode);
    assertEquals(10, ((DataFetcherNode)executionTree).getLimit());
    assertEquals(20, ((DataFetcherNode)executionTree).getOffset());
    assertEquals(List.of(), ((DataFetcherNode)executionTree).getOrderByExpressionList());
  }

  @Test
  public void test_build_selectAttributeAndAggregateMetricWithSameSource_shouldCreateDataFetcherNode() {
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
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(((SelectionNode) executionTree).getAggMetricSelectionSources().contains("QS"));

    QueryNode dataFetcherNode = ((SelectionNode)executionTree).getChildNode();
    assertTrue(dataFetcherNode instanceof DataFetcherNode);
    assertEquals("QS", ((DataFetcherNode)dataFetcherNode).getSource());
    assertEquals(0, ((DataFetcherNode)dataFetcherNode).getOffset());
    assertEquals(10, ((DataFetcherNode)dataFetcherNode).getLimit());
    assertEquals(
        Collections.emptyList(), ((DataFetcherNode) dataFetcherNode).getOrderByExpressionList());
  }

  @Test
  public void test_build_selectAttributesTimeAggregationAndFilterWithSameSource_shouldCreateDataFetcherNode() {
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
    assertTrue(executionTree instanceof  SelectionNode);
    assertTrue(((SelectionNode) executionTree).getTimeSeriesSelectionSources().contains("QS"));

    QueryNode selectionNode = ((SelectionNode) executionTree).getChildNode();
    assertTrue(selectionNode instanceof SelectionNode);
    assertTrue(((SelectionNode) selectionNode).getAggMetricSelectionSources().contains("QS"));

    QueryNode dataFetcherNode = ((SelectionNode)selectionNode).getChildNode();
    assertTrue(dataFetcherNode instanceof DataFetcherNode);
    assertEquals("QS", ((DataFetcherNode)dataFetcherNode).getSource());
    assertEquals(0, ((DataFetcherNode)dataFetcherNode).getOffset());
    assertEquals(10, ((DataFetcherNode)dataFetcherNode).getLimit());
  }

  @Test
  public void test_build_selectAttributesTimeAggregationFilterAndOrderByWithSameSource_shouldCreateDataFetcherNode() {
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
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(((SelectionNode) executionTree).getTimeSeriesSelectionSources().contains("QS"));

    QueryNode selectionNode = ((SelectionNode) executionTree).getChildNode();
    assertTrue(selectionNode instanceof SelectionNode);
    assertTrue(((SelectionNode) selectionNode).getAggMetricSelectionSources().contains("QS"));

    QueryNode dataFetcherNode = ((SelectionNode)selectionNode).getChildNode();
    assertTrue(dataFetcherNode instanceof DataFetcherNode);
    assertEquals("QS", ((DataFetcherNode)dataFetcherNode).getSource());
    assertEquals(0, ((DataFetcherNode)dataFetcherNode).getOffset());
    assertEquals(10, ((DataFetcherNode)dataFetcherNode).getLimit());
  }

  @Test
  public void test_build_selectAttributesAndFilterWithSameSourceNonZeroOffset_shouldCreateDataFetcherNodeAndPaginateOnlyNode() {
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
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(((SelectionNode) executionTree).getAggMetricSelectionSources().contains("QS"));

    QueryNode paginateOnlyNode = ((SelectionNode)executionTree).getChildNode();
    assertTrue(paginateOnlyNode instanceof PaginateOnlyNode);
    assertEquals(10, ((PaginateOnlyNode)paginateOnlyNode).getOffset());
    assertEquals(10, ((PaginateOnlyNode)paginateOnlyNode).getLimit());

    QueryNode dataFetcherNode = ((PaginateOnlyNode)paginateOnlyNode).getChildNode();
    assertTrue(dataFetcherNode instanceof DataFetcherNode);
    assertEquals("QS", ((DataFetcherNode)dataFetcherNode).getSource());
    assertEquals(0, ((DataFetcherNode)dataFetcherNode).getOffset());
    assertEquals(20, ((DataFetcherNode)dataFetcherNode).getLimit());
  }

  @Test
  public void test_build_selectAttributesAndFilterWithDifferentSourceNonZeroOffset_shouldCreateDataFetcherNodeAndPaginateOnlyNode() {
    // selections on EDS and QS
    // filters and order by on QS
    OrderByExpression orderByExpression = buildOrderByExpression(API_STATE_ATTR);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_TYPE_ATTR))
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
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(((SelectionNode) executionTree).getAggMetricSelectionSources().contains("QS"));

    QueryNode selectionNode = ((SelectionNode) executionTree).getChildNode();
    assertTrue(selectionNode instanceof SelectionNode);
    assertTrue(((SelectionNode) selectionNode).getAttrSelectionSources().contains("EDS"));

    QueryNode paginateOnlyNode = ((SelectionNode)selectionNode).getChildNode();
    assertTrue(paginateOnlyNode instanceof PaginateOnlyNode);
    assertEquals(10, ((PaginateOnlyNode)paginateOnlyNode).getOffset());
    assertEquals(10, ((PaginateOnlyNode)paginateOnlyNode).getLimit());

    QueryNode dataFetcherNode = ((PaginateOnlyNode)paginateOnlyNode).getChildNode();
    assertTrue(dataFetcherNode instanceof DataFetcherNode);
    assertEquals("QS", ((DataFetcherNode)dataFetcherNode).getSource());
    assertEquals(0, ((DataFetcherNode)dataFetcherNode).getOffset());
    assertEquals(20, ((DataFetcherNode)dataFetcherNode).getLimit());
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
            .addOrderBy(buildOrderByExpression(API_NAME_ATTR))
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
    assertTrue(firstChild instanceof SortAndPaginateNode);
    assertEquals(entitiesRequest.getLimit(), ((SortAndPaginateNode) firstChild).getLimit());

    QueryNode secondChild = ((SortAndPaginateNode) firstChild).getChildNode();
    assertTrue(secondChild instanceof SelectionNode);
    assertTrue(((SelectionNode) secondChild).getAttrSelectionSources().contains(AttributeSource.EDS.name()));

    QueryNode thirdChild = ((SelectionNode) secondChild).getChildNode();
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
    assertTrue(firstChild instanceof AndNode);
    List<QueryNode> childNodes = ((AndNode) firstChild).getChildNodes();
    assertEquals(2, childNodes.size());
    QueryNode firstChildNode = childNodes.get(0);
    assertTrue(firstChildNode instanceof DataFetcherNode);
    assertEquals(AttributeSource.QS.name(), ((DataFetcherNode) firstChildNode).getSource());

    QueryNode secondChildNode = childNodes.get(1);
    assertTrue(secondChildNode instanceof DataFetcherNode);
    assertEquals(AttributeSource.EDS.name(), ((DataFetcherNode) secondChildNode).getSource());
  }

  @Test
  public void test_build_includeResultsOutsideTimeRange() {
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
            .setIncludeNonLiveEntities(true)
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

  @Test
  public void test_build_filterAndOrderBySameSourceSets_paginationToDataSourceToQs() {
    // filter and order by on QS
    // selections on both EDS and QS
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .setFilter(getTimeRangeFilter(API_START_TIME_ATTR, System.currentTimeMillis() - 1000, System.currentTimeMillis()))
            .addOrderBy(buildOrderByExpression(API_NUM_CALLS_ATTR))
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
    assertTrue(secondChild instanceof DataFetcherNode);
    assertEquals(AttributeSource.QS.name(), ((DataFetcherNode) secondChild).getSource());
    assertEquals(10, ((DataFetcherNode) secondChild).getLimit());
    assertEquals(0, ((DataFetcherNode) secondChild).getOffset());
    assertEquals(1, ((DataFetcherNode) secondChild).getOrderByExpressionList().size());
  }

  @Test
  public void test_build_filterAndOrderBySameSourceSets_paginationToDataSourceToEds_nonLiveEntities() {
    // filter and order by on EDS
    // selections on both EDS and QS
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_ID_ATTR))
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(
                buildAggregateExpression(API_NUM_CALLS_ATTR,
                    FunctionType.SUM,
                    "SUM_numCalls",
                    List.of()))
            .setFilter(generateEQFilter(API_NAME_ATTR, "api1"))
            .addOrderBy(buildOrderByExpression(API_ID_ATTR))
            .setLimit(10)
            .setOffset(0)
            .setIncludeNonLiveEntities(true)
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

    QueryNode secondChild = ((SelectionNode) executionTree).getChildNode();
    assertTrue(secondChild instanceof DataFetcherNode);
    assertEquals(AttributeSource.EDS.name(), ((DataFetcherNode) secondChild).getSource());
    assertEquals(10, ((DataFetcherNode) secondChild).getLimit());
    assertEquals(0, ((DataFetcherNode) secondChild).getOffset());
    assertEquals(1, ((DataFetcherNode) secondChild).getOrderByExpressionList().size());
  }

  @Test
  public void nonLiveEntities_filtersOnOtherDataSourceThanEds() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_API_ID_ATTR))
            .setFilter(generateEQFilter(API_NUM_CALLS_ATTR, "123"))
            .setIncludeNonLiveEntities(true)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, entitiesRequest.getStartTimeMillis(), entitiesRequest.getEndTimeMillis(),
            "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            entitiesRequest,
            entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof SelectionNode);
    assertTrue(
        ((SelectionNode) executionTree)
            .getAttrSelectionSources()
            .contains(AttributeSource.EDS.name()));

    QueryNode firstChild = ((SelectionNode) executionTree).getChildNode();
    assertTrue(firstChild instanceof DataFetcherNode);
    // should be QS, because there is a filter on QS, even though `setIncludeNonLiveEntities` is set
    // to true
    assertEquals(AttributeSource.QS.name(), ((DataFetcherNode) firstChild).getSource());
  }

  @Test
  public void nonLiveEntities_noFilters_shouldFetchFromEds() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_API_ID_ATTR))
            .setIncludeNonLiveEntities(true)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, entitiesRequest.getStartTimeMillis(), entitiesRequest.getEndTimeMillis(),
            "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            entitiesRequest,
            entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof DataFetcherNode);
    // should be EDS, since `setIncludeNonLiveEntities` is set to true, and there are no other
    // filters
    assertEquals(AttributeSource.EDS.name(), ((DataFetcherNode) executionTree).getSource());
  }

  @Test
  public void nonLiveEntities_filterOnEds_shouldFetchFromEds() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .addSelection(buildExpression(API_API_ID_ATTR))
            .setFilter(generateEQFilter(API_NAME_ATTR, "apiName"))
            .setIncludeNonLiveEntities(true)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, entitiesRequest.getStartTimeMillis(), entitiesRequest.getEndTimeMillis(),
            "API", "API.startTime", new HashMap<>());
    ExecutionContext executionContext =
        ExecutionContext.from(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            entitiesRequest,
            entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    assertNotNull(executionTree);
    assertTrue(executionTree instanceof DataFetcherNode);
    // should be EDS, since `setIncludeNonLiveEntities` is set to true, and there are no other
    // filters
    assertEquals(AttributeSource.EDS.name(), ((DataFetcherNode) executionTree).getSource());
  }
}
