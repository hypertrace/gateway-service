package org.hypertrace.gateway.service.entity.query.visitor;

import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildAggregateExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildOrderByExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildTimeAggregation;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.compareEntityFetcherResponses;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateEQFilter;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.getAggregatedMetricValue;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.getStringValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.gateway.service.common.datafetcher.EntityDataServiceEntityFetcher;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.EntityQueryHandlerRegistry;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.SelectionAndFilterNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.TotalFetcherNode;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Interval;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.MetricSeries;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExecutionVisitorTest {
  private static final String QS_SOURCE = "QS";
  private static final String EDS_SOURCE = "EDS";
  private static final long START_TIME = 10000L;
  private static final long END_TIME = 90000L;
  private static final String ENTITY_TYPE = EntityType.API.name();

  private static final String API_ID_ATTR = "API.id";
  private static final String API_NAME_ATTR = "API.name";
  private static final String API_NUM_CALLS_ATTR = "API.numCalls";
  private static final String API_DURATION_ATTR = "API.duration";
  private static final String API_DISCOVERY_STATE = "API.apiDiscoveryState";

  private final EntityFetcherResponse result1 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id1"),
                  Entity.newBuilder().putAttribute("key11", getStringValue("value11")),
              EntityKey.of("id2"),
                  Entity.newBuilder().putAttribute("key12", getStringValue("value12")),
              EntityKey.of("id3"),
                  Entity.newBuilder().putAttribute("key13", getStringValue("value13"))));
  private final EntityFetcherResponse result2 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id1"),
                  Entity.newBuilder().putAttribute("key21", getStringValue("value21")),
              EntityKey.of("id2"),
                  Entity.newBuilder().putAttribute("key22", getStringValue("value22"))));
  private final EntityFetcherResponse result3 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id1"),
                  Entity.newBuilder().putAttribute("key31", getStringValue("value31")),
              EntityKey.of("id3"),
                  Entity.newBuilder().putAttribute("key33", getStringValue("value33"))));
  private final EntityFetcherResponse result4 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id4"),
              Entity.newBuilder().putAttribute("key41", getStringValue("value41"))));
  private static final EntitiesRequest ENTITIES_REQUEST = EntitiesRequest.newBuilder()
      .setStartTimeMillis(START_TIME)
      .setEndTimeMillis(END_TIME)
      .setEntityType(ENTITY_TYPE)
      .build();

  private EntityQueryHandlerRegistry entityQueryHandlerRegistry;
  private ExecutionContext executionContext;
  private ExecutionVisitor executionVisitor;
  private QueryServiceEntityFetcher queryServiceEntityFetcher;
  private EntityDataServiceEntityFetcher entityDataServiceEntityFetcher;

  @BeforeEach
  public void setup() {
    executionContext = mock(ExecutionContext.class);
    entityQueryHandlerRegistry = mock(EntityQueryHandlerRegistry.class);
    queryServiceEntityFetcher = mock(QueryServiceEntityFetcher.class);
    entityDataServiceEntityFetcher = mock(EntityDataServiceEntityFetcher.class);
    when(entityQueryHandlerRegistry.getEntityFetcher(QS_SOURCE)).thenReturn(queryServiceEntityFetcher);
    when(entityQueryHandlerRegistry.getEntityFetcher(EDS_SOURCE)).thenReturn(entityDataServiceEntityFetcher);
    executionVisitor = new ExecutionVisitor(executionContext, entityQueryHandlerRegistry);
  }

  @Test
  public void testIntersect() {
    {
      Map<EntityKey, Builder> finalResult =
          ExecutionVisitor.intersect(Arrays.asList(result1, result2, result3))
              .getEntityKeyBuilderMap();
      Assertions.assertEquals(1, finalResult.size());
      Entity.Builder builder = finalResult.get(EntityKey.of("id1"));
      Assertions.assertNotNull(builder);
      Assertions.assertEquals("value11", builder.getAttributeMap().get("key11").getString());
      Assertions.assertEquals("value21", builder.getAttributeMap().get("key21").getString());
      Assertions.assertEquals("value31", builder.getAttributeMap().get("key31").getString());
    }
    {
      Map<EntityKey, Builder> finalResult =
          ExecutionVisitor.intersect(Arrays.asList(result1, result2, result4))
              .getEntityKeyBuilderMap();
      assertTrue(finalResult.isEmpty());
    }
  }

  @Test
  public void testUnion() {
    {
      Map<EntityKey, Builder> finalResult =
          ExecutionVisitor.union(Arrays.asList(result1, result4)).getEntityKeyBuilderMap();
      Assertions.assertEquals(4, finalResult.size());
      assertTrue(
          finalResult
              .keySet()
              .containsAll(
                  Stream.of("id1", "id2", "id3", "id4")
                      .map(EntityKey::of)
                      .collect(Collectors.toList())));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id1")),
          finalResult.get(EntityKey.of("id1")));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id2")),
          finalResult.get(EntityKey.of("id2")));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id3")),
          finalResult.get(EntityKey.of("id3")));
      Assertions.assertEquals(
          result4.getEntityKeyBuilderMap().get(EntityKey.of("id4")),
          finalResult.get(EntityKey.of("id4")));
    }
  }

  @Test
  public void testConstructFilterFromChildNodesResultEmptyResults() {
    // Empty results.
    EntityFetcherResponse result = new EntityFetcherResponse();
    when(executionContext.getEntityIdExpressions())
        .thenReturn(
            List.of(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName("API.id")
                            .setAlias("entityId0")
                            .build())
                    .build()));

    Filter filter = executionVisitor.constructFilterFromChildNodesResult(result);

    Assertions.assertEquals(Filter.getDefaultInstance(), filter);
  }

  @Test
  public void testConstructFilterFromChildNodesNonEmptyResultsSingleEntityIdExpression() {
    EntityFetcherResponse result =
        new EntityFetcherResponse(
            Map.of(
                EntityKey.of("api0"), Entity.newBuilder(),
                EntityKey.of("api1"), Entity.newBuilder(),
                EntityKey.of("api2"), Entity.newBuilder()));
    Expression entityIdExpression =
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("API.id").setAlias("entityId0").build())
            .build();
    when(executionContext.getEntityIdExpressions()).thenReturn(List.of(entityIdExpression));

    Filter filter = executionVisitor.constructFilterFromChildNodesResult(result);

    Assertions.assertEquals(0, filter.getChildFilterCount());
    Assertions.assertEquals(entityIdExpression, filter.getLhs());
    Assertions.assertEquals(Operator.IN, filter.getOperator());
    Assertions.assertEquals(
        Set.of("api0", "api1", "api2"),
        new HashSet<>(filter.getRhs().getLiteral().getValue().getStringArrayList()));
    Assertions.assertEquals(
        ValueType.STRING_ARRAY, filter.getRhs().getLiteral().getValue().getValueType());
  }

  @Test
  public void testConstructFilterFromChildNodesNonEmptyResultsMultipleEntityIdExpressions() {
    EntityFetcherResponse result =
        new EntityFetcherResponse(
            Map.of(
                EntityKey.of("api0", "v10"), Entity.newBuilder(),
                EntityKey.of("api1", "v11"), Entity.newBuilder(),
                EntityKey.of("api2", "v12"), Entity.newBuilder()));
    Expression entityIdExpression0 =
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("API.id").setAlias("entityId0").build())
            .build();
    Expression entityIdExpression1 =
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder()
                    .setColumnName("API.someCol")
                    .setAlias("entityId1")
                    .build())
            .build();
    when(executionContext.getEntityIdExpressions())
        .thenReturn(List.of(entityIdExpression0, entityIdExpression1));

    Filter filter = executionVisitor.constructFilterFromChildNodesResult(result);

    Assertions.assertEquals(3, filter.getChildFilterCount());
    Assertions.assertEquals(Operator.OR, filter.getOperator());
    assertFalse(filter.hasLhs());
    assertFalse(filter.hasRhs());
    Assertions.assertEquals(
        Set.of(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression0)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("api0")
                                                .setValueType(ValueType.STRING)))))
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression1)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("v10")
                                                .setValueType(ValueType.STRING)))))
                .build(),
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression0)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("api1")
                                                .setValueType(ValueType.STRING)))))
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression1)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("v11")
                                                .setValueType(ValueType.STRING)))))
                .build(),
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression0)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("api2")
                                                .setValueType(ValueType.STRING)))))
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression1)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("v12")
                                                .setValueType(ValueType.STRING)))))
                .build()),
        new HashSet<>(filter.getChildFilterList()));
  }

  @Test
  public void test_visitSelectionAndFilterNode() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 10;
    int offset = 0;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildExpression(API_NAME_ATTR))
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext = new EntitiesRequestContext(
        tenantId,
        startTime,
        endTime,
        entityType.name(),
        "API.startTime",
        requestHeaders);
    Map<EntityKey, Builder> entityKeyBuilderResponseMap = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")),
        EntityKey.of("entity-id-1"), Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")),
        EntityKey.of("entity-id-2"), Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2"))
    );
    EntityFetcherResponse entityFetcherResponse = new EntityFetcherResponse(entityKeyBuilderResponseMap);
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getRequestHeaders()).thenReturn(requestHeaders);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(queryServiceEntityFetcher.getEntitiesAndAggregatedMetrics(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(entityFetcherResponse);
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse());

    SelectionAndFilterNode selectionAndFilterNode = new SelectionAndFilterNode("QS", limit, offset);

    compareEntityFetcherResponses(entityFetcherResponse, executionVisitor.visit(selectionAndFilterNode));
  }

  @Test
  public void test_visitSelectionAndFilterNodeEds() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 10;
    int offset = 0;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildExpression(API_NAME_ATTR))
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext = new EntitiesRequestContext(
        tenantId,
        startTime,
        endTime,
        entityType.name(),
        "API.startTime",
        requestHeaders);
    Map<EntityKey, Builder> entityKeyBuilderResponseMap = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")),
        EntityKey.of("entity-id-1"), Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")),
        EntityKey.of("entity-id-2"), Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2"))
    );
    EntityFetcherResponse entityFetcherResponse = new EntityFetcherResponse(entityKeyBuilderResponseMap);
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getRequestHeaders()).thenReturn(requestHeaders);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(entityDataServiceEntityFetcher.getEntities(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(entityFetcherResponse);

    SelectionAndFilterNode selectionAndFilterNode = new SelectionAndFilterNode("EDS", limit, offset);

    assertEquals(entityFetcherResponse, executionVisitor.visit(selectionAndFilterNode));
  }

  @Test
  public void test_visitSelectionAndTimeAggregationAndFilterNode() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 10;
    int offset = 0;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(buildAggregateExpression(API_DURATION_ATTR, FunctionType.AVG, "AVG_API.duration", List.of()))
            .addTimeAggregation(buildTimeAggregation(30, API_NUM_CALLS_ATTR, FunctionType.SUM, "SUM_API.numCalls", List.of()))
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext = new EntitiesRequestContext(
        tenantId,
        startTime,
        endTime,
        entityType.name(),
        "API.startTime",
        requestHeaders);
    Map<EntityKey, Builder> entityKeyBuilderResponseMap1 = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-0"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 12.0)),
        EntityKey.of("entity-id-1"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-1"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 13.0)),
        EntityKey.of("entity-id-2"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0))
    );
    Map<EntityKey, Builder> entityKeyBuilderResponseMap2 = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-1"), Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-2"), Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );

    Map<EntityKey, Builder> combinedEntityKeyBuilderResponseMap = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-0"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 12.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-1"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-1"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 13.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-2"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );

    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getRequestHeaders()).thenReturn(requestHeaders);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(queryServiceEntityFetcher.getEntitiesAndAggregatedMetrics(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap1));
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap2));

    SelectionAndFilterNode selectionAndFilterNode = new SelectionAndFilterNode("QS", limit, offset);

    compareEntityFetcherResponses(new EntityFetcherResponse(combinedEntityKeyBuilderResponseMap),
        executionVisitor.visit(selectionAndFilterNode));
  }

  @Test
  public void test_visitPaginateOnlyNode() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 2;
    int offset = 2;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(buildAggregateExpression(API_DURATION_ATTR, FunctionType.AVG, "AVG_API.duration", List.of()))
            .addTimeAggregation(buildTimeAggregation(30, API_NUM_CALLS_ATTR, FunctionType.SUM, "SUM_API.numCalls", List.of()))
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext = new EntitiesRequestContext(
        tenantId,
        startTime,
        endTime,
        entityType.name(),
        "API.startTime",
        requestHeaders);

    // Order matters since we will do the pagination ourselves. So we use a LinkedHashMap
    Map<EntityKey, Builder> entityKeyBuilderResponseMap1 = new LinkedHashMap<>();
    entityKeyBuilderResponseMap1.put(EntityKey.of("entity-id-0"), Entity.newBuilder()
        .putAttribute("API.name", getStringValue("entity-0"))
        .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 12.0))
    );
    entityKeyBuilderResponseMap1.put(EntityKey.of("entity-id-1"), Entity.newBuilder()
        .putAttribute("API.name", getStringValue("entity-1"))
        .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 13.0))
    );
    entityKeyBuilderResponseMap1.put(EntityKey.of("entity-id-2"), Entity.newBuilder()
        .putAttribute("API.name", getStringValue("entity-2"))
        .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0))
    );
    entityKeyBuilderResponseMap1.put(EntityKey.of("entity-id-3"), Entity.newBuilder()
        .putAttribute("API.name", getStringValue("entity-3"))
        .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 15.0))
    );
    Map<EntityKey, Builder> entityKeyBuilderResponseMap2 = new LinkedHashMap<>();
    entityKeyBuilderResponseMap2.put(EntityKey.of("entity-id-0"),
        Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );
    entityKeyBuilderResponseMap2.put(EntityKey.of("entity-id-1"),
        Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );
    entityKeyBuilderResponseMap2.put(EntityKey.of("entity-id-2"),
        Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );
    entityKeyBuilderResponseMap2.put(EntityKey.of("entity-id-3"),
        Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );

    Map<EntityKey, Builder> expectedEntityKeyBuilderResponseMap = new LinkedHashMap<>();
    expectedEntityKeyBuilderResponseMap.put(EntityKey.of("entity-id-2"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );
    expectedEntityKeyBuilderResponseMap.put(EntityKey.of("entity-id-3"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-3"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 15.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );

    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getRequestHeaders()).thenReturn(requestHeaders);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    EntitiesRequest entitiesRequestForEntityFetcher = EntitiesRequest.newBuilder(entitiesRequest)
        .setLimit(limit + offset)
        .setOffset(0)
        .build();
    when(queryServiceEntityFetcher.getEntitiesAndAggregatedMetrics(eq(entitiesRequestContext),
        eq(entitiesRequestForEntityFetcher)))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap1));
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(eq(entitiesRequestContext), eq(entitiesRequestForEntityFetcher)))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap2));

    SelectionAndFilterNode selectionAndFilterNode = new SelectionAndFilterNode("QS", limit + offset, 0);
    PaginateOnlyNode paginateOnlyNode = new PaginateOnlyNode(selectionAndFilterNode, limit, offset);

    compareEntityFetcherResponses(new EntityFetcherResponse(expectedEntityKeyBuilderResponseMap),
        executionVisitor.visit(paginateOnlyNode));
  }

  @Test
  public void test_visitTotalNode() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 10;
    int offset = 0;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(buildAggregateExpression(API_DURATION_ATTR, FunctionType.AVG, "AVG_API.duration", List.of()))
            .addTimeAggregation(buildTimeAggregation(30, API_NUM_CALLS_ATTR, FunctionType.SUM, "SUM_API.numCalls", List.of()))
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext = new EntitiesRequestContext(
        tenantId,
        startTime,
        endTime,
        entityType.name(),
        "API.startTime",
        requestHeaders);
    Map<EntityKey, Builder> entityKeyBuilderResponseMap1 = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-0"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 12.0)),
        EntityKey.of("entity-id-1"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-1"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 13.0)),
        EntityKey.of("entity-id-2"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0))
    );
    Map<EntityKey, Builder> entityKeyBuilderResponseMap2 = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-1"), Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-2"), Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );

    Map<EntityKey, Builder> combinedEntityKeyBuilderResponseMap = Map.of(
        EntityKey.of("entity-id-0"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-0"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 12.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-1"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-1"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 13.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")),
        EntityKey.of("entity-id-2"), Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM"))
    );
    //EntityFetcherResponse entityFetcherResponse = new EntityFetcherResponse(entityKeyBuilderResponseMap1);
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getRequestHeaders()).thenReturn(requestHeaders);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(queryServiceEntityFetcher.getEntitiesAndAggregatedMetrics(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap1));
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap2));

    when(executionContext.getEntitiesRequestContext()).thenReturn(entitiesRequestContext);
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(queryServiceEntityFetcher.getTotalEntities(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(12);

    SelectionAndFilterNode selectionAndFilterNode = new SelectionAndFilterNode("QS", limit, offset);
    TotalFetcherNode totalFetcherNode = new TotalFetcherNode(selectionAndFilterNode, "QS");

    compareEntityFetcherResponses(new EntityFetcherResponse(combinedEntityKeyBuilderResponseMap),
        executionVisitor.visit(totalFetcherNode));
    verify(executionContext, times(1)).setTotal(eq(12));
  }

  @Test
  public void test_visitSelectionNode_differentSource_callSeparatedCalls() {
    ExecutionVisitor executionVisitor =
        spy(new ExecutionVisitor(executionContext, entityQueryHandlerRegistry));
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    SelectionNode selectionNode = new SelectionNode.Builder(new NoOpNode())
        .setAttrSelectionSources(Set.of(EDS_SOURCE))
        .setAggMetricSelectionSources(Set.of(QS_SOURCE))
        .build();
    mockExecutionContext(
        Set.of(EDS_SOURCE),
        Set.of(QS_SOURCE),
        Map.of(EDS_SOURCE, Collections.emptyList()),
        Map.of(QS_SOURCE, Collections.emptyList()));
    when(entityDataServiceEntityFetcher.getEntities(any(), any())).thenReturn(result4);
    when(queryServiceEntityFetcher.getAggregatedMetrics(any(), any())).thenReturn(result4);
    when(executionVisitor.visit(any(NoOpNode.class))).thenReturn(result4);
    executionVisitor.visit(selectionNode);
    verify(entityDataServiceEntityFetcher).getEntities(any(), any());
    verify(queryServiceEntityFetcher).getAggregatedMetrics(any(), any());
  }

  @Test
  public void test_visitSelectionNode_nonEmptyFilter_emptyResult() {
    // Create a request with non-empty filter.
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.API.name())
            .setStartTimeMillis(10)
            .setEndTimeMillis(20)
            .addSelection(buildExpression(API_NAME_ATTR))
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .build();
    ExecutionVisitor executionVisitor =
        spy(new ExecutionVisitor(executionContext, entityQueryHandlerRegistry));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);

    // Selection node with NoOp child, to short-circuit the call to first service.
    SelectionNode selectionNode = new SelectionNode.Builder(new NoOpNode())
        .setAttrSelectionSources(Set.of(EDS_SOURCE))
        .setAggMetricSelectionSources(Set.of(QS_SOURCE))
        .build();

    EntityFetcherResponse response = executionVisitor.visit(selectionNode);
    Assertions.assertTrue(response.isEmpty());
    verify(queryServiceEntityFetcher, never()).getEntities(any(), any());
    verify(queryServiceEntityFetcher, never()).getAggregatedMetrics(any(), any());
  }

  private MetricSeries getMockMetricSeries(int period, String aggregation) {
    return MetricSeries.newBuilder()
        .setPeriod(
            Period.newBuilder()
                .setUnit("Seconds")
                .setValue(period)
                .build()
        )
        .addValue(
            Interval.newBuilder()
                .setStartTimeMillis(0)
                .setEndTimeMillis(1)
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(10.0))
        )
        .addValue(
            Interval.newBuilder()
                .setStartTimeMillis(1)
                .setEndTimeMillis(2)
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(11.0))
        )
        .addValue(
            Interval.newBuilder()
                .setStartTimeMillis(2)
                .setEndTimeMillis(3)
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(12.0))
        )
        .setAggregation(aggregation)
        .build();
  }

  private ExecutionContext mockExecutionContext(
      Set<String> selectionSource,
      Set<String> aggregateSource,
      Map<String, List<Expression>> sourceToSelectionMap,
      Map<String, List<Expression>> sourceToAggregateMap) {
    when(executionContext.getEntityIdExpressions()).thenReturn(Collections.singletonList(
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(API_ID_ATTR))
            .build()
    ));

    when(executionContext.getTenantId()).thenReturn("tenantId");
    when(executionContext.getRequestHeaders()).thenReturn(Collections.emptyMap());
    when(executionContext.getEntitiesRequest()).thenReturn(ENTITIES_REQUEST);
    when(executionContext.getPendingSelectionSources()).thenReturn(selectionSource);
    when(executionContext.getPendingMetricAggregationSources()).thenReturn(aggregateSource);
    when(executionContext.getSourceToSelectionExpressionMap()).thenReturn(sourceToSelectionMap);
    when(executionContext.getSourceToMetricExpressionMap()).thenReturn(sourceToAggregateMap);
    return executionContext;
  }

}
