package org.hypertrace.gateway.service.entity.query.visitor;

import static org.hypertrace.core.grpcutils.context.RequestContext.forTenantId;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildAggregateExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildOrderByExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildTimeAggregation;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.compareEntityResponses;
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
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.common.datafetcher.EntityDataServiceEntityFetcher;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.EntityResponse;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.EntityQueryHandlerRegistry;
import org.hypertrace.gateway.service.entity.query.DataFetcherNode;
import org.hypertrace.gateway.service.entity.query.EntityExecutionContext;
import org.hypertrace.gateway.service.entity.query.NoOpNode;
import org.hypertrace.gateway.service.entity.query.PaginateOnlyNode;
import org.hypertrace.gateway.service.entity.query.SelectionNode;
import org.hypertrace.gateway.service.entity.query.SortAndPaginateNode;
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
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
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
  private static final EntitiesRequest ENTITIES_REQUEST =
      EntitiesRequest.newBuilder()
          .setStartTimeMillis(START_TIME)
          .setEndTimeMillis(END_TIME)
          .setEntityType(ENTITY_TYPE)
          .build();

  private EntityQueryHandlerRegistry entityQueryHandlerRegistry;
  private ExpressionContext expressionContext;
  private EntityExecutionContext executionContext;
  private ExecutionVisitor executionVisitor;
  private QueryServiceEntityFetcher queryServiceEntityFetcher;
  private EntityDataServiceEntityFetcher entityDataServiceEntityFetcher;

  @BeforeEach
  public void setup() {
    expressionContext = mock(ExpressionContext.class);
    executionContext = mock(EntityExecutionContext.class);
    when(executionContext.getExpressionContext()).thenReturn(expressionContext);
    entityQueryHandlerRegistry = mock(EntityQueryHandlerRegistry.class);
    queryServiceEntityFetcher = mock(QueryServiceEntityFetcher.class);
    entityDataServiceEntityFetcher = mock(EntityDataServiceEntityFetcher.class);
    when(entityQueryHandlerRegistry.getEntityFetcher(QS_SOURCE))
        .thenReturn(queryServiceEntityFetcher);
    when(entityQueryHandlerRegistry.getEntityFetcher(EDS_SOURCE))
        .thenReturn(entityDataServiceEntityFetcher);
    executionVisitor =
        new ExecutionVisitor(
            executionContext, entityQueryHandlerRegistry, Executors.newSingleThreadExecutor());
  }

  @Test
  public void testIntersect() {
    {
      EntityResponse finalResult =
          ExecutionVisitor.intersect(
              Arrays.asList(
                  new EntityResponse(result1, result1.getEntityKeyBuilderMap().size()),
                  new EntityResponse(result2, result2.getEntityKeyBuilderMap().size()),
                  new EntityResponse(result3, result3.getEntityKeyBuilderMap().size())));

      Map<EntityKey, Builder> finalEntities =
          finalResult.getEntityFetcherResponse().getEntityKeyBuilderMap();
      long total = finalResult.getTotal();

      Assertions.assertEquals(1, finalEntities.size());
      Assertions.assertEquals(1, total);

      Entity.Builder builder = finalEntities.get(EntityKey.of("id1"));
      Assertions.assertNotNull(builder);
      Assertions.assertEquals("value11", builder.getAttributeMap().get("key11").getString());
      Assertions.assertEquals("value21", builder.getAttributeMap().get("key21").getString());
      Assertions.assertEquals("value31", builder.getAttributeMap().get("key31").getString());
    }
    {
      EntityResponse finalResult =
          ExecutionVisitor.intersect(
              Arrays.asList(
                  new EntityResponse(result1, result1.getEntityKeyBuilderMap().size()),
                  new EntityResponse(result2, result2.getEntityKeyBuilderMap().size()),
                  new EntityResponse(result4, result4.getEntityKeyBuilderMap().size())));

      Map<EntityKey, Builder> finalEntities =
          finalResult.getEntityFetcherResponse().getEntityKeyBuilderMap();
      long total = finalResult.getTotal();
      assertTrue(finalEntities.isEmpty());
      assertEquals(0, total);
    }
  }

  @Test
  public void testUnion() {
    {
      EntityResponse finalResult =
          ExecutionVisitor.union(
              Arrays.asList(
                  new EntityResponse(result1, result1.getEntityKeyBuilderMap().size()),
                  new EntityResponse(result4, result4.getEntityKeyBuilderMap().size())));

      Map<EntityKey, Builder> finalEntities =
          finalResult.getEntityFetcherResponse().getEntityKeyBuilderMap();
      long total = finalResult.getTotal();

      Assertions.assertEquals(4, finalEntities.size());
      Assertions.assertEquals(4, total);
      assertTrue(
          finalEntities
              .keySet()
              .containsAll(
                  Stream.of("id1", "id2", "id3", "id4")
                      .map(EntityKey::of)
                      .collect(Collectors.toList())));
      assertEquals(4, total);
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id1")),
          finalEntities.get(EntityKey.of("id1")));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id2")),
          finalEntities.get(EntityKey.of("id2")));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id3")),
          finalEntities.get(EntityKey.of("id3")));
      Assertions.assertEquals(
          result4.getEntityKeyBuilderMap().get(EntityKey.of("id4")),
          finalEntities.get(EntityKey.of("id4")));
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
  public void test_visitDataFetcherNodeQs() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 10;
    int offset = 0;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    AttributeScope entityType = AttributeScope.API;
    Expression selectionExpression = buildExpression(API_NAME_ATTR);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(selectionExpression)
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");
    Map<EntityKey, Builder> entityKeyBuilderResponseMap =
        Map.of(
            EntityKey.of("entity-id-0"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")),
            EntityKey.of("entity-id-1"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")),
            EntityKey.of("entity-id-2"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2")));

    EntityFetcherResponse entityFetcherResponse =
        new EntityFetcherResponse(entityKeyBuilderResponseMap);

    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(Map.of("QS", List.of(selectionExpression)));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getEntitiesRequestContext()).thenReturn(entitiesRequestContext);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(queryServiceEntityFetcher.getEntities(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(entityFetcherResponse);
    when(queryServiceEntityFetcher.getTotal(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(100L);
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(
            eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse());

    DataFetcherNode dataFetcherNode =
        new DataFetcherNode(
            "QS", entitiesRequest.getFilter(), limit, offset, orderByExpressions, true);

    compareEntityResponses(
        new EntityResponse(entityFetcherResponse, 100L), executionVisitor.visit(dataFetcherNode));
  }

  @Test
  public void test_visitDataFetcherNode_cannotFetchTotal() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 10;
    int offset = 0;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    AttributeScope entityType = AttributeScope.API;
    Expression selectionExpression = buildExpression(API_NAME_ATTR);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(selectionExpression)
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");
    Map<EntityKey, Builder> entityKeyBuilderResponseMap =
        Map.of(
            EntityKey.of("entity-id-0"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")),
            EntityKey.of("entity-id-1"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")),
            EntityKey.of("entity-id-2"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2")));

    EntityFetcherResponse entityFetcherResponse =
        new EntityFetcherResponse(entityKeyBuilderResponseMap);
    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(Map.of("QS", List.of(selectionExpression)));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getEntitiesRequestContext()).thenReturn(entitiesRequestContext);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(queryServiceEntityFetcher.getEntities(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(entityFetcherResponse);
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(
            eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse());

    DataFetcherNode dataFetcherNode =
        new DataFetcherNode(
            "QS", entitiesRequest.getFilter(), limit, offset, orderByExpressions, false);

    compareEntityResponses(
        new EntityResponse(
            entityFetcherResponse, entityFetcherResponse.getEntityKeyBuilderMap().size()),
        executionVisitor.visit(dataFetcherNode));
  }

  @Test
  public void test_visitDataFetcherNodeEds() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 10;
    int offset = 0;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    AttributeScope entityType = AttributeScope.API;
    Expression selectionExpression = buildExpression(API_NAME_ATTR);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(selectionExpression)
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");
    Map<EntityKey, Builder> entityKeyBuilderResponseMap =
        Map.of(
            EntityKey.of("entity-id-0"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")),
            EntityKey.of("entity-id-1"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")),
            EntityKey.of("entity-id-2"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2")));
    EntityFetcherResponse entityFetcherResponse =
        new EntityFetcherResponse(entityKeyBuilderResponseMap);

    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(Map.of("EDS", List.of(selectionExpression)));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getEntitiesRequestContext()).thenReturn(entitiesRequestContext);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(entityDataServiceEntityFetcher.getEntities(
            eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(entityFetcherResponse);
    when(entityDataServiceEntityFetcher.getTotal(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(100L);
    DataFetcherNode dataFetcherNode =
        new DataFetcherNode(
            "EDS", entitiesRequest.getFilter(), limit, offset, orderByExpressions, true);

    compareEntityResponses(
        new EntityResponse(entityFetcherResponse, 100), executionVisitor.visit(dataFetcherNode));
  }

  @Test
  public void test_visitDataFetcherNodeWithoutPagination() {
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    AttributeScope entityType = AttributeScope.API;
    Expression selectionExpression = buildExpression(API_NAME_ATTR);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(selectionExpression)
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");
    Map<EntityKey, Builder> entityKeyBuilderResponseMap =
        Map.of(
            EntityKey.of("entity-id-0"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")),
            EntityKey.of("entity-id-1"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")),
            EntityKey.of("entity-id-2"),
                Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2")));

    EntityFetcherResponse entityFetcherResponse =
        new EntityFetcherResponse(entityKeyBuilderResponseMap);
    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(Map.of("QS", List.of(selectionExpression)));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getEntitiesRequestContext()).thenReturn(entitiesRequestContext);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    when(queryServiceEntityFetcher.getEntities(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(entityFetcherResponse);
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(
            eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse());

    // no pagination in data fetcher node
    DataFetcherNode dataFetcherNode = new DataFetcherNode("QS", entitiesRequest.getFilter());

    compareEntityResponses(
        new EntityResponse(
            entityFetcherResponse, entityFetcherResponse.getEntityKeyBuilderMap().size()),
        executionVisitor.visit(dataFetcherNode));
    verify(queryServiceEntityFetcher, times(1)).getEntities(any(), any());
  }

  @Test
  public void test_visitPaginateOnlyNode() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 2;
    int offset = 2;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    AttributeScope entityType = AttributeScope.API;
    Expression selectionExpression = buildExpression(API_NAME_ATTR);
    Expression metricExpression =
        buildAggregateExpression(
            API_DURATION_ATTR, FunctionType.AVG, "AVG_API.duration", List.of());
    TimeAggregation timeAggregation =
        buildTimeAggregation(
            30, API_NUM_CALLS_ATTR, FunctionType.SUM, "SUM_API.numCalls", List.of());
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(selectionExpression)
            .addSelection(metricExpression)
            .addTimeAggregation(timeAggregation)
            .setFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED"))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");

    // Order matters since we will do the pagination ourselves. So we use a LinkedHashMap
    Map<EntityKey, Builder> entityKeyBuilderResponseMap1 = new LinkedHashMap<>();
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-0"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")));
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-1"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")));
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-2"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2")));
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-3"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-3")));

    Map<EntityKey, Builder> entityKeyBuilderResponseMap2 = new LinkedHashMap<>();
    entityKeyBuilderResponseMap2.put(
        EntityKey.of("entity-id-2"),
        Entity.newBuilder()
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0)));
    entityKeyBuilderResponseMap2.put(
        EntityKey.of("entity-id-3"),
        Entity.newBuilder()
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 15.0)));

    Map<EntityKey, Builder> entityKeyBuilderResponseMap3 = new LinkedHashMap<>();
    entityKeyBuilderResponseMap3.put(
        EntityKey.of("entity-id-2"),
        Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")));
    entityKeyBuilderResponseMap3.put(
        EntityKey.of("entity-id-3"),
        Entity.newBuilder().putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")));

    Map<EntityKey, Builder> expectedEntityKeyBuilderResponseMap = new LinkedHashMap<>();
    expectedEntityKeyBuilderResponseMap.put(
        EntityKey.of("entity-id-2"),
        Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")));
    expectedEntityKeyBuilderResponseMap.put(
        EntityKey.of("entity-id-3"),
        Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-3"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 15.0))
            .putMetricSeries("SUM_API.numCalls", getMockMetricSeries(30, "SUM")));

    when(executionContext.getEntityIdExpressions())
        .thenReturn(List.of(buildExpression(API_ID_ATTR)));
    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(Map.of("QS", List.of(selectionExpression)));
    when(expressionContext.getSourceToMetricExpressionMap())
        .thenReturn(Map.of("QS", List.of(metricExpression)));
    when(expressionContext.getSourceToTimeAggregationMap())
        .thenReturn(Map.of("QS", List.of(timeAggregation)));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getEntitiesRequestContext()).thenReturn(entitiesRequestContext);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    EntitiesRequest entitiesRequestForAttributes =
        EntitiesRequest.newBuilder(entitiesRequest)
            .clearSelection()
            .clearTimeAggregation()
            .addSelection(selectionExpression)
            .setLimit(limit + offset)
            .setOffset(0)
            .build();
    EntityFetcherResponse attributesResponse =
        new EntityFetcherResponse(entityKeyBuilderResponseMap1);
    EntitiesRequest entitiesRequestForMetricAggregation =
        EntitiesRequest.newBuilder(entitiesRequest)
            .clearLimit()
            .clearOffset()
            .clearOrderBy()
            .clearSelection()
            .clearTimeAggregation()
            .addSelection(metricExpression)
            .clearFilter()
            .setFilter(generateInFilter(API_ID_ATTR, List.of("entity-id-3", "entity-id-2")))
            .build();
    EntitiesRequest entitiesRequestForTimeAggregation =
        EntitiesRequest.newBuilder(entitiesRequest)
            .clearSelection()
            .clearLimit()
            .clearOffset()
            .clearOrderBy()
            .clearFilter()
            .setFilter(generateInFilter(API_ID_ATTR, List.of("entity-id-3", "entity-id-2")))
            .build();
    when(queryServiceEntityFetcher.getEntities(
            entitiesRequestContext, entitiesRequestForAttributes))
        .thenReturn(attributesResponse);
    when(queryServiceEntityFetcher.getTotal(eq(entitiesRequestContext), eq(entitiesRequest)))
        .thenReturn(100L);
    when(queryServiceEntityFetcher.getEntities(
            entitiesRequestContext, entitiesRequestForMetricAggregation))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap2));
    when(queryServiceEntityFetcher.getTimeAggregatedMetrics(
            entitiesRequestContext, entitiesRequestForTimeAggregation))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap3));

    DataFetcherNode dataFetcherNode =
        new DataFetcherNode(
            "QS", entitiesRequest.getFilter(), limit + offset, 0, orderByExpressions, true);
    PaginateOnlyNode paginateOnlyNode = new PaginateOnlyNode(dataFetcherNode, limit, offset);
    SelectionNode childSelectionNode =
        new SelectionNode.Builder(paginateOnlyNode)
            .setAggMetricSelectionSources(Set.of("QS"))
            .build();
    SelectionNode selectionNode =
        new SelectionNode.Builder(childSelectionNode)
            .setTimeSeriesSelectionSources(Set.of("QS"))
            .build();

    compareEntityResponses(
        new EntityResponse(new EntityFetcherResponse(expectedEntityKeyBuilderResponseMap), 100),
        executionVisitor.visit(selectionNode));
  }

  @Test
  public void test_visitSelectionNode_differentSource_callSeparatedCalls() {
    ExecutionVisitor executionVisitor =
        spy(
            new ExecutionVisitor(
                executionContext, entityQueryHandlerRegistry, Executors.newSingleThreadExecutor()));
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    SelectionNode selectionNode =
        new SelectionNode.Builder(new NoOpNode())
            .setAttrSelectionSources(Set.of(EDS_SOURCE))
            .setAggMetricSelectionSources(Set.of(QS_SOURCE))
            .build();
    mockExecutionContext(
        Set.of(EDS_SOURCE),
        Set.of(QS_SOURCE),
        Map.of(EDS_SOURCE, Collections.emptyList()),
        Map.of(QS_SOURCE, Collections.emptyList()));
    when(entityDataServiceEntityFetcher.getEntities(any(), any())).thenReturn(result4);
    when(queryServiceEntityFetcher.getEntities(any(), any())).thenReturn(result4);
    when(executionVisitor.visit(any(NoOpNode.class)))
        .thenReturn(new EntityResponse(result4, result4.getEntityKeyBuilderMap().size()));
    when(executionContext.getEntitiesRequestContext())
        .thenReturn(mock(EntitiesRequestContext.class));
    executionVisitor.visit(selectionNode);
    verify(entityDataServiceEntityFetcher).getEntities(any(), any());
    verify(queryServiceEntityFetcher).getEntities(any(), any());
  }

  @Test
  public void test_visitOnlySelectionsNode_shouldSetTotalEntityKeys() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    int limit = 2;
    int offset = 2;
    long startTime = 0;
    long endTime = 10;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    Expression selectionExpression = buildExpression(API_NAME_ATTR);
    Expression metricExpression =
        buildAggregateExpression(
            API_DURATION_ATTR, FunctionType.AVG, "AVG_API.duration", List.of());
    TimeAggregation timeAggregation =
        buildTimeAggregation(
            30, API_NUM_CALLS_ATTR, FunctionType.SUM, "SUM_API.numCalls", List.of());
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setFilter(Filter.getDefaultInstance())
            .addSelection(selectionExpression)
            .addSelection(metricExpression)
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");

    // Order matters since we will do the pagination ourselves. So we use a LinkedHashMap
    Map<EntityKey, Builder> entityKeyBuilderResponseMap1 = new LinkedHashMap<>();
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-0"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-0")));
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-1"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-1")));
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-2"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-2")));
    entityKeyBuilderResponseMap1.put(
        EntityKey.of("entity-id-3"),
        Entity.newBuilder().putAttribute("API.name", getStringValue("entity-3")));

    Map<EntityKey, Builder> entityKeyBuilderResponseMap2 = new LinkedHashMap<>();
    entityKeyBuilderResponseMap2.put(
        EntityKey.of("entity-id-2"),
        Entity.newBuilder()
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0)));
    entityKeyBuilderResponseMap2.put(
        EntityKey.of("entity-id-3"),
        Entity.newBuilder()
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 15.0)));

    Map<EntityKey, Builder> expectedEntityKeyBuilderResponseMap = new LinkedHashMap<>();
    expectedEntityKeyBuilderResponseMap.put(
        EntityKey.of("entity-id-2"),
        Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0)));
    expectedEntityKeyBuilderResponseMap.put(
        EntityKey.of("entity-id-3"),
        Entity.newBuilder()
            .putAttribute("API.name", getStringValue("entity-3"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 15.0)));

    when(executionContext.getEntityIdExpressions())
        .thenReturn(List.of(buildExpression(API_ID_ATTR)));
    when(expressionContext.getSourceToSelectionExpressionMap())
        .thenReturn(Map.of("QS", List.of(selectionExpression)));
    when(expressionContext.getSourceToMetricExpressionMap())
        .thenReturn(Map.of("QS", List.of(metricExpression)));
    when(expressionContext.getSourceToTimeAggregationMap())
        .thenReturn(Map.of("QS", List.of(timeAggregation)));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);
    when(executionContext.getTenantId()).thenReturn(tenantId);
    when(executionContext.getEntitiesRequestContext()).thenReturn(entitiesRequestContext);
    when(executionContext.getTimestampAttributeId()).thenReturn("API.startTime");
    EntitiesRequest entitiesRequestForAttributes =
        EntitiesRequest.newBuilder(entitiesRequest)
            .clearLimit()
            .clearOffset()
            .clearOrderBy()
            .clearSelection()
            .clearTimeAggregation()
            .addSelection(selectionExpression)
            .build();
    EntityFetcherResponse attributesResponse =
        new EntityFetcherResponse(entityKeyBuilderResponseMap1);
    EntitiesRequest entitiesRequestForMetricAggregation =
        EntitiesRequest.newBuilder(entitiesRequest)
            .clearLimit()
            .clearOffset()
            .clearOrderBy()
            .clearSelection()
            .clearTimeAggregation()
            .addSelection(metricExpression)
            .clearFilter()
            .setFilter(generateInFilter(API_ID_ATTR, List.of("entity-id-3", "entity-id-2")))
            .build();
    when(queryServiceEntityFetcher.getEntities(
            entitiesRequestContext, entitiesRequestForAttributes))
        .thenReturn(attributesResponse);
    when(queryServiceEntityFetcher.getEntities(
            entitiesRequestContext, entitiesRequestForMetricAggregation))
        .thenReturn(new EntityFetcherResponse(entityKeyBuilderResponseMap2));

    SelectionNode childSelectionNode =
        new SelectionNode.Builder(new NoOpNode()).setAttrSelectionSources(Set.of("QS")).build();
    SortAndPaginateNode sortAndPaginateNode =
        new SortAndPaginateNode(childSelectionNode, limit, offset, orderByExpressions);
    SelectionNode selectionNode =
        new SelectionNode.Builder(sortAndPaginateNode)
            .setAggMetricSelectionSources(Set.of("QS"))
            .build();

    // child selection node has no child data fetcher node. it should set total entity keys
    {
      compareEntityResponses(
          new EntityResponse(new EntityFetcherResponse(entityKeyBuilderResponseMap1), 4),
          executionVisitor.visit(childSelectionNode));
    }

    // selection node has child nodes. it should not set total entity keys, and fallback to the
    // total entity keys set by the child selection node
    {
      compareEntityResponses(
          new EntityResponse(new EntityFetcherResponse(expectedEntityKeyBuilderResponseMap), 4),
          executionVisitor.visit(selectionNode));
    }
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
        spy(
            new ExecutionVisitor(
                executionContext, entityQueryHandlerRegistry, Executors.newSingleThreadExecutor()));
    when(executionContext.getEntitiesRequest()).thenReturn(entitiesRequest);

    // Selection node with NoOp child, to short-circuit the call to first service.
    SelectionNode selectionNode =
        new SelectionNode.Builder(new NoOpNode())
            .setAttrSelectionSources(Set.of(EDS_SOURCE))
            .setAggMetricSelectionSources(Set.of(QS_SOURCE))
            .build();

    EntityResponse response = executionVisitor.visit(selectionNode);
    Assertions.assertTrue(response.getEntityFetcherResponse().isEmpty());
    Assertions.assertEquals(0, response.getTotal());
    verify(queryServiceEntityFetcher, never()).getEntities(any(), any());
  }

  private MetricSeries getMockMetricSeries(int period, String aggregation) {
    return MetricSeries.newBuilder()
        .setPeriod(Period.newBuilder().setUnit("Seconds").setValue(period).build())
        .addValue(
            Interval.newBuilder()
                .setStartTimeMillis(0)
                .setEndTimeMillis(1)
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(10.0)))
        .addValue(
            Interval.newBuilder()
                .setStartTimeMillis(1)
                .setEndTimeMillis(2)
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(11.0)))
        .addValue(
            Interval.newBuilder()
                .setStartTimeMillis(2)
                .setEndTimeMillis(3)
                .setValue(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(12.0)))
        .setAggregation(aggregation)
        .build();
  }

  private EntityExecutionContext mockExecutionContext(
      Set<String> selectionSource,
      Set<String> aggregateSource,
      Map<String, List<Expression>> sourceToSelectionMap,
      Map<String, List<Expression>> sourceToAggregateMap) {
    when(executionContext.getEntityIdExpressions())
        .thenReturn(
            Collections.singletonList(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(API_ID_ATTR))
                    .build()));

    when(executionContext.getTenantId()).thenReturn("tenantId");
    when(executionContext.getRequestHeaders()).thenReturn(Collections.emptyMap());
    when(executionContext.getEntitiesRequest()).thenReturn(ENTITIES_REQUEST);
    when(executionContext.getPendingSelectionSources()).thenReturn(selectionSource);
    when(executionContext.getPendingMetricAggregationSources()).thenReturn(aggregateSource);
    when(expressionContext.getSourceToSelectionExpressionMap()).thenReturn(sourceToSelectionMap);
    when(expressionContext.getSourceToMetricExpressionMap()).thenReturn(sourceToAggregateMap);
    return executionContext;
  }

  private Filter generateInFilter(String key, List<String> values) {
    return Filter.newBuilder()
        .setLhs(buildExpression(key))
        .setOperator(Operator.IN)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .addAllStringArray(values)
                                .setValueType(ValueType.STRING_ARRAY))))
        .build();
  }
}
