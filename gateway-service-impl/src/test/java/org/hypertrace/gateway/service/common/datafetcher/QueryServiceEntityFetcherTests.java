package org.hypertrace.gateway.service.common.datafetcher;

import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildAggregateExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildOrderByExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildTimeAggregation;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.compareEntityFetcherResponses;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateEQFilter;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.getAggregatedMetricValue;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.getStringValue;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsAggregationExpression;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsRequestFilter;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createColumnExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringFilter;
import static org.hypertrace.gateway.service.v1.common.Operator.AND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.converters.QueryRequestUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryServiceEntityFetcherTests {
  private static final String API_ID_ATTR = "API.id";
  private static final String API_NAME_ATTR = "API.name";
  private static final String API_TYPE_ATTR = "API.type";
  private static final String API_PATTERN_ATTR = "API.urlPattern";
  private static final String API_START_TIME_ATTR = "API.startTime";
  private static final String API_END_TIME_ATTR = "API.endTime";
  private static final String API_NUM_CALLS_ATTR = "API.numCalls";
  private static final String API_DURATION_ATTR = "API.duration";
  private static final String API_DISCOVERY_STATE_ATTR = "API.apiDiscoveryState";
  private static final String SPACE_IDS_ATTR = "EVENT.spaceIds";

  private QueryServiceClient queryServiceClient;
  private AttributeMetadataProvider attributeMetadataProvider;
  private EntityIdColumnsConfigs entityIdColumnsConfigs;
  private QueryServiceEntityFetcher queryServiceEntityFetcher;

  @BeforeEach
  public void setup () {
    queryServiceClient = mock(QueryServiceClient.class);
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    mockAttributeMetadataProvider(AttributeScope.API.name());

    entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);
    when(entityIdColumnsConfigs.getIdKey("API")).thenReturn(Optional.of("id"));

    queryServiceEntityFetcher = new QueryServiceEntityFetcher(queryServiceClient, 500,
        attributeMetadataProvider, entityIdColumnsConfigs);
  }

  @Test
  public void testGetEntities() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    long startTime = 1L;
    long endTime = 10L;
    int limit = 10;
    int offset = 5;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addSelection(buildExpression(API_NAME_ATTR))
            .addSelection(
                buildAggregateExpression(
                    API_NUM_CALLS_ATTR, FunctionType.SUM, "Sum_numCalls", Collections.emptyList()))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(AND)
                    .addChildFilter(
                        EntitiesRequestAndResponseUtils.getTimeRangeFilter(
                            "API.startTime", startTime, endTime))
                    .addChildFilter(generateEQFilter(API_DISCOVERY_STATE_ATTR, "DISCOVERED")))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            tenantId, startTime, endTime, entityType.name(), "API.startTime", requestHeaders);

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createColumnExpression(API_ID_ATTR))
            .addSelection(createQsAggregationExpression("SUM", API_NUM_CALLS_ATTR, "Sum_numCalls"))
            .addSelection(createColumnExpression(API_NAME_ATTR))
            .setFilter(
                createQsRequestFilter(
                    API_START_TIME_ATTR,
                    API_ID_ATTR,
                    startTime,
                    endTime,
                    createStringFilter(API_DISCOVERY_STATE_ATTR, Operator.EQ, "DISCOVERED")))
            .addGroupBy(createColumnExpression(API_ID_ATTR))
            .addGroupBy(createColumnExpression(API_NAME_ATTR))
            .setOffset(offset)
            .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .addAllOrderBy(
                QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(orderByExpressions))
            .build();

    List<ResultSetChunk> resultSetChunks =
        List.of(
            getResultSetChunk(
                List.of("API.id", "API.name", "Sum_numCalls"),
                new String[][] {{"apiId1", "api 1", "3"}, {"apiId2", "api 2", "5"}}));

    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), eq(requestHeaders), eq(500)))
        .thenReturn(resultSetChunks.iterator());

    EntityFetcherResponse response =
        queryServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest);
    assertEquals(2, response.size());

    Map<EntityKey, Builder> expectedEntityKeyBuilderResponseMap = new LinkedHashMap<>();
    expectedEntityKeyBuilderResponseMap.put(EntityKey.of("apiId1"), Entity.newBuilder()
        .setId("apiId1")
        .setEntityType("API")
        .putAttribute("API.id", getStringValue("apiId1"))
        .putAttribute("API.name", getStringValue("api 1"))
        .putMetric("Sum_numCalls", getAggregatedMetricValue(FunctionType.SUM, 3))
    );
    expectedEntityKeyBuilderResponseMap.put(EntityKey.of("apiId2"), Entity.newBuilder()
        .setId("apiId2")
        .setEntityType("API")
        .putAttribute("API.id", getStringValue("apiId2"))
        .putAttribute("API.name", getStringValue("api 2"))
        .putMetric("Sum_numCalls", getAggregatedMetricValue(FunctionType.SUM, 5))
    );
    compareEntityFetcherResponses(
        new EntityFetcherResponse(expectedEntityKeyBuilderResponseMap), response);
  }

  @Test
  public void test_getEntitiesWithPagination() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    long startTime = 1L;
    long endTime = 10L;
    int limit = 10;
    int offset = 5;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addTimeAggregation(
                buildTimeAggregation(
                    30, API_NUM_CALLS_ATTR, FunctionType.SUM, "SUM_API.numCalls", List.of()))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(AND)
                    .addChildFilter(
                        EntitiesRequestAndResponseUtils.getTimeRangeFilter(
                            "API.startTime", startTime, endTime))
                    .addChildFilter(generateEQFilter(API_DISCOVERY_STATE_ATTR, "DISCOVERED")))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            tenantId, startTime, endTime, entityType.name(), "API.startTime", requestHeaders);

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createColumnExpression(API_ID_ATTR))
            .addSelection(createQsAggregationExpression("COUNT", API_ID_ATTR))
            .setFilter(
                createQsRequestFilter(
                    API_START_TIME_ATTR,
                    API_ID_ATTR,
                    startTime,
                    endTime,
                    createStringFilter(API_DISCOVERY_STATE_ATTR, Operator.EQ, "DISCOVERED")))
            .addGroupBy(createColumnExpression(API_ID_ATTR))
            .setOffset(offset)
            .setLimit(limit)
            .addAllOrderBy(
                QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(orderByExpressions))
            .build();

    List<ResultSetChunk> resultSetChunks =
        List.of(getResultSetChunk(List.of("API.apiId"), new String[][] {{"apiId1"}, {"apiId2"}}));

    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), eq(requestHeaders), eq(500)))
        .thenReturn(resultSetChunks.iterator());

    assertEquals(
        2, queryServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest).size());
  }

  @Test
  public void test_getEntitiesWithoutPagination() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    long startTime = 1L;
    long endTime = 10L;
    int limit = 0;
    int offset = 0;
    String tenantId = "TENANT_ID";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .addTimeAggregation(
                buildTimeAggregation(
                    30, API_NUM_CALLS_ATTR, FunctionType.SUM, "SUM_API.numCalls", List.of()))
            .setFilter(
                Filter.newBuilder()
                    .setOperator(AND)
                    .addChildFilter(
                        EntitiesRequestAndResponseUtils.getTimeRangeFilter(
                            "API.startTime", startTime, endTime))
                    .addChildFilter(generateEQFilter(API_DISCOVERY_STATE_ATTR, "DISCOVERED")))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            tenantId, startTime, endTime, entityType.name(), "API.startTime", requestHeaders);

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createColumnExpression(API_ID_ATTR))
            .addSelection(createQsAggregationExpression("COUNT", API_ID_ATTR))
            .setFilter(
                createQsRequestFilter(
                    API_START_TIME_ATTR,
                    API_ID_ATTR,
                    startTime,
                    endTime,
                    createStringFilter(API_DISCOVERY_STATE_ATTR, Operator.EQ, "DISCOVERED")))
            .addGroupBy(createColumnExpression(API_ID_ATTR))
            .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .addAllOrderBy(
                QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(orderByExpressions))
            .build();

    List<ResultSetChunk> resultSetChunks =
        List.of(getResultSetChunk(List.of("API.apiId"), new String[][] {{"apiId1"}, {"apiId2"}}));

    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), eq(requestHeaders), eq(500)))
        .thenReturn(resultSetChunks.iterator());

    assertEquals(
        2, queryServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest).size());
  }
  
  @Test
  public void test_getEntitiesBySpace() {
    long startTime = 1L;
    long endTime = 10L;
    int limit = 10;
    int offset = 0;
    String tenantId = "TENANT_ID";
    String space = "test-space";
    Map<String, String> requestHeaders = Map.of("x-tenant-id", tenantId);
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
                       .setEntityType(entityType.name())
                       .setStartTimeMillis(startTime)
                       .setEndTimeMillis(endTime)
                       .addSelection(buildExpression(API_NAME_ATTR))
                       .setSpaceId(space)
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

    QueryRequest expectedQueryRequest =
        QueryRequest.newBuilder()
            .addSelection(createColumnExpression(API_ID_ATTR))
            .addSelection(createColumnExpression(API_NAME_ATTR))
            .addSelection(
                QueryRequestUtil.createCountByColumnSelection("API.id"))
            .setFilter(
                createQsRequestFilter(
                    API_START_TIME_ATTR,
                    API_ID_ATTR,
                    startTime,
                    endTime,
                    createStringFilter(SPACE_IDS_ATTR, Operator.EQ, "test-space")))
            .addGroupBy(createColumnExpression(API_ID_ATTR))
            .addGroupBy(createColumnExpression(API_NAME_ATTR))
            .setOffset(offset)
            .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
            .build();

    List<ResultSetChunk> resultSetChunks = List.of(
        getResultSetChunk(
            List.of(API_ID_ATTR, API_NAME_ATTR),
            new String[][]{
                {"api-id-0", "api-0"}
            }
        )
    );

    Map<EntityKey, Builder> expectedEntityKeyBuilderResponseMap = Map.of(
        EntityKey.of("api-id-0"), Entity.newBuilder()
                                        .setEntityType(AttributeScope.API.name())
                                        .setId("api-id-0")
                                        .putAttribute(API_NAME_ATTR, getStringValue("api-0"))
                                        .putAttribute(API_ID_ATTR, getStringValue("api-id-0"))
    );

    EntityFetcherResponse expectedEntityFetcherResponse = new EntityFetcherResponse(expectedEntityKeyBuilderResponseMap);
    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), eq(requestHeaders), eq(500)))
        .thenReturn(resultSetChunks.iterator());

    compareEntityFetcherResponses(expectedEntityFetcherResponse,
        queryServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest));
  }

  private void mockAttributeMetadataProvider(String attributeScope) {
    AttributeMetadata idAttributeMetadata =
        AttributeMetadata.newBuilder()
            .setId(API_ID_ATTR)
            .setKey("id")
            .setScopeString(attributeScope)
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();
    AttributeMetadata startTimeAttributeMetadata =
        AttributeMetadata.newBuilder()
            .setId(API_START_TIME_ATTR)
            .setKey("startTime")
            .setScopeString(attributeScope)
            .setValueKind(AttributeKind.TYPE_TIMESTAMP)
            .build();
    AttributeMetadata spaceIdAttributeMetadata =
        AttributeMetadata.newBuilder()
            .setId(SPACE_IDS_ATTR)
            .setKey("spaceIds")
            .setScopeString(AttributeScope.EVENT.name())
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .build();
    when(attributeMetadataProvider.getAttributesMetadata(any(RequestContext.class), eq(attributeScope)))
        .thenReturn(Map.of(
            API_ID_ATTR, idAttributeMetadata,
            API_NAME_ATTR, AttributeMetadata.newBuilder().setId(API_NAME_ATTR).setKey("name").setScopeString(attributeScope)
                .setValueKind(AttributeKind.TYPE_STRING).build(),
            API_TYPE_ATTR, AttributeMetadata.newBuilder().setId(API_TYPE_ATTR).setKey("type").setScopeString(attributeScope)
                .setValueKind(AttributeKind.TYPE_STRING).build(),
            API_PATTERN_ATTR, AttributeMetadata.newBuilder().setId(API_PATTERN_ATTR).setKey("urlPattern").setScopeString(attributeScope)
                .setValueKind(AttributeKind.TYPE_STRING).build(),
            API_START_TIME_ATTR, startTimeAttributeMetadata,
            API_END_TIME_ATTR, AttributeMetadata.newBuilder().setId(API_END_TIME_ATTR).setKey("endTime").setScopeString(attributeScope)
                .setValueKind(AttributeKind.TYPE_TIMESTAMP).build(),
            API_NUM_CALLS_ATTR, AttributeMetadata.newBuilder().setId(API_NUM_CALLS_ATTR).setKey("numCalls").setScopeString(attributeScope)
                .setValueKind(AttributeKind.TYPE_INT64).build(),
            API_DURATION_ATTR, AttributeMetadata.newBuilder().setId(API_DURATION_ATTR).setKey("duration").setScopeString(attributeScope)
                .setValueKind(AttributeKind.TYPE_DOUBLE).build(),
            API_DISCOVERY_STATE_ATTR, AttributeMetadata.newBuilder().setId(API_DISCOVERY_STATE_ATTR).setKey("apiDiscoveryState").setScopeString(attributeScope)
                .setValueKind(AttributeKind.TYPE_STRING).build()
        ));
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(attributeScope), eq("id")))
        .thenReturn(Optional.of(idAttributeMetadata));
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(attributeScope), eq("startTime")))
        .thenReturn(Optional.of(startTimeAttributeMetadata));
    when(attributeMetadataProvider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.EVENT.name()), eq("spaceIds")))
        .thenReturn(Optional.of(spaceIdAttributeMetadata));
  }
}
