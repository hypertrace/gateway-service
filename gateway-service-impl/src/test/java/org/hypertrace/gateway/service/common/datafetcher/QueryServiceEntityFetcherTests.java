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
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsColumnExpression;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsFilter;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsOrderBy;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsRequestFilter;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsStringLiteralExpression;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.hypertrace.gateway.service.v1.common.Operator.AND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils;
import org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.junit.jupiter.api.AfterEach;
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

  private QueryServiceClient queryServiceClient;
  private AttributeMetadataProvider attributeMetadataProvider;
  private QueryServiceEntityFetcher queryServiceEntityFetcher;

  @BeforeEach
  public void setup () {
    queryServiceClient = mock(QueryServiceClient.class);
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    mockAttributeMetadataProvider(AttributeScope.API.name());
    mockDomainObjectConfigs();
    queryServiceEntityFetcher = new QueryServiceEntityFetcher(queryServiceClient, 500,
        attributeMetadataProvider);
  }

  @AfterEach
  public void teardown() {
    DomainObjectConfigs.clearDomainObjectConfigs();
  }

  @Test
  public void test_getEntitiesAndAggregatedMetrics() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    long startTime = 0L;
    long endTime = 10L;
    int limit = 10;
    int offset = 0;
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
            .setFilter(
                Filter.newBuilder().setOperator(AND)
                .addChildFilter(EntitiesRequestAndResponseUtils.getTimeRangeFilter("API.startTime", startTime, endTime))
                .addChildFilter(generateEQFilter(API_DISCOVERY_STATE_ATTR, "DISCOVERED"))
            )
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext = new EntitiesRequestContext(
        tenantId,
        startTime,
        endTime,
        entityType.name(),
        requestHeaders);

    QueryRequest expectedQueryRequest = QueryRequest.newBuilder()
        .addSelection(createQsColumnExpression(API_ID_ATTR)) // Added implicitly in the getEntitiesAndAggregatedMetrics() in order to do GroupBy on the entity id
        // For some reason we do not add to aggregation in qs request. Also note that the aggregations are added before the rest of the
        // selections. No reason for this but that's how the code is ordered.
        .addSelection(createQsAggregationExpression("AVG", API_DURATION_ATTR, "AVG_API.duration"))
        .addSelection(createQsColumnExpression(API_NAME_ATTR))
        .setFilter(
            createQsRequestFilter(
                API_START_TIME_ATTR,
                API_ID_ATTR,
                startTime,
                endTime,
                createQsFilter(
                    createQsColumnExpression(API_DISCOVERY_STATE_ATTR),
                    Operator.EQ,
                    createQsStringLiteralExpression("DISCOVERED")
                )
            )
        )
        .addGroupBy(createQsColumnExpression(API_ID_ATTR))
        .addGroupBy(createQsColumnExpression(API_NAME_ATTR))
        .addOrderBy(createQsOrderBy(createQsColumnExpression(API_ID_ATTR), SortOrder.ASC))
        .setOffset(offset)
        // Though the limit on entities request is less, since there are multiple columns in the
        // groupBy, the limit will be set to the default from query service.
        .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
        .build();

    List<ResultSetChunk> resultSetChunks = List.of(
        getResultSetChunk(
            List.of(API_ID_ATTR, API_NAME_ATTR, "AVG_API.duration"),
            new String[][]{
                {"api-id-0", "api-0", "14.0"},
                {"api-id-1", "api-1", "15.0"},
                {"api-id-2", "api-2", "16.0"},
                {"api-id-3", "api-3", "17.0"}
            }
            )
    );

    Map<EntityKey, Builder> expectedEntityKeyBuilderResponseMap = Map.of(
        EntityKey.of("api-id-0"), Entity.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .putAttribute(API_NAME_ATTR, getStringValue("api-0"))
            .putAttribute(API_ID_ATTR, getStringValue("api-id-0"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 14.0)),
        EntityKey.of("api-id-1"), Entity.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .putAttribute(API_NAME_ATTR, getStringValue("api-1"))
            .putAttribute(API_ID_ATTR, getStringValue("api-id-1"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 15.0)),
        EntityKey.of("api-id-2"), Entity.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .putAttribute(API_NAME_ATTR, getStringValue("api-2"))
            .putAttribute(API_ID_ATTR, getStringValue("api-id-2"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 16.0)),
        EntityKey.of("api-id-3"), Entity.newBuilder()
            .setEntityType(AttributeScope.API.name())
            .putAttribute(API_NAME_ATTR, getStringValue("api-3"))
            .putAttribute(API_ID_ATTR, getStringValue("api-id-3"))
            .putMetric("AVG_API.duration", getAggregatedMetricValue(FunctionType.AVG, 17.0))
    );
    EntityFetcherResponse expectedEntityFetcherResponse = new EntityFetcherResponse(expectedEntityKeyBuilderResponseMap);

    try {
      System.out.println("Expected: " + JsonFormat.printer().omittingInsignificantWhitespace().print(expectedQueryRequest));
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), eq(requestHeaders), eq(500)))
        .thenReturn(resultSetChunks.iterator());

    compareEntityFetcherResponses(expectedEntityFetcherResponse,
        queryServiceEntityFetcher.getEntitiesAndAggregatedMetrics(entitiesRequestContext, entitiesRequest));
  }

  @Test
  public void test_getTotalEntitiesSingleEntityIdAttribute() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    long startTime = 0L;
    long endTime = 10L;
    int limit = 10;
    int offset = 0;
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
            .setFilter(
                Filter.newBuilder().setOperator(AND)
                    .addChildFilter(EntitiesRequestAndResponseUtils.getTimeRangeFilter("API.startTime", startTime, endTime))
                    .addChildFilter(generateEQFilter(API_DISCOVERY_STATE_ATTR, "DISCOVERED"))
            )
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext = new EntitiesRequestContext(
        tenantId,
        startTime,
        endTime,
        entityType.name(),
        requestHeaders);

    QueryRequest expectedQueryRequest = QueryRequest.newBuilder()
        .addSelection(createQsColumnExpression(API_ID_ATTR))
        .addSelection(createQsAggregationExpression("Count", API_ID_ATTR))
        .setFilter(
            createQsRequestFilter(
                API_START_TIME_ATTR,
                API_ID_ATTR,
                startTime,
                endTime,
                createQsFilter(
                    createQsColumnExpression(API_DISCOVERY_STATE_ATTR),
                    Operator.EQ,
                    createQsStringLiteralExpression("DISCOVERED")
                )
            )
        )
        .addGroupBy(createQsColumnExpression(API_ID_ATTR))
        .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
        .build();

    List<ResultSetChunk> resultSetChunks =
        List.of(getResultSetChunk(List.of("API.apiId"), new String[][]{ {"apiId1"}, {"apiId2"}}));

    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), eq(requestHeaders), eq(500)))
        .thenReturn(resultSetChunks.iterator());

    assertEquals(2, queryServiceEntityFetcher.getTotalEntities(entitiesRequestContext, entitiesRequest));
  }

  private void mockAttributeMetadataProvider(String attributeScope) {
    AttributeMetadata idAttributeMetadata = AttributeMetadata.newBuilder().setId(API_ID_ATTR).setKey("id").setScopeString(attributeScope)
        .setValueKind(AttributeKind.TYPE_STRING).build();
    AttributeMetadata startTimeAttributeMetadata = AttributeMetadata.newBuilder().setId(API_START_TIME_ATTR).setKey("startTime").setScopeString(attributeScope)
        .setValueKind(AttributeKind.TYPE_TIMESTAMP).build();
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
    when(attributeMetadataProvider.getAttributeMetadata(any(RequestContext.class), eq(attributeScope), eq("id"))).thenReturn(Optional.of(idAttributeMetadata));
    when(attributeMetadataProvider.getAttributeMetadata(any(RequestContext.class), eq(attributeScope), eq("startTime"))).thenReturn(Optional.of(startTimeAttributeMetadata));
  }

  private void mockDomainObjectConfigs() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = API\n"
            + "    key = id\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = id\n"
            + "      }"
            + "    ]\n"
            + "  }\n"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);
    DomainObjectConfigs.init(config);
  }
}
