package org.hypertrace.gateway.service.entity;

import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsAggregationExpression;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsColumnExpression;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.createQsDefaultRequestFilter;
import static org.hypertrace.gateway.service.common.QueryServiceRequestAndResponseUtils.getResultSetChunk;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.gateway.service.AbstractGatewayServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.entity.config.LogConfig;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class EntityServiceTest extends AbstractGatewayServiceTest {

  private QueryServiceClient queryServiceClient;
  private EntityQueryServiceClient entityQueryServiceClient;
  private AttributeMetadataProvider attributeMetadataProvider;
  private LogConfig logConfig;

  @BeforeEach
  public void setup() {
    super.setup();
    mockDomainObjectConfigs();
    queryServiceClient = Mockito.mock(QueryServiceClient.class);
    entityQueryServiceClient = Mockito.mock(EntityQueryServiceClient.class);
    attributeMetadataProvider = Mockito.mock(AttributeMetadataProvider.class);
    mock(attributeMetadataProvider);
    logConfig = Mockito.mock(LogConfig.class);
    when(logConfig.getQueryThresholdInMillis()).thenReturn(1500L);
  }

  private void mockDomainObjectConfigs() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = API\n"
            + "    key = apiId\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = apiId\n"
            + "      }"
            + "    ]\n"
            + "  }\n"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);
    DomainObjectConfigs.init(config);
  }

  private void mock(AttributeMetadataProvider attributeMetadataProvider) {
    when(
            attributeMetadataProvider.getAttributesMetadata(
                any(RequestContext.class), eq(AttributeScope.API.name())))
        .thenReturn(
            Map.of(
                "API.startTime",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("startTime")
                    .setFqn("API.startTime")
                    .setValueKind(AttributeKind.TYPE_INT64)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("API.startTime")
                    .build(),
                "API.apiId",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("apiId")
                    .setFqn("API.apiId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS).addSources(AttributeSource.EDS)
                    .setId("API.apiId")
                    .build(),
                "API.apiName",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("apiName")
                    .setFqn("API.name")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS).addSources(AttributeSource.EDS)
                    .setId("API.name")
                    .build(),
                "API.httpMethod",
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("httpMethod")
                    .setFqn("API.http.method")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.EDS)
                    .setId("API.httpMethod")
                    .build()));

    when(
            attributeMetadataProvider.getAttributeMetadata(
                any(RequestContext.class), eq(AttributeScope.API.name()), eq("apiId")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("apiId")
                    .setFqn("API.apiId")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setId("API.apiId")
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS).addSources(AttributeSource.EDS)
                    .build()));
    when(
            attributeMetadataProvider.getAttributeMetadata(
                any(RequestContext.class), eq(AttributeScope.API.name()), eq("startTime")))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API.name())
                    .setKey("startTime")
                    .setFqn("API.start_time_millis")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .setType(AttributeType.ATTRIBUTE)
                    .addSources(AttributeSource.QS)
                    .setId("API.startTime")
                    .build()));
  }

  @Test
  public void testGetEntitiesOnlySelectFromSingleSourceWithTimeRangeShouldUseQueryService() {
    long endTime = System.currentTimeMillis();
    long startTime = endTime - 1000;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("API")
            .setStartTimeMillis(startTime).setEndTimeMillis(endTime)
            .addSelection(getExpressionFor("API.apiId", "API Id"))
            .addSelection(getExpressionFor("API.apiName", "API Name"))
            .setLimit(2)
            .build();

    // The filter sent down to query-service by QueryServiceEntityFetcher when there is no filter in
    // EntitiesRequest
    Filter queryServiceFilter = createQsDefaultRequestFilter("API.startTime",
        "API.apiId", startTime, endTime);

    QueryRequest expectedQueryRequest = QueryRequest.newBuilder()
        .addSelection(createQsColumnExpression("API.apiId")) // Added implicitly in the getEntitiesAndAggregatedMetrics() in order to do GroupBy on the entity id
        .addSelection(createQsColumnExpression("API.apiName", "API Name"))
        // QueryServiceEntityFetcher adds Count(entityId) to the request for one that does not have an aggregation.
        // This is because internally a GroupBy request is created out of the entities request and
        // an aggregation is needed.
        .addSelection(createQsAggregationExpression("Count", "API.apiId"))
        .setFilter(queryServiceFilter)
        .addGroupBy(createQsColumnExpression("API.apiId"))
        .addGroupBy(createQsColumnExpression("API.apiName", "API Name"))
        .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
        .build();
    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), any(), Mockito.anyInt()))
        .thenReturn(
            List.of(
                getResultSetChunk(
                    List.of("API.apiId", "API.apiName"),
                    new String[][]{
                        {"apiId1", "/login",},
                        {"apiId2", "/checkout"}
                    }
                )
            ).iterator());

    // get total request.
    expectedQueryRequest = QueryRequest.newBuilder()
        .addSelection(createQsColumnExpression("API.apiId")) // Added implicitly in the getEntitiesAndAggregatedMetrics() in order to do GroupBy on the entity id
        // QueryServiceEntityFetcher adds Count(entityId) to the request for one that does not have an aggregation.
        // This is because internally a GroupBy request is created out of the entities request and
        // an aggregation is needed.
        .addSelection(createQsAggregationExpression("Count", "API.apiId"))
        .setFilter(queryServiceFilter)
        .addGroupBy(createQsColumnExpression("API.apiId"))
        .setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT)
        .build();

    when(queryServiceClient.executeQuery(eq(expectedQueryRequest), any(), Mockito.anyInt()))
        .thenReturn(List.of(
            getResultSetChunk(List.of("API.apiId"), new String[][]{ {"apiId1"}, {"apiId2"}})).iterator());

    ScopeFilterConfigs scopeFilterConfigs = new ScopeFilterConfigs(ConfigFactory.empty());
    EntityService entityService = new EntityService(queryServiceClient, 500,
        entityQueryServiceClient, attributeMetadataProvider, scopeFilterConfigs, logConfig);
    EntitiesResponse response = entityService.getEntities(TENANT_ID, entitiesRequest, Map.of());
    Assertions.assertNotNull(response);
    Assertions.assertEquals(2, response.getTotal());
    Entity entity1 = response.getEntity(0);
    Assertions.assertEquals("apiId1", entity1.getAttributeMap().get("API.apiId").getString());
    Assertions.assertEquals("/login", entity1.getAttributeMap().get("API.apiName").getString());
    Entity entity2 = response.getEntity(1);
    Assertions.assertEquals("apiId2", entity2.getAttributeMap().get("API.apiId").getString());
    Assertions.assertEquals("/checkout", entity2.getAttributeMap().get("API.apiName").getString());
  }

  @Test
  public void testGetEntitiesOnlySelectFromMultipleSources() {
    when(queryServiceClient.executeQuery(any(), any(), Mockito.anyInt()))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateResultSetMetadataFor("API.apiId", "API.apiName"))
                        .addRow(generateRowFor("apiId1", "/login"))
                        .addRow(generateRowFor("apiId2", "/checkout"))
                        .build())
                .iterator());
    when(entityQueryServiceClient.execute(any(), any()))
        .thenReturn(
            List.of(
                    org.hypertrace.entity.query.service.v1.ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            generateEntityServiceResultSetMetadataFor(
                                "API.apiId", "API.httpMethod"))
                        .addRow(generateEntityServiceRowFor("apiId1", "GET"))
                        .addRow(generateEntityServiceRowFor("apiId2", "POST"))
                        .build())
                .iterator());
    ScopeFilterConfigs scopeFilterConfigs = new ScopeFilterConfigs(ConfigFactory.empty());
    EntityService entityService = new EntityService(queryServiceClient, 500,
        entityQueryServiceClient, attributeMetadataProvider, scopeFilterConfigs, logConfig);
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("API")
            .setStartTimeMillis(System.currentTimeMillis() - 1000)
            .setEndTimeMillis(System.currentTimeMillis())
            .setFilter(org.hypertrace.gateway.service.v1.common.Filter.newBuilder()
                .setOperator(Operator.IN)
                .setLhs(getExpressionFor("API.httpMethod", "API Http method"))
                .setRhs(getStringListLiteral(List.of("GET", "POST")))
            )
            .addSelection(getExpressionFor("API.apiId", "API Id"))
            .addSelection(getExpressionFor("API.apiName", "API Name"))
            .addSelection(getExpressionFor("API.httpMethod", "API Http method"))
            .build();
    EntitiesResponse response = entityService.getEntities(TENANT_ID, entitiesRequest, Map.of());
    Assertions.assertNotNull(response);
    Assertions.assertEquals(2, response.getTotal());
    for (Entity entity: response.getEntityList()) {
      if ("apiId1".equals(entity.getAttributeMap().get("API.apiId").getString())) {
        Assertions.assertEquals("/login", entity.getAttributeMap().get("API.apiName").getString());
        Assertions.assertEquals("GET", entity.getAttributeMap().get("API.httpMethod").getString());
      } else if ("apiId2".equals(entity.getAttributeMap().get("API.apiId").getString())) {
        Assertions.assertEquals("/checkout", entity.getAttributeMap().get("API.apiName").getString());
        Assertions.assertEquals("POST", entity.getAttributeMap().get("API.httpMethod").getString());
      }
    }
  }

  private Expression getStringListLiteral(List<String> values) {
    return Expression.newBuilder().setLiteral(
        LiteralConstant.newBuilder().setValue(
            org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                .setValueType(org.hypertrace.gateway.service.v1.common.ValueType.STRING_ARRAY)
                .addAllStringArray(values))).build();
  }

  private Expression getExpressionFor(String columnName, String alias) {
    return Expression.newBuilder()
        .setColumnIdentifier(
            ColumnIdentifier.newBuilder().setColumnName(columnName).setAlias(alias))
        .build();
  }

  private Row generateRowFor(String... columnValues) {
    Row.Builder rowBuilder = Row.newBuilder();
    Arrays.stream(columnValues)
        .forEach(
            columnValue ->
                rowBuilder.addColumn(
                    Value.newBuilder().setValueType(ValueType.STRING).setString(columnValue)));
    return rowBuilder.build();
  }

  private org.hypertrace.entity.query.service.v1.Row generateEntityServiceRowFor(
      String... columnValues) {
    org.hypertrace.entity.query.service.v1.Row.Builder rowBuilder =
        org.hypertrace.entity.query.service.v1.Row.newBuilder();
    Arrays.stream(columnValues)
        .forEach(
            columnValue ->
                rowBuilder.addColumn(
                    org.hypertrace.entity.query.service.v1.Value.newBuilder()
                        .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                        .setString(columnValue)));
    return rowBuilder.build();
  }

  private ResultSetMetadata generateResultSetMetadataFor(String... columnNames) {
    ResultSetMetadata.Builder builder = ResultSetMetadata.newBuilder();
    Arrays.stream(columnNames)
        .forEach(
            columnName ->
                builder.addColumnMetadata(
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(ValueType.STRING)
                        .build()));
    return builder.build();
  }

  private org.hypertrace.entity.query.service.v1.ResultSetMetadata
      generateEntityServiceResultSetMetadataFor(String... columnNames) {
    org.hypertrace.entity.query.service.v1.ResultSetMetadata.Builder builder =
        org.hypertrace.entity.query.service.v1.ResultSetMetadata.newBuilder();
    Arrays.stream(columnNames)
        .forEach(
            columnName ->
                builder.addColumnMetadata(
                    org.hypertrace.entity.query.service.v1.ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                        .build()));
    return builder.build();
  }
}
