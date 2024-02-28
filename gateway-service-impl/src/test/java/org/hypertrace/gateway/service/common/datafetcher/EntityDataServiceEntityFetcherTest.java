package org.hypertrace.gateway.service.common.datafetcher;

import static org.hypertrace.core.grpcutils.context.RequestContext.*;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildOrderByExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateEQFilter;
import static org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter.convertToEntityServiceExpression;
import static org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter.convertToEntityServiceFilter;
import static org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter.createColumnExpression;
import static org.hypertrace.gateway.service.v1.common.Operator.AND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.TotalEntitiesRequest;
import org.hypertrace.entity.query.service.v1.TotalEntitiesResponse;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfig;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class EntityDataServiceEntityFetcherTest {
  private static final String TENANT_ID = "tenant-id";
  private static final String API_ID_ATTR = "API.id";
  private static final String API_NAME_ATTR = "API.name";
  private static final String API_TYPE_ATTR = "API.type";

  private EntityQueryServiceClient entityQueryServiceClient;
  private AttributeMetadataProvider attributeMetadataProvider;
  private EntityDataServiceEntityFetcher entityDataServiceEntityFetcher;
  private EntityIdColumnsConfig entityIdColumnsConfig;

  @BeforeEach
  public void setup() {
    entityQueryServiceClient = mock(EntityQueryServiceClient.class);
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    mockAttributeMetadataProvider(AttributeScope.API.name());

    entityIdColumnsConfig = mock(EntityIdColumnsConfig.class);
    when(entityIdColumnsConfig.getIdKey("API")).thenReturn(Optional.of("id"));

    entityDataServiceEntityFetcher =
        new EntityDataServiceEntityFetcher(
            entityQueryServiceClient, attributeMetadataProvider, entityIdColumnsConfig);
  }

  @Test
  public void test_getEntities_WithPagination() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    long startTime = 1L;
    long endTime = 10L;
    int limit = 10;
    int offset = 5;
    String tenantId = "TENANT_ID";
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(AND)
                    .addChildFilter(generateEQFilter(API_TYPE_ATTR, "HTTP"))
                    .addChildFilter(generateEQFilter(API_NAME_ATTR, "DISCOVERED")))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");

    EntityQueryRequest expectedQueryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType("API")
            .addSelection(
                convertToEntityServiceExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(API_ID_ATTR))
                        .build()))
            .setFilter(convertToEntityServiceFilter(entitiesRequest.getFilter()))
            .setOffset(offset)
            .setLimit(limit)
            .addAllOrderBy(
                EntityServiceAndGatewayServiceConverter.convertToOrderByExpressions(
                    orderByExpressions))
            .build();

    List<ResultSetChunk> resultSetChunks =
        List.of(getResultSetChunk(List.of("API.apiId"), new String[][] {{"apiId1"}, {"apiId2"}}));

    when(entityQueryServiceClient.execute(
            eq(expectedQueryRequest), eq(entitiesRequestContext.getHeaders())))
        .thenReturn(resultSetChunks.iterator());

    assertEquals(
        2,
        entityDataServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest).size());
  }

  @Test
  public void test_getEntities_WithoutPagination() {
    List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
    long startTime = 1L;
    long endTime = 10L;
    int limit = 0;
    int offset = 0;
    String tenantId = "TENANT_ID";
    AttributeScope entityType = AttributeScope.API;
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType(entityType.name())
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(endTime)
            .setFilter(
                Filter.newBuilder()
                    .setOperator(AND)
                    .addChildFilter(generateEQFilter(API_TYPE_ATTR, "HTTP"))
                    .addChildFilter(generateEQFilter(API_NAME_ATTR, "DISCOVERED")))
            .addAllOrderBy(orderByExpressions)
            .setLimit(limit)
            .setOffset(offset)
            .build();
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");

    EntityQueryRequest expectedQueryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType("API")
            .addSelection(
                convertToEntityServiceExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName(API_ID_ATTR))
                        .build()))
            .setFilter(convertToEntityServiceFilter(entitiesRequest.getFilter()))
            .addAllOrderBy(
                EntityServiceAndGatewayServiceConverter.convertToOrderByExpressions(
                    orderByExpressions))
            .build();

    List<ResultSetChunk> resultSetChunks =
        List.of(getResultSetChunk(List.of("API.apiId"), new String[][] {{"apiId1"}, {"apiId2"}}));

    when(entityQueryServiceClient.execute(
            eq(expectedQueryRequest), eq(entitiesRequestContext.getHeaders())))
        .thenReturn(resultSetChunks.iterator());

    assertEquals(
        2,
        entityDataServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest).size());
  }

  @Test
  public void test_getTimeAggregatedMetrics() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          entityDataServiceEntityFetcher.getTimeAggregatedMetrics(
              new EntitiesRequestContext(forTenantId(TENANT_ID), 0, 1, "API", "API.startTime"),
              EntitiesRequest.newBuilder().build());
        });
  }

  @Nested
  class TotalEntities {
    @Test
    public void shouldReturnTotal() {
      List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
      long startTime = 1L;
      long endTime = 10L;
      int limit = 10;
      int offset = 5;
      String tenantId = "TENANT_ID";
      AttributeScope entityType = AttributeScope.API;
      EntitiesRequest entitiesRequest =
          EntitiesRequest.newBuilder()
              .setEntityType(entityType.name())
              .setStartTimeMillis(startTime)
              .setEndTimeMillis(endTime)
              .setFilter(
                  Filter.newBuilder()
                      .setOperator(AND)
                      .addChildFilter(generateEQFilter(API_TYPE_ATTR, "HTTP"))
                      .addChildFilter(generateEQFilter(API_NAME_ATTR, "DISCOVERED")))
              .addAllOrderBy(orderByExpressions)
              .setLimit(limit)
              .setOffset(offset)
              .build();
      EntitiesRequestContext entitiesRequestContext =
          new EntitiesRequestContext(
              forTenantId(tenantId), startTime, endTime, entityType.name(), "API.startTime");

      TotalEntitiesRequest expectedQueryRequest =
          TotalEntitiesRequest.newBuilder()
              .setEntityType("API")
              .setFilter(convertToEntityServiceFilter(entitiesRequest.getFilter()))
              .build();

      when(entityQueryServiceClient.total(
              eq(expectedQueryRequest), eq(entitiesRequestContext.getHeaders())))
          .thenReturn(TotalEntitiesResponse.newBuilder().setTotal(100L).build());

      assertEquals(
          100, entityDataServiceEntityFetcher.getTotal(entitiesRequestContext, entitiesRequest));
    }
  }

  @Nested
  class TimeBounds {
    @Test
    public void test_getEntities_WithLiveness() {
      List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
      long startTime = 1L;
      long endTime = 10L;
      int limit = 0;
      int offset = 0;
      RequestContext requestContext = forTenantId(TENANT_ID);
      AttributeScope entityType = AttributeScope.API;
      EntitiesRequest entitiesRequest =
          EntitiesRequest.newBuilder()
              .setEntityType(entityType.name())
              .setStartTimeMillis(startTime)
              .setEndTimeMillis(endTime)
              .setIncludeNonLiveEntities(false)
              .addAllOrderBy(orderByExpressions)
              .setLimit(limit)
              .setOffset(offset)
              .setFetchTotal(true)
              .build();
      EntitiesRequestContext entitiesRequestContext =
          new EntitiesRequestContext(
              requestContext, startTime, endTime, entityType.name(), "API.startTime");

      EntityQueryRequest expectedQueryRequest =
          EntityQueryRequest.newBuilder()
              .setEntityType("API")
              .addSelection(
                  convertToEntityServiceExpression(
                      Expression.newBuilder()
                          .setColumnIdentifier(
                              ColumnIdentifier.newBuilder().setColumnName(API_ID_ATTR))
                          .build()))
              .setFilter(buildTimeRangeFilter("API.startTime", startTime, endTime))
              .addAllOrderBy(
                  EntityServiceAndGatewayServiceConverter.convertToOrderByExpressions(
                      orderByExpressions))
              .build();
      TotalEntitiesRequest expectedTotalEntitiesRequest =
          TotalEntitiesRequest.newBuilder()
              .setEntityType("API")
              .setFilter(buildTimeRangeFilter("API.startTime", startTime, endTime))
              .build();

      when(entityQueryServiceClient.execute(any(), any())).thenReturn(Collections.emptyIterator());
      when(entityQueryServiceClient.total(any(), any()))
          .thenReturn(TotalEntitiesResponse.newBuilder().setTotal(200L).build());

      mockTimestamp("startTime");
      try (MockedStatic<TimestampConfigs> mockedTimestamps =
          Mockito.mockStatic(TimestampConfigs.class)) {
        mockedTimestamps
            .when(() -> TimestampConfigs.getTimestampColumn("API"))
            .thenReturn("startTime");
        entityDataServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest);
        verify(entityQueryServiceClient, times(1)).execute(eq(expectedQueryRequest), any());

        entityDataServiceEntityFetcher.getTotal(entitiesRequestContext, entitiesRequest);
        verify(entityQueryServiceClient, times(1)).total(eq(expectedTotalEntitiesRequest), any());
      }
    }

    private void mockTimestamp(String key) {
      AttributeMetadata timestampMetadata =
          AttributeMetadata.newBuilder()
              .setId("API." + key)
              .setKey(key)
              .setScopeString("API")
              .setValueKind(AttributeKind.TYPE_TIMESTAMP)
              .build();

      when(attributeMetadataProvider.getAttributeMetadata(
              any(EntitiesRequestContext.class), eq("API"), eq(key)))
          .thenReturn(Optional.of(timestampMetadata));
    }

    @Test
    public void test_getEntities_NonLive() {
      List<OrderByExpression> orderByExpressions = List.of(buildOrderByExpression(API_ID_ATTR));
      long startTime = 1L;
      long endTime = 10L;
      int limit = 0;
      int offset = 0;
      RequestContext requestContext = forTenantId(TENANT_ID);
      AttributeScope entityType = AttributeScope.API;
      EntitiesRequest entitiesRequest =
          EntitiesRequest.newBuilder()
              .setEntityType(entityType.name())
              .setStartTimeMillis(startTime)
              .setEndTimeMillis(endTime)
              .setIncludeNonLiveEntities(true)
              .addAllOrderBy(orderByExpressions)
              .setLimit(limit)
              .setOffset(offset)
              .setFetchTotal(true)
              .build();
      EntitiesRequestContext entitiesRequestContext =
          new EntitiesRequestContext(
              requestContext, startTime, endTime, entityType.name(), "API.startTime");

      EntityQueryRequest expectedQueryRequest =
          EntityQueryRequest.newBuilder()
              .setEntityType("API")
              .addSelection(
                  convertToEntityServiceExpression(
                      Expression.newBuilder()
                          .setColumnIdentifier(
                              ColumnIdentifier.newBuilder().setColumnName(API_ID_ATTR))
                          .build()))
              .setFilter(org.hypertrace.entity.query.service.v1.Filter.getDefaultInstance())
              .addAllOrderBy(
                  EntityServiceAndGatewayServiceConverter.convertToOrderByExpressions(
                      orderByExpressions))
              .build();

      TotalEntitiesRequest expectedTotalEntitiesRequest =
          TotalEntitiesRequest.newBuilder()
              .setEntityType("API")
              .setFilter(org.hypertrace.entity.query.service.v1.Filter.getDefaultInstance())
              .build();

      when(entityQueryServiceClient.execute(any(), any())).thenReturn(Collections.emptyIterator());
      when(entityQueryServiceClient.total(any(), any()))
          .thenReturn(TotalEntitiesResponse.newBuilder().setTotal(200L).build());

      entityDataServiceEntityFetcher.getEntities(entitiesRequestContext, entitiesRequest);
      verify(entityQueryServiceClient, times(1)).execute(eq(expectedQueryRequest), any());

      entityDataServiceEntityFetcher.getTotal(entitiesRequestContext, entitiesRequest);
      verify(entityQueryServiceClient, times(1)).total(eq(expectedTotalEntitiesRequest), any());
    }

    private org.hypertrace.entity.query.service.v1.Filter buildTimeRangeFilter(
        String id, long start, long end) {
      org.hypertrace.entity.query.service.v1.Expression.Builder startTimeConstant =
          org.hypertrace.entity.query.service.v1.Expression.newBuilder()
              .setLiteral(
                  LiteralConstant.newBuilder()
                      .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(start)));

      org.hypertrace.entity.query.service.v1.Expression.Builder endTimeConstant =
          org.hypertrace.entity.query.service.v1.Expression.newBuilder()
              .setLiteral(
                  LiteralConstant.newBuilder()
                      .setValue(Value.newBuilder().setValueType(ValueType.LONG).setLong(end)));

      org.hypertrace.entity.query.service.v1.Filter.Builder startTimeFilterBuilder =
          org.hypertrace.entity.query.service.v1.Filter.newBuilder();
      startTimeFilterBuilder.setOperator(Operator.GE);
      startTimeFilterBuilder.setLhs(createColumnExpression(id));
      startTimeFilterBuilder.setRhs(startTimeConstant);

      org.hypertrace.entity.query.service.v1.Filter.Builder endTimeFilterBuilder =
          org.hypertrace.entity.query.service.v1.Filter.newBuilder();
      endTimeFilterBuilder.setOperator(Operator.LT);
      endTimeFilterBuilder.setLhs(createColumnExpression(id));
      endTimeFilterBuilder.setRhs(endTimeConstant);

      return org.hypertrace.entity.query.service.v1.Filter.newBuilder()
          .setOperator(Operator.AND)
          .addChildFilter(startTimeFilterBuilder)
          .addChildFilter(endTimeFilterBuilder)
          .build();
    }
  }

  private void mockAttributeMetadataProvider(String attributeScope) {
    AttributeMetadata idAttributeMetadata =
        AttributeMetadata.newBuilder()
            .setId(API_ID_ATTR)
            .setKey("id")
            .setScopeString(attributeScope)
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();
    when(attributeMetadataProvider.getAttributesMetadata(
            any(EntitiesRequestContext.class), eq(attributeScope)))
        .thenReturn(
            Map.of(
                API_ID_ATTR, idAttributeMetadata,
                API_NAME_ATTR,
                    AttributeMetadata.newBuilder()
                        .setId(API_NAME_ATTR)
                        .setKey("name")
                        .setScopeString(attributeScope)
                        .setValueKind(AttributeKind.TYPE_STRING)
                        .build(),
                API_TYPE_ATTR,
                    AttributeMetadata.newBuilder()
                        .setId(API_TYPE_ATTR)
                        .setKey("type")
                        .setScopeString(attributeScope)
                        .setValueKind(AttributeKind.TYPE_STRING)
                        .build()));
    when(attributeMetadataProvider.getAttributeMetadata(
            any(EntitiesRequestContext.class), eq(attributeScope), eq("id")))
        .thenReturn(Optional.of(idAttributeMetadata));
  }

  private ResultSetChunk getResultSetChunk(List<String> columnNames, String[][] resultsTable) {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();

    // ColumnMetadata from the keyset
    List<ColumnMetadata> columnMetadataBuilders =
        columnNames.stream()
            .map(
                (columnName) ->
                    ColumnMetadata.newBuilder()
                        .setColumnName(columnName)
                        .setValueType(ValueType.STRING)
                        .build())
            .collect(Collectors.toList());
    resultSetChunkBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder().addAllColumnMetadata(columnMetadataBuilders));

    // Add the rows.
    for (int i = 0; i < resultsTable.length; i++) {
      Row.Builder rowBuilder = Row.newBuilder();
      for (int j = 0; j < resultsTable[i].length; j++) {
        rowBuilder.addColumn(
            Value.newBuilder().setString(resultsTable[i][j]).setValueType(ValueType.STRING));
      }
      resultSetChunkBuilder.addRow(rowBuilder);
    }

    return resultSetChunkBuilder.build();
  }
}
