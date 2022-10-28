package org.hypertrace.gateway.service.explore.entity;

import static org.hypertrace.core.grpcutils.context.RequestContext.forTenantId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.ExpressionContext;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.explore.ExploreRequestContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class EntityRequestHandlerTest {
  @Mock private AttributeMetadataProvider attributeMetadataProvider;
  @Mock private QueryServiceEntityFetcher queryServiceEntityFetcher;
  @Mock private EntityServiceEntityFetcher entityServiceEntityFetcher;

  private EntityRequestHandler entityRequestHandler;

  @BeforeEach
  void setup() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    queryServiceEntityFetcher = mock(QueryServiceEntityFetcher.class);
    entityServiceEntityFetcher = mock(EntityServiceEntityFetcher.class);

    this.entityRequestHandler =
        new EntityRequestHandler(
            attributeMetadataProvider,
            mock(QueryServiceClient.class),
            queryServiceEntityFetcher,
            entityServiceEntityFetcher);
  }

  @Test
  void shouldBuildEntityResponse_multipleDataSources() {
    Expression aggregation = createFunctionExpression("API.external");
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API")
            .setStartTimeMillis(123L)
            .setEndTimeMillis(234L)
            .setFilter(createEqFilter("API.name", "api1"))
            .addSelection(aggregation)
            .addGroupBy(createColumnExpression("API.type"))
            .build();
    ExploreRequestContext exploreRequestContext =
        new ExploreRequestContext(forTenantId("customer1"), exploreRequest);
    exploreRequestContext.mapAliasToFunctionExpression(
        "COUNT_API.external_[]", aggregation.getFunction());

    when(attributeMetadataProvider.getAttributeMetadata(exploreRequestContext, "API", "startTime"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setKey("API.startTime").build()));
    Map<String, AttributeMetadata> attributeMetadataMap =
        Map.of(
            "API.type",
            AttributeMetadata.newBuilder()
                .setKey("API.type")
                .setValueKind(AttributeKind.TYPE_STRING)
                .build(),
            "API.external",
            AttributeMetadata.newBuilder()
                .setKey("API.external")
                .setValueKind(AttributeKind.TYPE_STRING)
                .build());
    when(attributeMetadataProvider.getAttributesMetadata(exploreRequestContext, "API"))
        .thenReturn(attributeMetadataMap);

    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("API")
            .setStartTimeMillis(123L)
            .setEndTimeMillis(234L)
            .build();
    when(queryServiceEntityFetcher.getEntities(
            any(EntitiesRequestContext.class), eq(entitiesRequest)))
        .thenReturn(mockEntityFetcherResponse());
    when(entityServiceEntityFetcher.getResults(
            exploreRequestContext, exploreRequest, Set.of("api1", "api2")))
        .thenReturn(mockResults());
    ExpressionContext expressionContext =
        new ExpressionContext(
            attributeMetadataMap,
            entitiesRequest.getFilter(),
            entitiesRequest.getSelectionList(),
            entitiesRequest.getTimeAggregationList(),
            entitiesRequest.getOrderByList(),
            Collections.emptyList());
    ExploreResponse exploreResponse =
        entityRequestHandler.handleRequest(exploreRequestContext, expressionContext).build();
    assertEquals(2, exploreResponse.getRowCount());
    assertEquals(
        Map.of(
            "API.type",
            Value.newBuilder().setString("HTTP").setValueType(ValueType.STRING).build(),
            "COUNT_API.external_[]",
            Value.newBuilder().setLong(12).setValueType(ValueType.LONG).build()),
        exploreResponse.getRow(0).getColumnsMap());
    assertEquals(
        Map.of(
            "API.type",
            Value.newBuilder().setString("GRPC").setValueType(ValueType.STRING).build(),
            "COUNT_API.external_[]",
            Value.newBuilder().setLong(24).setValueType(ValueType.LONG).build()),
        exploreResponse.getRow(1).getColumnsMap());
  }

  @Test
  void testHandleRequest_emptyEntityIds() {
    Expression aggregation = createFunctionExpression("API.external");
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API")
            .setStartTimeMillis(123L)
            .setEndTimeMillis(234L)
            .setFilter(createEqFilter("API.name", "api1"))
            .addSelection(aggregation)
            .addGroupBy(createColumnExpression("API.type"))
            .build();
    ExploreRequestContext exploreRequestContext =
        new ExploreRequestContext(forTenantId("customer1"), exploreRequest);
    exploreRequestContext.mapAliasToFunctionExpression(
        "COUNT_API.external_[]", aggregation.getFunction());

    when(attributeMetadataProvider.getAttributeMetadata(exploreRequestContext, "API", "startTime"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setKey("API.startTime").build()));
    Map<String, AttributeMetadata> attributeMetadataMap =
        Map.of(
            "API.type",
            AttributeMetadata.newBuilder()
                .setKey("API.type")
                .setValueKind(AttributeKind.TYPE_STRING)
                .build(),
            "API.external",
            AttributeMetadata.newBuilder()
                .setKey("API.external")
                .setValueKind(AttributeKind.TYPE_STRING)
                .build());
    when(attributeMetadataProvider.getAttributesMetadata(exploreRequestContext, "API"))
        .thenReturn(attributeMetadataMap);

    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .setEntityType("API")
            .setStartTimeMillis(123L)
            .setEndTimeMillis(234L)
            .build();
    when(queryServiceEntityFetcher.getEntities(
            any(EntitiesRequestContext.class), eq(entitiesRequest)))
        .thenReturn(new EntityFetcherResponse());

    ExpressionContext expressionContext =
        new ExpressionContext(
            attributeMetadataMap,
            entitiesRequest.getFilter(),
            entitiesRequest.getSelectionList(),
            entitiesRequest.getTimeAggregationList(),
            entitiesRequest.getOrderByList(),
            Collections.emptyList());
    ExploreResponse exploreResponse =
        entityRequestHandler.handleRequest(exploreRequestContext, expressionContext).build();
    assertEquals(0, exploreResponse.getRowCount());
  }

  private EntityFetcherResponse mockEntityFetcherResponse() {
    return new EntityFetcherResponse(
        Map.of(
            EntityKey.of("api1"),
            Entity.newBuilder().setEntityType("API").setId("api1"),
            EntityKey.of("api2"),
            Entity.newBuilder().setEntityType("API").setId("api2")));
  }

  private Iterator<ResultSetChunk> mockResults() {
    ResultSetChunk chunk =
        ResultSetChunk.newBuilder()
            .setResultSetMetadata(
                ResultSetMetadata.newBuilder()
                    .addColumnMetadata(
                        ColumnMetadata.newBuilder().setColumnName("API.type").build())
                    .addColumnMetadata(
                        ColumnMetadata.newBuilder().setColumnName("COUNT_API.external_[]").build())
                    .build())
            .addRow(
                Row.newBuilder()
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                            .setString("HTTP")
                            .build())
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.LONG)
                            .setLong(12))
                    .build())
            .addRow(
                Row.newBuilder()
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                            .setString("GRPC")
                            .build())
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.LONG)
                            .setLong(24))
                    .build())
            .build();

    return List.of(chunk).iterator();
  }

  private Filter createEqFilter(String column, String value) {
    return Filter.newBuilder()
        .setLhs(createColumnExpression(column))
        .setOperator(Operator.EQ)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setString(value)
                                .setValueType(ValueType.STRING)
                                .build())
                        .build())
                .build())
        .build();
  }

  private Expression createColumnExpression(String column) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(column).build())
        .build();
  }

  private Expression createFunctionExpression(String column) {
    return Expression.newBuilder()
        .setFunction(
            FunctionExpression.newBuilder()
                .setFunction(FunctionType.COUNT)
                .setAlias("COUNT_" + column + "_[]")
                .addArguments(createColumnExpression(column))
                .build())
        .build();
  }
}
