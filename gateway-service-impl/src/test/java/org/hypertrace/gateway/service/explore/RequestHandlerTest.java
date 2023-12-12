package org.hypertrace.gateway.service.explore;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.explore.entity.EntityServiceEntityFetcher;
import org.hypertrace.gateway.service.v1.common.AttributeExpression;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse.Builder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RequestHandlerTest {

  @Test
  public void orderByExpressionsWithFunction_shouldMatchCorrespondingSelections() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVGRATE)
                            .setAlias("RATE_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))
                            .addArguments(
                                Expression.newBuilder()
                                    .setLiteral(
                                        LiteralConstant.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setValueType(ValueType.LONG)
                                                    .setLong(30))))))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.ASC)
                    .setExpression(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration_different_alias")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName(
                                                        "Api.Trace.metrics.duration_millis"))))))
            .build();

    RequestHandler requestHandler =
        new RequestHandler(
            mock(QueryServiceClient.class),
            mock(AttributeMetadataProvider.class),
            mock(EntityIdColumnsConfigs.class),
            mock(QueryServiceEntityFetcher.class),
            mock(EntityServiceEntityFetcher.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    Assertions.assertEquals(1, orderByExpressions.size());
    // Should switch out the alias in the OrderBy expression
    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .build(),
        orderByExpressions.get(0));
  }

  @Test
  public void noChangeIfOrderByExpressionHasSameAliasAsSelection() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVGRATE)
                            .setAlias("RATE_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))
                            .addArguments(
                                Expression.newBuilder()
                                    .setLiteral(
                                        LiteralConstant.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setValueType(ValueType.LONG)
                                                    .setLong(30))))))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.ASC)
                    .setExpression(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName(
                                                        "Api.Trace.metrics.duration_millis"))))))
            .build();

    RequestHandler requestHandler =
        new RequestHandler(
            mock(QueryServiceClient.class),
            mock(AttributeMetadataProvider.class),
            mock(EntityIdColumnsConfigs.class),
            mock(QueryServiceEntityFetcher.class),
            mock(EntityServiceEntityFetcher.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    Assertions.assertEquals(1, orderByExpressions.size());
    // Should switch out the alias in the OrderBy expression
    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .build(),
        orderByExpressions.get(0));
  }

  @Test
  public void orderByExpressionsWithFunction_shouldMatchCorrespondingTimeAggregationExpression() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName(
                                                        "Api.Trace.metrics.duration_millis"))))))
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.MAX)
                                    .setAlias("MAX_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName(
                                                        "Api.Trace.metrics.duration_millis"))))))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.DESC)
                    .setExpression(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration_different_alias")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName(
                                                        "Api.Trace.metrics.duration_millis"))))))
            .build();

    RequestHandler requestHandler =
        new RequestHandler(
            mock(QueryServiceClient.class),
            mock(AttributeMetadataProvider.class),
            mock(EntityIdColumnsConfigs.class),
            mock(QueryServiceEntityFetcher.class),
            mock(EntityServiceEntityFetcher.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    Assertions.assertEquals(1, orderByExpressions.size());
    // Should switch out the alias in the OrderBy expression
    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.DESC)
            .setExpression(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .build(),
        orderByExpressions.get(0));
  }

  @Test
  public void noChangeIfOrderByExpressionIsAColumnAndOrderExpressionOrderIsMaintained() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Service.name")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("API.name")))
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.DESC)
                    .setExpression(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder().setColumnName("Service.name"))))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.ASC)
                    .setExpression(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration_different_alias")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName(
                                                        "Api.Trace.metrics.duration_millis"))))))
            .build();

    RequestHandler requestHandler =
        new RequestHandler(
            mock(QueryServiceClient.class),
            mock(AttributeMetadataProvider.class),
            mock(EntityIdColumnsConfigs.class),
            mock(QueryServiceEntityFetcher.class),
            mock(EntityServiceEntityFetcher.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    Assertions.assertEquals(2, orderByExpressions.size());
    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.DESC)
            .setExpression(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Service.name")))
            .build(),
        orderByExpressions.get(0));
    // Should switch out the alias in the OrderBy expression
    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .build(),
        orderByExpressions.get(1));
  }

  @Test
  public void testExploreQueryWithEDSFilter() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API")
            .setFilter(
                Filter.newBuilder()
                    .setOperator(Operator.AND)
                    .addChildFilter(
                        Filter.newBuilder()
                            .setLhs(
                                Expression.newBuilder()
                                    .setAttributeExpression(
                                        AttributeExpression.newBuilder()
                                            .setAttributeId("API.attributeId1")
                                            .build())
                                    .build())
                            .setOperator(Operator.EQ)
                            .setRhs(
                                Expression.newBuilder()
                                    .setLiteral(
                                        LiteralConstant.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setValueType(ValueType.STRING)
                                                    .setString("value")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .addChildFilter(
                        Filter.newBuilder()
                            .setLhs(
                                Expression.newBuilder()
                                    .setAttributeExpression(
                                        AttributeExpression.newBuilder()
                                            .setAttributeId("API.attributeId2")
                                            .build())
                                    .build())
                            .setOperator(Operator.EQ)
                            .setRhs(
                                Expression.newBuilder()
                                    .setLiteral(
                                        LiteralConstant.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setValueType(ValueType.STRING)
                                                    .setString("value")
                                                    .build())
                                            .build())
                                    .build())
                            .build())
                    .build())
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVGRATE)
                            .setAlias("RATE_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))
                            .addArguments(
                                Expression.newBuilder()
                                    .setLiteral(
                                        LiteralConstant.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setValueType(ValueType.LONG)
                                                    .setLong(30))))))
            .build();

    AttributeMetadataProvider attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    QueryServiceEntityFetcher queryServiceEntityFetcher = mock(QueryServiceEntityFetcher.class);
    EntityServiceEntityFetcher entityServiceEntityFetcher = mock(EntityServiceEntityFetcher.class);
    EntityIdColumnsConfigs entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);
    QueryServiceClient queryServiceClient = mock(QueryServiceClient.class);
    RequestHandler requestHandler =
        new RequestHandler(
            queryServiceClient,
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            queryServiceEntityFetcher,
            entityServiceEntityFetcher);
    ExploreRequestContext newExploreRequestContext =
        new ExploreRequestContext(RequestContext.forTenantId("tenantId"), exploreRequest);

    when(entityIdColumnsConfigs.getIdKey("API")).thenReturn(Optional.of("entityId"));
    when(attributeMetadataProvider.getAttributesMetadata(any(), any()))
        .thenReturn(
            Map.of(
                "API.attributeId1",
                    AttributeMetadata.newBuilder()
                        .addAllSources(List.of(AttributeSource.EDS, AttributeSource.QS))
                        .build(),
                "API.attributeId2",
                    AttributeMetadata.newBuilder().addSources(AttributeSource.EDS).build()));

    when(attributeMetadataProvider.getAttributeMetadata(any(), any(), eq("entityId")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("API.id").build()));

    when(attributeMetadataProvider.getAttributeMetadata(any(), any(), any()))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("API.timestampId").build()));

    when(queryServiceEntityFetcher.getEntities(any(), any()))
        .thenReturn(
            new EntityFetcherResponse(
                Map.of(
                    EntityKey.from("entityId1"),
                    Entity.newBuilder().setEntityType("API").setId("entityId1"),
                    EntityKey.from("entityId2"),
                    Entity.newBuilder().setEntityType("API").setId("entityId2"),
                    EntityKey.from("entityId3"),
                    Entity.newBuilder().setEntityType("API").setId("entityId3"))));

    when(entityServiceEntityFetcher.getResults(
            any(),
            eq(ExploreRequest.newBuilder().setFilter(exploreRequest.getFilter()).build()),
            eq(Set.of("entityId1", "entityId2", "entityId3"))))
        .thenReturn(
            List.of(
                Row.newBuilder()
                    .putColumns("entityId", Value.newBuilder().setString("entityId1").build())
                    .build(),
                Row.newBuilder()
                    .putColumns("entityId", Value.newBuilder().setString("entityId2").build())
                    .build()));

    when(queryServiceClient.executeQuery(any(), any()))
        .thenReturn(
            List.of(
                    ResultSetChunk.newBuilder()
                        .setResultSetMetadata(
                            ResultSetMetadata.newBuilder()
                                .addColumnMetadata(
                                    ColumnMetadata.newBuilder()
                                        .setColumnName("columnName1")
                                        .build())
                                .addColumnMetadata(
                                    ColumnMetadata.newBuilder()
                                        .setColumnName("columnName2")
                                        .build())
                                .build())
                        .addRow(
                            org.hypertrace.core.query.service.api.Row.newBuilder()
                                .addColumn(
                                    org.hypertrace.core.query.service.api.Value.newBuilder()
                                        .setString("value1")
                                        .build())
                                .addColumn(
                                    org.hypertrace.core.query.service.api.Value.newBuilder()
                                        .setString("value2")
                                        .build())
                                .build())
                        .addRow(
                            org.hypertrace.core.query.service.api.Row.newBuilder()
                                .addColumn(
                                    org.hypertrace.core.query.service.api.Value.newBuilder()
                                        .setString("value3")
                                        .build())
                                .addColumn(
                                    org.hypertrace.core.query.service.api.Value.newBuilder()
                                        .setString("value4")
                                        .build())
                                .build())
                        .build())
                .iterator());
    Builder responseBuilder =
        requestHandler.handleRequest(newExploreRequestContext, exploreRequest);
    Assertions.assertEquals(2, responseBuilder.getRowList().size());
  }
}
