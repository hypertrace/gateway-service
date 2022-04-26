package org.hypertrace.gateway.service.explore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.v1.common.AttributeExpression;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
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
            mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    assertEquals(1, orderByExpressions.size());
    // Should switch out the alias in the OrderBy expression
    assertEquals(
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
            mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    assertEquals(1, orderByExpressions.size());
    // Should switch out the alias in the OrderBy expression
    assertEquals(
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
            mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    assertEquals(1, orderByExpressions.size());
    // Should switch out the alias in the OrderBy expression
    assertEquals(
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
            mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    assertEquals(2, orderByExpressions.size());
    assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.DESC)
            .setExpression(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Service.name")))
            .build(),
        orderByExpressions.get(0));
    // Should switch out the alias in the OrderBy expression
    assertEquals(
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
  void shouldHandleMapAttributeExpressions() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("SERVICE")
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("SERVICE.name")))
            .addGroupBy(
                Expression.newBuilder()
                    .setAttributeExpression(
                        AttributeExpression.newBuilder()
                            .setAlias("SERVICE.attributes.url")
                            .setAttributeId("SERVICE.attributes")
                            .setSubpath("url")
                            .build())
                    .build())
            .build();

    ExploreRequestContext exploreRequestContext =
        new ExploreRequestContext("customer-1", exploreRequest, Collections.emptyMap());

    AttributeMetadataProvider attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    when(attributeMetadataProvider.getAttributeMetadata(
            exploreRequestContext, "SERVICE", "startTime"))
        .thenReturn(
            Optional.of(AttributeMetadata.newBuilder().setFqn("SERVICE.startTime").build()));
    when(attributeMetadataProvider.getAttributeMetadata(exploreRequestContext, "EVENT", "spaceIds"))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setFqn("EVENT.spaceIds").build()));
    when(attributeMetadataProvider.getAttributesMetadata(exploreRequestContext, "SERVICE"))
        .thenReturn(
            Map.of(
                "SERVICE.attributes",
                AttributeMetadata.newBuilder()
                    .setValueKind(AttributeKind.TYPE_STRING_MAP)
                    .build()));
    RequestHandler requestHandler =
        new RequestHandler(mock(QueryServiceClient.class), 500, attributeMetadataProvider);
    QueryRequest queryRequest =
        requestHandler.buildQueryRequest(
            exploreRequestContext, exploreRequest, attributeMetadataProvider);

    assertEquals(
        AttributeExpression.newBuilder()
            .setAlias("SERVICE.attributes.url")
            .setAttributeId("SERVICE.attributes")
            .setSubpath("url")
            .build(),
        exploreRequestContext.getMapAttributeExpressionByAlias("SERVICE.attributes.url"));

    org.hypertrace.core.query.service.api.Value queryServiceValue =
        org.hypertrace.core.query.service.api.Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("https://www.traceable.ai")
            .build();
    ColumnMetadata columnMetadata =
        ColumnMetadata.newBuilder().setColumnName("SERVICE.attributes.url").build();
    Row.Builder rowBuilder = Row.newBuilder();
    requestHandler.handleQueryServiceResponseSingleColumn(
        queryServiceValue,
        columnMetadata,
        rowBuilder,
        exploreRequestContext,
        attributeMetadataProvider);

    assertEquals(
        Row.newBuilder()
            .putColumns(
                "SERVICE.attributes.url",
                Value.newBuilder()
                    .setValueType(ValueType.STRING)
                    .setString("https://www.traceable.ai")
                    .build())
            .build(),
        rowBuilder.build());
  }
}
