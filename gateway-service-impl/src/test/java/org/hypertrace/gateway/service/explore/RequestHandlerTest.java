package org.hypertrace.gateway.service.explore;

import static org.mockito.Mockito.mock;

import java.util.List;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
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
        new RequestHandler(mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
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
        new RequestHandler(mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
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
        new RequestHandler(mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
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
        new RequestHandler(mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
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
}
