package org.hypertrace.gateway.service.explore;

import static org.mockito.Mockito.mock;

import java.util.List;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.explore.ColumnName;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeAggregationsRequestHandlerTest {

  @Test
  public void intervalStartTimeOrderByShouldBeAddedToOrderByListAndAliasShouldMatchSelections() {
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

    TimeAggregationsRequestHandler requestHandler =
        new TimeAggregationsRequestHandler(
            mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    Assertions.assertEquals(2, orderByExpressions.size());
    // Should add the interval start time order by as the first in the list
    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(ColumnName.INTERVAL_START_TIME.name())))
            .build(),
        orderByExpressions.get(0));
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
        orderByExpressions.get(1));
  }

  @Test
  public void intervalStartTimeOrderingNotAddedIfAlreadyRequested() {
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
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
                                                    .setColumnName("duration"))))))
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setOrder(SortOrder.DESC)
                    .setExpression(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(ColumnName.INTERVAL_START_TIME.name()))))
            .build();

    TimeAggregationsRequestHandler requestHandler =
        new TimeAggregationsRequestHandler(
            mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));
    List<OrderByExpression> orderByExpressions =
        requestHandler.getRequestOrderByExpressions(exploreRequest);

    // Should maintain the interval start time order as only order by
    Assertions.assertEquals(
        List.of(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.DESC)
                .setExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder()
                                .setColumnName(ColumnName.INTERVAL_START_TIME.name())))
                .build()),
        orderByExpressions);
  }
}
