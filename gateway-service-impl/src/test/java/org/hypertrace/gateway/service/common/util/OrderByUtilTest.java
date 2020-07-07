package org.hypertrace.gateway.service.common.util;

import java.util.List;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OrderByUtilTest {

  @Test
  public void testOrderByAliasReplacementFromSelection() {
    List<OrderByExpression> orderByExpressions =
        List.of(
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
                                                    "Api.Trace.metrics.duration_millis")))))
                .build());

    List<Expression> selectionList =
        List.of(
            Expression.newBuilder()
                .setFunction(
                    FunctionExpression.newBuilder()
                        .setFunction(FunctionType.AVG)
                        .setAlias("AVG_Duration")
                        .addArguments(
                            Expression.newBuilder()
                                .setColumnIdentifier(
                                    ColumnIdentifier.newBuilder()
                                        .setColumnName("Api.Trace.metrics.duration_millis"))))
                .build());

    List<TimeAggregation> timeAggregationList = List.of();

    List<OrderByExpression> newOrderByExpressions =
        OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
            orderByExpressions, selectionList, timeAggregationList);

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
        newOrderByExpressions.get(0));
  }

  @Test
  public void testOrderByNoColumnIdentifier_shouldRemainTheSame() {
    List<OrderByExpression> orderByExpressions =
        List.of(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.ASC)
                .setExpression(
                    Expression.newBuilder()
                        .setFunction(
                            FunctionExpression.newBuilder()
                                .setFunction(FunctionType.AVG)
                                .setAlias("AVG_Duration_different_alias")))
                .build());

    List<Expression> selectionList =
        List.of(
            Expression.newBuilder()
                .setFunction(
                    FunctionExpression.newBuilder()
                        .setFunction(FunctionType.AVG)
                        .setAlias("AVG_Duration")
                        .addArguments(
                            Expression.newBuilder()
                                .setColumnIdentifier(
                                    ColumnIdentifier.newBuilder()
                                        .setColumnName("Api.Trace.metrics.duration_millis"))))
                .build());

    List<TimeAggregation> timeAggregationList = List.of();

    List<OrderByExpression> newOrderByExpressions =
        OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
            orderByExpressions, selectionList, timeAggregationList);

    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration_different_alias")))
            .build(),
        newOrderByExpressions.get(0));
  }

  @Test
  public void testOrderByAliasReplacementFromTimeAggregation() {
    List<OrderByExpression> orderByExpressions =
        List.of(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.DESC)
                .setExpression(
                    Expression.newBuilder()
                        .setFunction(
                            FunctionExpression.newBuilder()
                                .setFunction(FunctionType.MAX)
                                .setAlias("MAX_Duration_different_alias")
                                .addArguments(
                                    Expression.newBuilder()
                                        .setColumnIdentifier(
                                            ColumnIdentifier.newBuilder()
                                                .setColumnName(
                                                    "Api.Trace.metrics.duration_millis")))))
                .build());

    List<Expression> selectionList = List.of();

    List<TimeAggregation> timeAggregationList =
        List.of(
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
                                                    "Api.Trace.metrics.duration_millis")))))
                .build());

    List<OrderByExpression> newOrderByExpressions =
        OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
            orderByExpressions, selectionList, timeAggregationList);

    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.DESC)
            .setExpression(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.MAX)
                            .setAlias("MAX_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("Api.Trace.metrics.duration_millis")))))
            .build(),
        newOrderByExpressions.get(0));
  }

  @Test
  public void testOrderByColumnNameExpression_shouldRemainTheSame() {
    List<OrderByExpression> orderByExpressions =
        List.of(
            OrderByExpression.newBuilder()
                .setOrder(SortOrder.DESC)
                .setExpression(
                    Expression.newBuilder()
                        .setColumnIdentifier(
                            ColumnIdentifier.newBuilder().setColumnName("Service.name")))
                .build());

    List<Expression> selectionList =
        List.of(
            Expression.newBuilder()
                .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Service.name"))
                .build());

    List<TimeAggregation> timeAggregationList = List.of();

    List<OrderByExpression> newOrderByExpressions =
        OrderByUtil.matchOrderByExpressionsAliasToSelectionAlias(
            orderByExpressions, selectionList, timeAggregationList);

    Assertions.assertEquals(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.DESC)
            .setExpression(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Service.name")))
            .build(),
        newOrderByExpressions.get(0));
  }
}
