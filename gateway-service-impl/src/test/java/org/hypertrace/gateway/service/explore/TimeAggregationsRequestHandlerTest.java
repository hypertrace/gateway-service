package org.hypertrace.gateway.service.explore;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.Row;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ColumnName;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeAggregationsRequestHandlerTest {
  private final Row.Builder builder1 = createRowBuilder("adService", 3, 6.7);
  private final Row.Builder builder2 = createRowBuilder("badService", 3, 3.7);
  private final Row.Builder builder3 = createRowBuilder("zzzService", 3, 12.7);
  private final Row.Builder builder4 = createRowBuilder("restService", 7, 4.7);
  private final Row.Builder builder5 = createRowBuilder("timService", 1, 8.7);
  private final Row.Builder builder6 = createRowBuilder("godModeService", 1, 60.7);
  private final Row.Builder builder7 = createRowBuilder("bigService", 1, 896.7);
  private final Row.Builder builder8 = createRowBuilder("traceService", 6, 61.7);
  private final Row.Builder builder9 = createRowBuilder("blahService", 6, 3.7);
  private final Row.Builder builder10 = createRowBuilder("hellService", 6, 0.7);
  private final Row.Builder builder11 = createRowBuilder("bottleService", 7, 9.7);
  private final Row.Builder builder12 = createRowBuilder("listeningService", 10, 1.7);
  private final Row.Builder builder13 = createRowBuilder("wowService", 10, 6.7);
  private final Row.Builder builder14 = createRowBuilder("hahaService", 2, 90.7);

  @Test
  public void testSortAndPaginateRowBuilders() {
    List<Row.Builder> rowBuilders = createRowBuildersList();
    List<OrderByExpression> orderByExpressions = new ArrayList<>();
    orderByExpressions.add(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.ASC)
            .setExpression(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(ColumnName.INTERVAL_START_TIME.name())))
            .build());
    orderByExpressions.add(
        OrderByExpression.newBuilder()
            .setOrder(SortOrder.DESC)
            .setExpression(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .build());

    TimeAggregationsRequestHandler requestHandler =
        new TimeAggregationsRequestHandler(
            mock(QueryServiceClient.class), 500, mock(AttributeMetadataProvider.class));

    List<Row.Builder> sortedRows =
        requestHandler.sortAndPaginateRowBuilders(rowBuilders, orderByExpressions, 2, 0);

    Assertions.assertEquals(builder5, sortedRows.get(0));
    Assertions.assertEquals(builder6, sortedRows.get(1));
    Assertions.assertEquals(builder14, sortedRows.get(2));
    Assertions.assertEquals(builder3, sortedRows.get(3));
    Assertions.assertEquals(builder2, sortedRows.get(4));
    Assertions.assertEquals(builder8, sortedRows.get(5));
    Assertions.assertEquals(builder10, sortedRows.get(6));
    Assertions.assertEquals(builder4, sortedRows.get(7));
    Assertions.assertEquals(builder11, sortedRows.get(8));
    Assertions.assertEquals(builder13, sortedRows.get(9));
    Assertions.assertEquals(builder12, sortedRows.get(10));
  }

  private List<Row.Builder> createRowBuildersList() {
    List<Row.Builder> rowBuilders = new ArrayList<>();

    rowBuilders.add(builder1);
    rowBuilders.add(builder2);
    rowBuilders.add(builder3);
    rowBuilders.add(builder4);
    rowBuilders.add(builder5);
    rowBuilders.add(builder6);
    rowBuilders.add(builder7);
    rowBuilders.add(builder8);
    rowBuilders.add(builder9);
    rowBuilders.add(builder10);
    rowBuilders.add(builder11);
    rowBuilders.add(builder12);
    rowBuilders.add(builder13);
    rowBuilders.add(builder14);

    return rowBuilders;
  }

  private Row.Builder createRowBuilder(String serviceName, long intervalStartTime, double v1) {
    return Row.newBuilder()
        .putColumns(
            "serviceName",
            Value.newBuilder().setValueType(ValueType.STRING).setString(serviceName).build())
        .putColumns(
            ColumnName.INTERVAL_START_TIME.name(),
            Value.newBuilder().setValueType(ValueType.LONG).setLong(intervalStartTime).build())
        .putColumns("v1", Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(v1).build());
  }

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
}
