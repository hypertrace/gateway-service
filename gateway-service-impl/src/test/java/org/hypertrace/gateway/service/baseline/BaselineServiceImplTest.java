//package org.hypertrace.gateway.service.baseline;
//
//import org.apache.commons.lang3.RandomStringUtils;
//import org.hypertrace.gateway.service.entity.EntityService;
//import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
//import org.hypertrace.gateway.service.v1.common.Expression;
//import org.hypertrace.gateway.service.v1.common.FunctionExpression;
//import org.hypertrace.gateway.service.v1.common.FunctionType;
//import org.hypertrace.gateway.service.v1.common.HealthExpression;
//import org.hypertrace.gateway.service.v1.common.Interval;
//import org.hypertrace.gateway.service.v1.common.MetricSeries;
//import org.hypertrace.gateway.service.v1.common.Period;
//import org.hypertrace.gateway.service.v1.common.TimeAggregation;
//import org.hypertrace.gateway.service.v1.common.Value;
//import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
//import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
//import org.hypertrace.gateway.service.v1.entity.Entity;
//import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
//import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//import org.mockito.Mockito;
//import org.mockito.stubbing.Answer;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//public class BaselineServiceImplTest {
//
//  protected static final String TENANT_ID = "tenant1";
//  private final EntityService entityService = Mockito.mock(EntityService.class);
//
//  @Test
//  public void testBaselineForEntities() {
//    long endTime = System.currentTimeMillis();
//    long startTime = endTime - 1000 * 60 * 5;
//    BaselineEntitiesRequest baselineEntitiesRequest =
//        BaselineEntitiesRequest.newBuilder()
//            .setEntityType("API")
//            .setStartTimeMillis(startTime)
//            .setEndTimeMillis(endTime)
//            .addSelection(getExpressionFor("API.apiId", "API Id"))
//            .addSelection(getExpressionFor("API.apiName", "API Name"))
//            .addSelection(getFunctionExpressionFor(FunctionType.AVG, "API.duration", "duration"))
//            .addTimeAggregation(
//                getTimeAggregationFor(
//                    getFunctionExpressionFor(FunctionType.AVG, "API.duration", "duration_ts")))
//            .build();
//    EntitiesResponse response = getEntitiesResponse(baselineEntitiesRequest, "duration");
//    EntitiesResponse aggResponse = getEntitiesResponse(baselineEntitiesRequest, "duration_ts");
//    Mockito.when(
//            entityService.getEntities(
//                Mockito.anyString(), Mockito.any(EntitiesRequest.class), Mockito.anyMap()))
//            .thenAnswer((Answer) invocation -> {
//              if (((EntitiesRequest)invocation.getArgument(1)).getTimeAggregation(0)
//                      .getAggregation().getFunction().getAlias().equals("duration")) {
//                return response;
//              }
//              return aggResponse;
//            });
//    BaselineService baselineService = new BaselineServiceImpl(entityService, queryServiceClient, qsRequestTimeout);
//    BaselineEntitiesResponse baselineResponse =
//        baselineService.getBaselineForEntities(TENANT_ID, baselineEntitiesRequest, Map.of());
//    Assertions.assertTrue(baselineResponse.getBaselineEntityCount() > 0);
//    Assertions.assertTrue(baselineResponse.getBaselineEntityList().get(0).getBaselineCount() > 0);
//  }
//
//  private EntitiesResponse getEntitiesResponse(
//      BaselineEntitiesRequest baselineEntitiesRequest, String alias) {
//    List<Interval> intervals = new ArrayList<>();
//    for (int i = 0; i < 5; i++) {
//      Interval interval =
//          Interval.newBuilder()
//              .setValue(
//                  Value.newBuilder()
//                      .setDouble(Double.valueOf(RandomStringUtils.randomNumeric(2)))
//                      .build())
//              .setStartTimeMillis(System.currentTimeMillis() - i * 60000)
//              .setEndTimeMillis(System.currentTimeMillis())
//              .build();
//      intervals.add(interval);
//    }
//
//    MetricSeries metricSeries = MetricSeries.newBuilder().addAllValue(intervals).build();
//    Entity entity =
//        Entity.newBuilder()
//            .setEntityType("API")
//            .setId("a2nbc42srgdd4256")
//            .putMetricSeries(alias, metricSeries)
//            .build();
//    return EntitiesResponse.newBuilder().addEntity(entity).build();
//  }
//
//  private TimeAggregation getTimeAggregationFor(Expression expression) {
//    return TimeAggregation.newBuilder()
//        .setAggregation(expression)
//        .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60).build())
//        .build();
//  }
//
//  private Expression getExpressionFor(String columnName, String alias) {
//    return Expression.newBuilder()
//        .setColumnIdentifier(
//            ColumnIdentifier.newBuilder().setColumnName(columnName).setAlias(alias))
//        .build();
//  }
//
//  private Expression getFunctionExpressionFor(FunctionType type, String columnName, String alias) {
//    return Expression.newBuilder()
//        .setHealth(
//            HealthExpression.newBuilder()
//                .setFunction(
//                    FunctionExpression.newBuilder()
//                        .setFunction(type)
//                        .setAlias(alias)
//                        .addArguments(
//                            Expression.newBuilder()
//                                .setColumnIdentifier(
//                                    ColumnIdentifier.newBuilder()
//                                        .setColumnName(columnName)
//                                        .setAlias(alias))))
//                .build())
//        .build();
//  }
//}
