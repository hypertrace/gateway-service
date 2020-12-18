package org.hypertrace.gateway.service.baseline;

import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.hypertrace.gateway.service.entity.EntityService;
import org.hypertrace.gateway.service.v1.common.Baseline;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.HealthExpression;
import org.hypertrace.gateway.service.v1.common.Interval;
import org.hypertrace.gateway.service.v1.common.MetricSeries;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntity;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** This service calculates baseline values for an Entity across time range.
 * This converts Functions in selection expression to time series data and generate baseline, lowerbound and upperbound.
 * It also changes Time aggregation requests to get more data points for proper baseline calculation 
 **/
public class BaselineServiceImpl implements BaselineService {
  private static long DAY_IN_MILLIS = 86400000L;
  private static long SIX_HOURS_IN_MILLIS = 21600000L;
  private static final Logger LOG = LoggerFactory.getLogger(BaselineServiceImpl.class);
  private final EntityService entityService;

  public BaselineServiceImpl(EntityService entityService) {
    this.entityService = entityService;
  }

  public BaselineEntitiesResponse getBaselineForEntities(
      String tenantId,
      BaselineEntitiesRequest originalRequest,
      Map<String, String> requestHeaders) {
    EntitiesRequest selectionEntityRequest = getEntityRequestForSelectionExpr(originalRequest);
    EntitiesRequest timeAggregationRequest = getEntityRequestForTimeAggregations(originalRequest);
    EntitiesResponse entitiesResponse =
        entityService.getEntities(tenantId, selectionEntityRequest, requestHeaders);
    EntitiesResponse timeAggResponse =
        entityService.getEntities(tenantId, timeAggregationRequest, requestHeaders);
    List<Entity> mergedEntityList = new ArrayList<>();
    mergedEntityList.addAll(entitiesResponse.getEntityList());
    mergedEntityList.addAll(timeAggResponse.getEntityList());
    BaselineEntitiesResponse BaselineEntitiesResponse =
        convertEntitiesToBaselineEntities(mergedEntityList);
    return BaselineEntitiesResponse;
  }

  private BaselineEntitiesResponse convertEntitiesToBaselineEntities(List<Entity> entityList) {
    List<BaselineEntity> baselineEntityList = new ArrayList<>();
    for (Entity entity : entityList) {
      BaselineEntity.Builder baselineEntityBuilder = BaselineEntity.newBuilder();
      baselineEntityBuilder.setId(entity.getId());
      Map<String, MetricSeries> entitySeriesMap = entity.getMetricSeriesMap();
      for (Map.Entry<String, MetricSeries> entry : entitySeriesMap.entrySet()) {
        MetricSeries metricSeries = entry.getValue();
        List<Interval> values = metricSeries.getValueList();
        List<Double> metricValues =
            values.stream().map(x -> x.getValue().getDouble()).collect(Collectors.toList());
        double[] metricValueArray =
            metricValues.stream().mapToDouble(Double::doubleValue).toArray();
        Baseline baseline = getBaseline(metricValueArray);
        baselineEntityBuilder.putBaseline(entry.getKey(), baseline);
      }
      baselineEntityList.add(baselineEntityBuilder.build());
    }
    return BaselineEntitiesResponse.newBuilder().addAllBaselineEntity(baselineEntityList).build();
  }

  private Baseline getBaseline(double[] metricValueArray) {
    StandardDeviation standardDeviation = new StandardDeviation();
    Mean mean = new Mean();
    double meanValue = mean.evaluate(metricValueArray);
    double sd = standardDeviation.evaluate(metricValueArray);
    double lowerBound = meanValue - (2 * sd);
    if (lowerBound < 0) {
      lowerBound = 0;
    }
    double upperBound = meanValue + (2 * sd);
    Baseline baseline =
        Baseline.newBuilder()
            .setLowerBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(lowerBound).build())
            .setUpperBound(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(upperBound).build())
            .setValue(
                Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(meanValue).build())
            .build();
    return baseline;
  }

  private EntitiesRequest getEntityRequestForTimeAggregations(
      BaselineEntitiesRequest originalRequest) {
    long seriesStartTime =
        getUpdatedStartTimeForSeriesRequests(
            originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());
    EntitiesRequest.Builder seriesEntitiesReqBuilder =
        EntitiesRequest.newBuilder()
            .setStartTimeMillis(seriesStartTime)
            .setEndTimeMillis(originalRequest.getEndTimeMillis())
            .setFilter(originalRequest.getFilter())
            .setEntityType(originalRequest.getEntityType());
    List<TimeAggregation> timeAggregations = originalRequest.getTimeAggregationList();
    for (TimeAggregation timeAggregation : timeAggregations) {
      if (timeAggregation.getAggregation().hasHealth()) {
        FunctionExpression function = timeAggregation.getAggregation().getHealth().getFunction();
        Expression aggregation = Expression.newBuilder().setFunction(function).build();
        TimeAggregation newAggregation =
            TimeAggregation.newBuilder()
                .setAggregation(aggregation)
                .setPeriod(timeAggregation.getPeriod())
                .build();
        seriesEntitiesReqBuilder.addTimeAggregation(newAggregation);
      }
    }
    List<Expression> selections = originalRequest.getSelectionList();
    for (Expression selection : selections) {
      if (!selection.hasHealth()) {
        seriesEntitiesReqBuilder.addSelection(selection);
      }
    }
    return seriesEntitiesReqBuilder.build();
  }

  private EntitiesRequest getEntityRequestForSelectionExpr(
      BaselineEntitiesRequest originalRequest) {
    List<Expression> selections = originalRequest.getSelectionList();
    long startTime =
        getUpdatedStartTimeForAggregations(
            originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());
    EntitiesRequest.Builder entitiesReqBuilder =
        EntitiesRequest.newBuilder()
            .setStartTimeMillis(startTime)
            .setEndTimeMillis(originalRequest.getEndTimeMillis())
            .setFilter(originalRequest.getFilter())
            .setEntityType(originalRequest.getEntityType());
    for (Expression expression : selections) {
      if (expression.hasHealth()) {
        HealthExpression healthExpression = expression.getHealth();
        FunctionExpression function = healthExpression.getFunction();
        TimeAggregation timeAggregation =
            getAggregationFunction(function, startTime, originalRequest.getEndTimeMillis());
        entitiesReqBuilder.addTimeAggregation(timeAggregation);
      } else {
        entitiesReqBuilder.addSelection(expression);
      }
    }
    return entitiesReqBuilder.build();
  }

  private TimeAggregation getAggregationFunction(
      FunctionExpression function, long startTimeMillis, long endTimeMillis) {

    Period period = getPeriod(startTimeMillis, endTimeMillis);
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(period)
            .setAggregation(Expression.newBuilder().setFunction(function).build())
            .build();
    return timeAggregation;
  }

  private Period getPeriod(long startTime, long endTimeMillis) {
    Period period;
    if ((endTimeMillis - startTime) < DAY_IN_MILLIS) {
      long timePeriod = (endTimeMillis - startTime) / (1000 * 60);
      period = Period.newBuilder().setUnit("MINUTES").setValue((int) timePeriod).build();
    } else {
      long timePeriod = (endTimeMillis - startTime) / (1000 * 60 * 60 * 24);
      period = Period.newBuilder().setUnit("HOURS").setValue((int) timePeriod).build();
    }
    return period;
  }

  private long getUpdatedStartTimeForAggregations(long startTimeMillis, long endTimeMillis) {
    if ((endTimeMillis - startTimeMillis) < DAY_IN_MILLIS) {
      startTimeMillis = endTimeMillis - DAY_IN_MILLIS;
    } else {
      startTimeMillis = endTimeMillis - (2 * (endTimeMillis - startTimeMillis));
    }
    return startTimeMillis;
  }

  private long getUpdatedStartTimeForSeriesRequests(long startTimeMillis, long endTimeMillis) {
    if ((endTimeMillis - startTimeMillis) < SIX_HOURS_IN_MILLIS) {
      startTimeMillis = endTimeMillis - (3 * (endTimeMillis - startTimeMillis));
    } else {
      startTimeMillis = endTimeMillis - (2 * (endTimeMillis - startTimeMillis));
    }
    return startTimeMillis;
  }
}
