package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.baseline.BaselineInterval;
import org.hypertrace.gateway.service.v1.baseline.BaselineMetricSeries;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntity;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This service calculates baseline values for an Entity across time range. This converts Functions
 * in selection expression to time series data and generate baseline, lower bound and upper bound.
 * It also changes Time aggregation requests to get more data points for proper baseline calculation
 */
public class BaselineServiceImpl implements BaselineService {
  private static long DAY_IN_MILLIS = 86400000L;
  private static long SIX_HOURS_IN_MILLIS = 21600000L;

  private static final Logger LOG = LoggerFactory.getLogger(BaselineServiceImpl.class);
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final QueryServiceBaselineHelper queryServiceBaselineHelper;

  public BaselineServiceImpl(
      AttributeMetadataProvider attributeMetadataProvider,
      QueryServiceBaselineHelper queryServiceBaselineHelper) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.queryServiceBaselineHelper = queryServiceBaselineHelper;
  }

  public BaselineEntitiesResponse getBaselineForEntities(
      String tenantId,
      BaselineEntitiesRequest originalRequest,
      Map<String, String> requestHeaders) {
    BaselineRequestContext requestContext =
        getRequestContext(tenantId, requestHeaders, originalRequest);
    String timeColumn =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, requestContext, originalRequest.getEntityType());
    Map<String, BaselineEntity> baselineEntityAggregatedMetricsMap = Collections.EMPTY_MAP;
    Map<String, BaselineEntity> baselineEntityTimeSeriesMap = Collections.EMPTY_MAP;

    if (!originalRequest.getBaselineAggregateRequestList().isEmpty()) {
      // Aggregated Functions data
      Period aggTimePeriod =
          getPeriod(originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());
      long periodSecs = getPeriodInSecs(aggTimePeriod);
      List<TimeAggregation> timeAggregations = getTimeAggregationsForAggregateExpr(originalRequest);
      // Take more data to calculate baseline and standard deviation.
      long seriesStartTime =
          getUpdatedStartTimeForAggregations(
              originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());
      QueryRequest aggQueryRequest =
          queryServiceBaselineHelper.getQueryRequest(
              seriesStartTime,
              originalRequest.getEndTimeMillis(),
              originalRequest.getEntityIdsList(),
              timeColumn,
              timeAggregations,
              periodSecs);
      Iterator<ResultSetChunk> aggResponseChunkIterator =
          queryServiceBaselineHelper.executeQuery(requestHeaders, aggQueryRequest);
      BaselineEntitiesResponse aggEntitiesResponse =
          queryServiceBaselineHelper.parseQueryResponse(
              aggResponseChunkIterator,
              requestContext,
              originalRequest.getEntityIdsCount(),
              originalRequest.getEntityType(),
              periodSecs);
      baselineEntityAggregatedMetricsMap = getEntitiesMapFromAggResponse(aggEntitiesResponse);
    }

    // Time Series data
    if (!originalRequest.getBaselineMetricSeriesRequestList().isEmpty()) {
      Period timeSeriesPeriod = getTimeSeriesPeriod(originalRequest.getBaselineMetricSeriesRequestList());
      long periodSecs = getPeriodInSecs(timeSeriesPeriod);
      List<TimeAggregation> timeAggregations =
          getTimeAggregationsForTimeSeriesExpr(originalRequest);
      long seriesStartTime =
          getUpdatedStartTimeForSeriesRequests(
              originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());
      QueryRequest timeSeriesQueryRequest =
          queryServiceBaselineHelper.getQueryRequest(
              seriesStartTime,
              originalRequest.getEndTimeMillis(),
              originalRequest.getEntityIdsList(),
              timeColumn,
              timeAggregations,
              periodSecs);
      Iterator<ResultSetChunk> timeSeriesChunkIterator =
          queryServiceBaselineHelper.executeQuery(requestHeaders, timeSeriesQueryRequest);
      BaselineEntitiesResponse timeSeriesEntitiesResponse =
          queryServiceBaselineHelper.parseQueryResponse(
              timeSeriesChunkIterator,
              requestContext,
              originalRequest.getEntityIdsCount(),
              originalRequest.getEntityType(),
              periodSecs);
      baselineEntityTimeSeriesMap =
          getEntitiesMapFromTimeSeriesResponse(
              timeSeriesEntitiesResponse, originalRequest.getStartTimeMillis());
    }

    BaselineEntitiesResponse mergedResponse =
        mergeEntities(baselineEntityAggregatedMetricsMap, baselineEntityTimeSeriesMap);
    return mergedResponse;
  }

  private BaselineRequestContext getRequestContext(
      String tenantId,
      Map<String, String> requestHeaders,
      BaselineEntitiesRequest baselineEntitiesRequest) {
    BaselineRequestContext requestContext = new BaselineRequestContext(tenantId, requestHeaders);
    baselineEntitiesRequest
        .getBaselineMetricSeriesRequestList()
        .forEach(
            (timeAggregation ->
                requestContext.mapAliasToTimeAggregation(
                    timeAggregation.getAggregation().getAlias(), timeAggregation)));
    return requestContext;
  }

  private BaselineEntitiesResponse mergeEntities(
      Map<String, BaselineEntity> baselineEntityAggregatedMetricsMap,
      Map<String, BaselineEntity> baselineEntityTimeSeriesMap) {
    BaselineEntitiesResponse.Builder baselineEntitiesResponseBuilder =
        BaselineEntitiesResponse.newBuilder();
    List<BaselineEntity> baselineEntityList = new ArrayList<>();
    if (!baselineEntityAggregatedMetricsMap.isEmpty()) {
      baselineEntityAggregatedMetricsMap.forEach(
          (key, value) -> {
            BaselineEntity baselineEntity =
                BaselineEntity.newBuilder()
                    .setEntityType(value.getEntityType())
                    .setId(value.getId())
                    .putAllBaselineAggregateMetric(value.getBaselineAggregateMetricMap())
                    .putAllBaselineMetricSeries(
                        baselineEntityTimeSeriesMap.get(key).getBaselineMetricSeriesMap())
                    .build();
            baselineEntityList.add(baselineEntity);
          });
    }
    return baselineEntitiesResponseBuilder.addAllBaselineEntity(baselineEntityList).build();
  }

  private Map<String, BaselineEntity> getEntitiesMapFromTimeSeriesResponse(
      BaselineEntitiesResponse baselineEntitiesResponse, long startTimeMillis) {
    Map<String, BaselineEntity> baselineEntityMap = new HashMap<>();
    for (BaselineEntity baselineEntity : baselineEntitiesResponse.getBaselineEntityList()) {
      Map<String, BaselineMetricSeries> metricSeriesMap =
          baselineEntity.getBaselineMetricSeriesMap();
      Map<String, Baseline> baselineMap = new HashMap<>();
      BaselineEntity.Builder baselineEntityBuilder =
          BaselineEntity.newBuilder()
              .setEntityType(baselineEntity.getEntityType())
              .setId(baselineEntity.getEntityType());
      Map<String, BaselineMetricSeries> revisedMetricSeriesMap = new HashMap<>();
      // Calculate baseline
      metricSeriesMap.forEach(
          (key, value) -> {
            List<Double> metricValues =
                value.getBaselineValueList().stream()
                    .map(x -> x.getBaseline().getValue().getDouble())
                    .collect(Collectors.toList());
            double[] metricValueArray =
                metricValues.stream().mapToDouble(Double::doubleValue).toArray();
            Baseline baseline = BaselineCalculator.getBaseline(metricValueArray);
            baselineMap.put(key, baseline);
          });
      // Update intervals
      metricSeriesMap.forEach(
          (key, value) -> {
            List<BaselineInterval> intervalList = value.getBaselineValueList();
            List<BaselineInterval> revisedList = new ArrayList<>();
            for (BaselineInterval baselineInterval : intervalList) {
              if (baselineInterval.getStartTimeMillis() >= startTimeMillis) {
                BaselineInterval newBaselineInterval =
                    BaselineInterval.newBuilder()
                        .setBaseline(baselineMap.get(key))
                        .setStartTimeMillis(baselineInterval.getStartTimeMillis())
                        .setEndTimeMillis(baselineInterval.getEndTimeMillis())
                        .build();
                revisedList.add(newBaselineInterval);
              }
            }
            BaselineMetricSeries baselineMetricSeries =
                BaselineMetricSeries.newBuilder()
                    .setAggregation(value.getAggregation())
                    .setPeriod(value.getPeriod())
                    .build();
            revisedMetricSeriesMap.put(key, baselineMetricSeries);
          });
      baselineEntityBuilder.putAllBaselineMetricSeries(revisedMetricSeriesMap);
      baselineEntityMap.put(baselineEntity.getId(), baselineEntityBuilder.build());
    }
    return baselineEntityMap;
  }

  private Map<String, BaselineEntity> getEntitiesMapFromAggResponse(
      BaselineEntitiesResponse baselineEntitiesResponse) {
    Map<String, BaselineEntity> baselineEntityMap = new HashMap<>();
    for (BaselineEntity baselineEntity : baselineEntitiesResponse.getBaselineEntityList()) {
      Map<String, BaselineMetricSeries> metricSeriesMap =
          baselineEntity.getBaselineMetricSeriesMap();
      Map<String, Baseline> baselineMap = new HashMap<>();
      BaselineEntity.Builder baselineEntityBuilder =
          BaselineEntity.newBuilder()
              .setEntityType(baselineEntity.getEntityType())
              .setId(baselineEntity.getEntityType());

      metricSeriesMap.forEach(
          (key, value) -> {
            List<Double> metricValues =
                value.getBaselineValueList().stream()
                    .map(x -> x.getBaseline().getValue().getDouble())
                    .collect(Collectors.toList());
            double[] metricValueArray =
                metricValues.stream().mapToDouble(Double::doubleValue).toArray();
            Baseline baseline = BaselineCalculator.getBaseline(metricValueArray);
            baselineMap.put(key, baseline);
          });
      baselineEntityBuilder.putAllBaselineAggregateMetric(baselineMap);
      baselineEntityMap.put(baselineEntity.getId(), baselineEntityBuilder.build());
    }
    return baselineEntityMap;
  }

  private List<TimeAggregation> getTimeAggregationsForTimeSeriesExpr(
      BaselineEntitiesRequest originalRequest) {
    List<BaselineTimeAggregation> timeSeriesList = originalRequest.getBaselineMetricSeriesRequestList();
    List<TimeAggregation> timeAggregations = new ArrayList<>();
    for (BaselineTimeAggregation timeAggregation : timeSeriesList) {
      Expression aggregation =
          Expression.newBuilder().setFunction(timeAggregation.getAggregation()).build();
      TimeAggregation agg =
          TimeAggregation.newBuilder()
              .setAggregation(aggregation)
              .setPeriod(timeAggregation.getPeriod())
              .build();
      timeAggregations.add(agg);
    }
    return timeAggregations;
  }

  private List<TimeAggregation> getTimeAggregationsForAggregateExpr(
      BaselineEntitiesRequest originalRequest) {
    List<FunctionExpression> aggregateList = originalRequest.getBaselineAggregateRequestList();
    List<TimeAggregation> timeAggregationList = new ArrayList<>();
    for (FunctionExpression function : aggregateList) {
      TimeAggregation timeAggregation =
          getAggregationFunction(
              function, originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());
      timeAggregationList.add(timeAggregation);
    }
    return timeAggregationList;
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

  private Period getPeriod(long startTimeInMillis, long endTimeMillis) {
    Period period;
    if ((endTimeMillis - startTimeInMillis) < DAY_IN_MILLIS) {
      long timePeriod = (endTimeMillis - startTimeInMillis) / (1000 * 60);
      period = Period.newBuilder().setUnit("MINUTES").setValue((int) timePeriod).build();
    } else {
      long timePeriod = (endTimeMillis - startTimeInMillis) / (1000 * 60 * 60 * 24);
      period = Period.newBuilder().setUnit("HOURS").setValue((int) timePeriod).build();
    }
    return period;
  }

  private Period getTimeSeriesPeriod(List<BaselineTimeAggregation> baselineTimeSeriesList) {
    return baselineTimeSeriesList.get(0).getPeriod();
  }

  private long getPeriodInSecs(Period period) {
    ChronoUnit unit = ChronoUnit.valueOf(period.getUnit());
    long periodSecs = Duration.of(period.getValue(), unit).getSeconds();
    return periodSecs;
  }

  /**
   * If user selected time range is less than 24 hrs then choose last 24hrs of data for baseline if
   * it is gt 24hours then choose 2X time range for baseline calculation.
   *
   * @param startTimeMillis
   * @param endTimeMillis
   * @return
   */
  private long getUpdatedStartTimeForAggregations(long startTimeMillis, long endTimeMillis) {
    if ((endTimeMillis - startTimeMillis) < DAY_IN_MILLIS) {
      startTimeMillis = endTimeMillis - DAY_IN_MILLIS;
    } else {
      startTimeMillis = endTimeMillis - (2 * (endTimeMillis - startTimeMillis));
    }
    return startTimeMillis;
  }

  /**
   * If user selected time range < 6 hours then 3X time range if it is > 6 hours the 2X time range.
   *
   * @param startTimeMillis
   * @param endTimeMillis
   * @return
   */
  private long getUpdatedStartTimeForSeriesRequests(long startTimeMillis, long endTimeMillis) {
    if ((endTimeMillis - startTimeMillis) < SIX_HOURS_IN_MILLIS) {
      startTimeMillis = endTimeMillis - (3 * (endTimeMillis - startTimeMillis));
    } else {
      startTimeMillis = endTimeMillis - (2 * (endTimeMillis - startTimeMillis));
    }
    return startTimeMillis;
  }
}
