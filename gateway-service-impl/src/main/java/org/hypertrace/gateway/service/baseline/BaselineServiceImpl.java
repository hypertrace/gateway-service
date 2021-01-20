package org.hypertrace.gateway.service.baseline;

import com.google.common.annotations.VisibleForTesting;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
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
import org.hypertrace.gateway.service.v1.common.Value;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This service calculates baseline values for an Entity across time range. This converts both
 * aggregate functions and time series functions in selection expression to time series data and
 * generate baseline, lower bound and upper bound. It also changes Time series requests to get more
 * data points for proper baseline calculation
 */
public class BaselineServiceImpl implements BaselineService {

  private static final BaselineEntitiesRequestValidator baselineEntitiesRequestValidator =
      new BaselineEntitiesRequestValidator();

  protected static final long DAY_IN_MILLIS = 86400000L;

  private final AttributeMetadataProvider attributeMetadataProvider;
  private final BaselineServiceQueryParser baselineServiceQueryParser;
  private final BaselineServiceQueryExecutor baselineServiceQueryExecutor;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;

  public BaselineServiceImpl(
      AttributeMetadataProvider attributeMetadataProvider,
      BaselineServiceQueryParser baselineServiceQueryParser,
      BaselineServiceQueryExecutor baselineServiceQueryExecutor,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.baselineServiceQueryParser = baselineServiceQueryParser;
    this.baselineServiceQueryExecutor = baselineServiceQueryExecutor;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
  }

  public BaselineEntitiesResponse getBaselineForEntities(
      String tenantId,
      BaselineEntitiesRequest originalRequest,
      Map<String, String> requestHeaders) {

    BaselineRequestContext requestContext =
        getRequestContext(tenantId, requestHeaders, originalRequest);
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(
            requestContext, originalRequest.getEntityType());
    baselineEntitiesRequestValidator.validate(originalRequest, attributeMetadataMap);
    String timeColumn =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, requestContext, originalRequest.getEntityType());
    Map<String, BaselineEntity> baselineEntityAggregatedMetricsMap = new HashMap<>();
    Map<String, BaselineEntity> baselineEntityTimeSeriesMap = new HashMap<>();

    if (originalRequest.getBaselineAggregateRequestCount() > 0) {
      // Aggregated Functions data
      Period aggTimePeriod =
          getPeriod(originalRequest.getStartTimeMillis(), originalRequest.getEndTimeMillis());
      long periodSecs = getPeriodInSecs(aggTimePeriod);
      long aggStartTime = originalRequest.getStartTimeMillis();
      long aggEndTime = originalRequest.getEndTimeMillis();

      List<TimeAggregation> timeAggregations =
          getTimeAggregationsForAggregateExpr(originalRequest, aggStartTime, aggEndTime);
      updateAliasMap(requestContext, timeAggregations);
      // Take more data to calculate baseline and standard deviation.
      long seriesStartTime = getUpdatedStartTime(aggStartTime, aggEndTime);
      long seriesEndTime = aggStartTime;
      List<String> entityIdAttributes =
          AttributeMetadataUtil.getIdAttributeIds(
              attributeMetadataProvider,
              entityIdColumnsConfigs,
              requestContext,
              originalRequest.getEntityType());
      QueryRequest aggQueryRequest =
          baselineServiceQueryParser.getQueryRequest(
              seriesStartTime,
              seriesEndTime,
              originalRequest.getEntityIdsList(),
              timeColumn,
              timeAggregations,
              periodSecs,
              entityIdAttributes);
      Iterator<ResultSetChunk> aggResponseChunkIterator =
          baselineServiceQueryExecutor.executeQuery(requestHeaders, aggQueryRequest);
      BaselineEntitiesResponse aggEntitiesResponse =
          baselineServiceQueryParser.parseQueryResponse(
              aggResponseChunkIterator,
              requestContext,
              entityIdAttributes.size(),
              originalRequest.getEntityType(),
              aggStartTime,
              aggEndTime);
      baselineEntityAggregatedMetricsMap = getEntitiesMapFromAggResponse(aggEntitiesResponse);
    }

    // Time Series data
    if (originalRequest.getBaselineMetricSeriesRequestCount() > 0) {
      Period timeSeriesPeriod =
          getTimeSeriesPeriod(originalRequest.getBaselineMetricSeriesRequestList());
      long periodSecs = getPeriodInSecs(timeSeriesPeriod);
      long alignedStartTime =
          QueryExpressionUtil.alignToPeriodBoundary(
              originalRequest.getStartTimeMillis(), periodSecs, true);
      long alignedEndTime =
          QueryExpressionUtil.alignToPeriodBoundary(
              originalRequest.getEndTimeMillis(), periodSecs, false);
      List<TimeAggregation> timeAggregations =
          getTimeAggregationsForTimeSeriesExpr(originalRequest);
      long seriesStartTime = getUpdatedStartTime(alignedStartTime, alignedEndTime);
      long seriesEndTime = alignedStartTime;
      List<String> entityIdAttributes =
          AttributeMetadataUtil.getIdAttributeIds(
              attributeMetadataProvider,
              entityIdColumnsConfigs,
              requestContext,
              originalRequest.getEntityType());
      QueryRequest timeSeriesQueryRequest =
          baselineServiceQueryParser.getQueryRequest(
              seriesStartTime,
              seriesEndTime,
              originalRequest.getEntityIdsList(),
              timeColumn,
              timeAggregations,
              periodSecs,
              entityIdAttributes);
      Iterator<ResultSetChunk> timeSeriesChunkIterator =
          baselineServiceQueryExecutor.executeQuery(requestHeaders, timeSeriesQueryRequest);
      BaselineEntitiesResponse timeSeriesEntitiesResponse =
          baselineServiceQueryParser.parseQueryResponse(
              timeSeriesChunkIterator,
              requestContext,
              entityIdAttributes.size(),
              originalRequest.getEntityType(),
              alignedStartTime,
              alignedEndTime);
      baselineEntityTimeSeriesMap =
          getEntitiesMapFromTimeSeriesResponse(
              timeSeriesEntitiesResponse, alignedStartTime, alignedEndTime, periodSecs);
    }

    return mergeEntities(baselineEntityAggregatedMetricsMap, baselineEntityTimeSeriesMap);
  }

  private void updateAliasMap(
      BaselineRequestContext requestContext, List<TimeAggregation> timeAggregations) {
    timeAggregations.forEach(
        (timeAggregation ->
            requestContext.mapAliasToTimeAggregation(
                timeAggregation.getAggregation().getFunction().getAlias(),
                getBaselineAggregation(timeAggregation))));
  }

  private BaselineTimeAggregation getBaselineAggregation(TimeAggregation timeAggregation) {
    return BaselineTimeAggregation.newBuilder()
        .setPeriod(timeAggregation.getPeriod())
        .setAggregation(timeAggregation.getAggregation().getFunction())
        .build();
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
            BaselineEntity.Builder baselineEntityBuilder =
                getBaselineEntityBuilder(value)
                    .putAllBaselineAggregateMetric(value.getBaselineAggregateMetricMap());
            if (!baselineEntityTimeSeriesMap.isEmpty()) {
              baselineEntityBuilder.putAllBaselineMetricSeries(
                  baselineEntityTimeSeriesMap.get(key).getBaselineMetricSeriesMap());
            }
            BaselineEntity baselineEntity = baselineEntityBuilder.build();
            baselineEntityList.add(baselineEntity);
          });
    } else if (!baselineEntityTimeSeriesMap.isEmpty()) {
      baselineEntityTimeSeriesMap.forEach(
          (key, value) -> {
            BaselineEntity baselineEntity =
                getBaselineEntityBuilder(value)
                    .putAllBaselineMetricSeries(value.getBaselineMetricSeriesMap())
                    .build();
            baselineEntityList.add(baselineEntity);
          });
    }
    return baselineEntitiesResponseBuilder.addAllBaselineEntity(baselineEntityList).build();
  }

  private Map<String, BaselineEntity> getEntitiesMapFromTimeSeriesResponse(
      BaselineEntitiesResponse baselineEntitiesResponse,
      long startTimeInMillis,
      long endTimeMillis,
      long periodInSecs) {
    Map<String, BaselineEntity> baselineEntityMap = new HashMap<>();
    for (BaselineEntity baselineEntity : baselineEntitiesResponse.getBaselineEntityList()) {
      Map<String, BaselineMetricSeries> metricSeriesMap =
          baselineEntity.getBaselineMetricSeriesMap();
      Map<String, Baseline> baselineMap = new HashMap<>();
      BaselineEntity.Builder baselineEntityBuilder = getBaselineEntityBuilder(baselineEntity);
      Map<String, BaselineMetricSeries> revisedMetricSeriesMap = new HashMap<>();
      // Calculate baseline
      metricSeriesMap.forEach(
          (key, value) -> {
            List<Value> metricValues =
                value.getBaselineValueList().stream()
                    .map(x -> x.getBaseline().getValue())
                    .collect(Collectors.toList());
            Baseline baseline = BaselineCalculator.getBaseline(metricValues);
            baselineMap.put(key, baseline);
          });
      // Update intervals
      metricSeriesMap.forEach(
          (key, value) -> {
            List<BaselineInterval> baselineIntervalList = new ArrayList<>();
            long intervalTime = startTimeInMillis;
            while (intervalTime < endTimeMillis) {
              long periodInMillis = TimeUnit.SECONDS.toMillis(periodInSecs);
              BaselineInterval newBaselineInterval =
                  BaselineInterval.newBuilder()
                      .setBaseline(baselineMap.get(key))
                      .setStartTimeMillis(intervalTime)
                      .setEndTimeMillis(intervalTime + periodInMillis)
                      .build();
              baselineIntervalList.add(newBaselineInterval);
              intervalTime += periodInMillis;
            }
            BaselineMetricSeries baselineMetricSeries =
                BaselineMetricSeries.newBuilder().addAllBaselineValue(baselineIntervalList).build();
            revisedMetricSeriesMap.put(key, baselineMetricSeries);
          });
      baselineEntityBuilder.putAllBaselineMetricSeries(revisedMetricSeriesMap);
      baselineEntityMap.put(baselineEntity.getId(), baselineEntityBuilder.build());
    }
    return baselineEntityMap;
  }

  private BaselineEntity.Builder getBaselineEntityBuilder(BaselineEntity baselineEntity) {
    return BaselineEntity.newBuilder()
        .setEntityType(baselineEntity.getEntityType())
        .setId(baselineEntity.getId());
  }

  private Map<String, BaselineEntity> getEntitiesMapFromAggResponse(
      BaselineEntitiesResponse baselineEntitiesResponse) {
    Map<String, BaselineEntity> baselineEntityMap = new HashMap<>();
    if (baselineEntitiesResponse.getBaselineEntityCount() > 0) {
      for (BaselineEntity baselineEntity : baselineEntitiesResponse.getBaselineEntityList()) {
        Map<String, BaselineMetricSeries> metricSeriesMap =
            baselineEntity.getBaselineMetricSeriesMap();
        Map<String, Baseline> baselineMap = new HashMap<>();
        BaselineEntity.Builder baselineEntityBuilder = getBaselineEntityBuilder(baselineEntity);
        metricSeriesMap.forEach(
            (key, value) -> {
              List<Value> metricValues =
                  value.getBaselineValueList().stream()
                      .map(x -> x.getBaseline().getValue())
                      .collect(Collectors.toList());
              Baseline baseline = BaselineCalculator.getBaseline(metricValues);
              baselineMap.put(key, baseline);
            });
        baselineEntityBuilder.putAllBaselineAggregateMetric(baselineMap);
        baselineEntityMap.put(baselineEntity.getId(), baselineEntityBuilder.build());
      }
    }
    return baselineEntityMap;
  }

  private List<TimeAggregation> getTimeAggregationsForTimeSeriesExpr(
      BaselineEntitiesRequest originalRequest) {
    List<BaselineTimeAggregation> timeSeriesList =
        originalRequest.getBaselineMetricSeriesRequestList();
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
      BaselineEntitiesRequest originalRequest, long alignedStartTime, long alignedEndTime) {
    List<FunctionExpression> aggregateList = originalRequest.getBaselineAggregateRequestList();
    List<TimeAggregation> timeAggregationList = new ArrayList<>();
    for (FunctionExpression function : aggregateList) {
      TimeAggregation timeAggregation =
          getAggregationFunction(function, alignedStartTime, alignedEndTime);
      timeAggregationList.add(timeAggregation);
    }
    return timeAggregationList;
  }

  private TimeAggregation getAggregationFunction(
      FunctionExpression function, long startTimeMillis, long endTimeMillis) {
    Period period = getPeriod(startTimeMillis, endTimeMillis);
    return TimeAggregation.newBuilder()
        .setPeriod(period)
        .setAggregation(Expression.newBuilder().setFunction(function).build())
        .build();
  }

  private Period getPeriod(long startTimeInMillis, long endTimeMillis) {
    long timePeriod = (endTimeMillis - startTimeInMillis) / (1000 * 60);
    return Period.newBuilder().setUnit("MINUTES").setValue((int) timePeriod).build();
  }

  private Period getTimeSeriesPeriod(List<BaselineTimeAggregation> baselineTimeSeriesRequestList) {
    return baselineTimeSeriesRequestList.get(0).getPeriod();
  }

  private long getPeriodInSecs(Period period) {
    ChronoUnit unit = ChronoUnit.valueOf(period.getUnit());
    return Duration.of(period.getValue(), unit).getSeconds();
  }

  /**
   * User selected time range(USTR) is difference between end time and start time. It updates start
   * time -> startTime minus maximum of (24h, 2xUSTR)
   */
  @VisibleForTesting
  long getUpdatedStartTime(long startTimeMillis, long endTimeMillis) {
    long timeDiff = Math.max(DAY_IN_MILLIS, (2 * (endTimeMillis - startTimeMillis)));
    return startTimeMillis - timeDiff;
  }
}
