package org.hypertrace.gateway.service.common.datafetcher;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.v1.common.Interval;
import org.hypertrace.gateway.service.v1.common.MetricSeries;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;

/**
 * Interface spec for fetching entities, aggregated metrics and time series data
 */
public interface IEntityFetcher {

  /**
   * Get entities matching the request criteria
   *
   * @param requestContext Additional context for the incoming request
   * @param entitiesRequest encapsulates the entity query (selection, filter, grouping, order,
   *     pagination etc)
   * @return Map of the Entity Builders keyed by the EntityId
   */
  EntityFetcherResponse getEntities(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest);

  /**
   * Get time series data
   *
   * @param requestContext Additional context for the incoming request
   * @param entitiesRequest encapsulates the time series query (time series selection, filter)
   * @return Nested map of MetricSeries keyed by EntityId and then TimeSeries alias
   */
  EntityFetcherResponse getTimeAggregatedMetrics(
      EntitiesRequestContext requestContext, EntitiesRequest entitiesRequest);

  default MetricSeries getSortedMetricSeries(MetricSeries.Builder builder) {
    List<Interval> sortedIntervals = new ArrayList<>(builder.getValueList());
    sortedIntervals.sort(Comparator.comparingLong(Interval::getStartTimeMillis));
    return MetricSeries.newBuilder()
        .setPeriod(builder.getPeriod())
        .setAggregation(builder.getAggregation())
        .addAllValue(sortedIntervals)
        .build();
  }
}
