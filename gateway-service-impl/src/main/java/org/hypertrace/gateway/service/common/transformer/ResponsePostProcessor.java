package org.hypertrace.gateway.service.common.transformer;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.entity.query.EntityExecutionContext;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;
import org.hypertrace.gateway.service.v1.common.MetricSeries;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResponsePostProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResponsePostProcessor.class);

  public List<Entity.Builder> transform(
      EntityExecutionContext executionContext, List<Entity.Builder> entityBuilders) {
    Set<String> selections =
        executionContext
            .getExpressionContext()
            .getSourceToSelectionExpressionMap()
            .values()
            .stream()
            .flatMap(Collection::stream)
            .map(ExpressionReader::getSelectionResultName)
            .flatMap(Optional::stream)
            .collect(Collectors.toSet());
    Set<String> aggregations =
        executionContext.getExpressionContext().getSourceToMetricAggregationExpressionMap().values().stream()
            .flatMap(Collection::stream)
            .map(expression -> expression.getFunction().getAlias())
            .collect(Collectors.toSet());
    Set<String> timeAggregations =
        executionContext.getExpressionContext().getSourceToTimeAggregationMap().values().stream()
            .flatMap(Collection::stream)
            .map(timeAggregation -> timeAggregation.getAggregation().getFunction().getAlias())
            .collect(Collectors.toSet());
    for (Entity.Builder entityBuilder : entityBuilders) {
      // if the number of selections requested does not match for an entity
      if (entityBuilder.getAttributeCount() != selections.size()) {
        Set<String> attributeKeySet = entityBuilder.getAttributeMap().keySet();
        for (String selection : selections) {
          // if the requested attribute does not exist in the entity, add a default value
          if (!attributeKeySet.contains(selection)) {
            entityBuilder.putAttribute(selection, Value.getDefaultInstance());
          }
        }
      }

      if (entityBuilder.getMetricCount() != aggregations.size()) {
        Set<String> metricKeySet = entityBuilder.getMetricMap().keySet();
        for (String aggregation : aggregations) {
          // if the requested aggregation does not exist in the entity, add a default value
          if (!metricKeySet.contains(aggregation)) {
            entityBuilder.putMetric(aggregation, AggregatedMetricValue.getDefaultInstance());
          }
        }
      }

      if (entityBuilder.getMetricSeriesCount() != timeAggregations.size()) {
        Set<String> metricSeriesKeySet = entityBuilder.getMetricSeriesMap().keySet();
        for (String timeAggregation : timeAggregations) {
          // if the requested metric series does not exist in the entity, add a default value
          if (!metricSeriesKeySet.contains(timeAggregation)) {
            entityBuilder.putMetricSeries(timeAggregation, MetricSeries.getDefaultInstance());
          }
        }
      }
    }

    return entityBuilders;
  }
}
