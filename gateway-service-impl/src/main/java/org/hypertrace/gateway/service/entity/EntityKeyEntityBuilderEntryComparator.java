package org.hypertrace.gateway.service.entity;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.hypertrace.gateway.service.common.comparators.AggregatedMetricValueComparator;
import org.hypertrace.gateway.service.common.comparators.OrderByComparator;
import org.hypertrace.gateway.service.common.comparators.ValueComparator;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;

/**
 * Comparator implementation to compare two entities(keyed by EntityKey) based on multiple order by
 * expressions {@link OrderByExpression}.
 */
public class EntityKeyEntityBuilderEntryComparator
    extends OrderByComparator<Map.Entry<EntityKey, Entity.Builder>> {

  public EntityKeyEntityBuilderEntryComparator(List<OrderByExpression> orderByList) {
    super(orderByList);
  }

  @Override
  protected int compareFunctionExpressionValues(
      Entry<EntityKey, Builder> left, Entry<EntityKey, Builder> right, String alias) {
    return AggregatedMetricValueComparator.compare(
        left.getValue().getMetricMap().get(alias), right.getValue().getMetricMap().get(alias));
  }

  @Override
  protected int compareColumnExpressionValues(
      Entry<EntityKey, Builder> left, Entry<EntityKey, Builder> right, String columnName) {
    return ValueComparator.compare(
        left.getValue().getAttributeMap().get(columnName),
        right.getValue().getAttributeMap().get(columnName));
  }
}
