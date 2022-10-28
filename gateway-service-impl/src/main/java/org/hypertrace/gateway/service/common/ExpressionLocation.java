package org.hypertrace.gateway.service.common;

public enum ExpressionLocation {
  COLUMN_SELECTION,
  METRIC_AGGREGATION,
  TIME_AGGREGATION,
  COLUMN_ORDER_BY,
  METRIC_ORDER_BY,
  COLUMN_FILTER,
  COLUMN_GROUP_BY,
}
