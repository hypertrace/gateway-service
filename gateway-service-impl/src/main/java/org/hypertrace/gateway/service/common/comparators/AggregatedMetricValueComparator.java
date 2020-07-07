package org.hypertrace.gateway.service.common.comparators;

import com.google.common.base.Preconditions;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;

public class AggregatedMetricValueComparator {
  public static int compare(AggregatedMetricValue left, AggregatedMetricValue right) {
    if (left == right) {
      return 0;
    } else if (left == null) {
      return -1;
    } else if (right == null) {
      return 1;
    }

    Preconditions.checkArgument(left.getFunction() == right.getFunction());
    return ValueComparator.compare(left.getValue(), right.getValue());
  }
}
