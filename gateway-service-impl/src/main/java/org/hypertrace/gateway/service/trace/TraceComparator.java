package org.hypertrace.gateway.service.trace;

import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.trace.Trace;

public class TraceComparator implements Comparator<Trace.Builder> {

  private final List<OrderByExpression> orderByExpressions;

  public TraceComparator(List<OrderByExpression> orderByExpressions) {
    this.orderByExpressions = orderByExpressions;
  }

  @Override
  public int compare(Trace.Builder left, Trace.Builder right) {
    for (OrderByExpression orderBy : orderByExpressions) {
      // Check if we need to order by an aggregated metric.
      switch (orderBy.getExpression().getValueCase()) {
        case VALUE_NOT_SET:
        case FUNCTION:
        case LITERAL:
          throw new IllegalArgumentException(
              "Invalid orderBy: " + orderBy.getExpression().getValueCase());

        case COLUMNIDENTIFIER:
          // Otherwise, this is either a simple attribute/metric or entity name itself.
          String attr = orderBy.getExpression().getColumnIdentifier().getColumnName();
          int value;
          value = compare(left.getAttributesMap().get(attr), right.getAttributesMap().get(attr));
          if (value == 0) {
            continue;
          }

          return orderBy.getOrder() == SortOrder.ASC ? value : Math.negateExact(value);
      }
    }
    // If all fields are matching, both the entities are same.
    return 0;
  }

  private int compare(Value left, Value right) {
    if (left == right) {
      return 0;
    } else if (left == null || right == null) {
      return 1;
    }

    Preconditions.checkArgument(left.getValueType() == right.getValueType());

    switch (left.getValueType()) {
      case BOOL:
        return Boolean.compare(left.getBoolean(), right.getBoolean());
      case LONG:
        return Long.compare(left.getLong(), right.getLong());
      case DOUBLE:
        return Double.compare(left.getDouble(), right.getDouble());
      case STRING:
        return StringUtils.compare(left.getString(), right.getString());
      case TIMESTAMP:
        return Long.compare(left.getTimestamp(), right.getTimestamp());
      case BOOLEAN_ARRAY:
        for (int i = 0; i < left.getBooleanArrayCount(); i++) {
          int value = Boolean.compare(left.getBooleanArray(i), right.getBooleanArray(i));
          if (value != 0) {
            return value;
          }
        }
        return 0;
      case LONG_ARRAY:
        for (int i = 0; i < left.getLongArrayCount(); i++) {
          int value = Long.compare(left.getLongArray(i), right.getLongArray(i));
          if (value != 0) {
            return value;
          }
        }
        return 0;
      case DOUBLE_ARRAY:
        for (int i = 0; i < left.getDoubleArrayCount(); i++) {
          int value = Double.compare(left.getDoubleArray(i), right.getDoubleArray(i));
          if (value != 0) {
            return value;
          }
        }
        return 0;
      case STRING_ARRAY:
        for (int i = 0; i < left.getStringArrayCount(); i++) {
          int value = StringUtils.compare(left.getStringArray(i), right.getStringArray(i));
          if (value != 0) {
            return value;
          }
        }
        return 0;
    }

    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }
}
