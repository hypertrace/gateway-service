package org.hypertrace.gateway.service.common.comparators;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.gateway.service.v1.common.Value;

public class ValueComparator {

  public static int compare(Value left, Value right) {
    if (left == right) {
      return 0;
    } else if (left == null) {
      return -1;
    } else if (right == null) {
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
      default:
        throw new IllegalArgumentException("Unhandled type: " + left.getValueType());
    }
  }
}
