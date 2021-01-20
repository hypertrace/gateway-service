package org.hypertrace.gateway.service.common.comparators;

import static java.util.Objects.isNull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import java.util.Comparator;
import java.util.Objects;
import org.hypertrace.gateway.service.v1.common.Value;

public class ValueComparator {

  private static final Comparator<Iterable<String>> STRING_LIST_COMPARATOR =
      Ordering.from(CharSequence::compare).lexicographical();

  private static final Comparator<Iterable<Double>> DOUBLE_LIST_COMPARATOR =
      Ordering.from(Double::compare).lexicographical();

  private static final Comparator<Iterable<Long>> LONG_LIST_COMPARATOR =
      Ordering.from(Long::compare).lexicographical();

  private static final Comparator<Iterable<Boolean>> BOOLEAN_LIST_COMPARATOR =
      Ordering.from(Boolean::compare).lexicographical();

  public static int compare(Value left, Value right) {
    if (Objects.equals(left, right)) {
      return 0;
    }
    if (isNull(left)) {
      return -1;
    }
    if (isNull(right)) {
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
        return CharSequence.compare(left.getString(), right.getString());
      case TIMESTAMP:
        return Long.compare(left.getTimestamp(), right.getTimestamp());
      case BOOLEAN_ARRAY:
        return BOOLEAN_LIST_COMPARATOR.compare(
            left.getBooleanArrayList(), right.getBooleanArrayList());
      case LONG_ARRAY:
        return LONG_LIST_COMPARATOR.compare(left.getLongArrayList(), right.getLongArrayList());
      case DOUBLE_ARRAY:
        return DOUBLE_LIST_COMPARATOR.compare(
            left.getDoubleArrayList(), right.getDoubleArrayList());
      case STRING_ARRAY:
        return STRING_LIST_COMPARATOR.compare(
            left.getStringArrayList(), right.getStringArrayList());
      default:
        throw new IllegalArgumentException("Unhandled type: " + left.getValueType());
    }
  }
}
