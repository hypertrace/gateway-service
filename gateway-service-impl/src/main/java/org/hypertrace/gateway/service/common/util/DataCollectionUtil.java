package org.hypertrace.gateway.service.common.util;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataCollectionUtil {

  public static <E, C extends Comparator<E>> List<E> limitAndSort(
      Stream<E> unsortedStream, int limit, int offset, int orderByCount, C comparator) {
    // Now apply the sorting and limiting.
    Stream<E> sorted = unsortedStream;
    if (orderByCount > 0) {
      sorted = unsortedStream.sorted(comparator);
    }

    if (offset > 0) {
      sorted = sorted.skip(offset);
    }

    if (limit > 0) {
      sorted = sorted.limit(limit);
    }
    return sorted.collect(Collectors.toList());
  }
}
