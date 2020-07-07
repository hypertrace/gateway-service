package org.hypertrace.gateway.service.common.converters;

import java.util.ArrayList;
import java.util.List;

public class NumberListConverter {
  static Iterable<? extends Long> convertIntListToLongList(List<Integer> intArrayList) {
    List<Long> longList = new ArrayList<>();
    intArrayList.forEach(e -> longList.add(Long.valueOf(e)));
    return longList;
  }

  static Iterable<? extends Double> convertFloatListToDoubleList(List<Float> intArrayList) {
    List<Double> doubleList = new ArrayList<>();
    intArrayList.forEach(e -> doubleList.add(Double.valueOf(e)));
    return doubleList;
  }
}
