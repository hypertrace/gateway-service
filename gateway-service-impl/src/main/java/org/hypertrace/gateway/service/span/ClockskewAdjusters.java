package org.hypertrace.gateway.service.span;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

class ClockskewAdjusters {
  private static final Map<String, Supplier<? extends ClockskewAdjuster>> REGISTRY =
      new HashMap<>();

  static {
    REGISTRY.put("jaeger", JaegarBasedClockskewAdjuster::new);
    REGISTRY.put("noop", NoOpClockskewAdjuster::new);
  }

  static ClockskewAdjuster getAdjuster(String type) {
    Supplier<? extends ClockskewAdjuster> supplier = REGISTRY.get(type);
    if (null == supplier) {
      return REGISTRY.get("noop").get();
    }
    return supplier.get();
  }
}
