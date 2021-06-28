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
    return REGISTRY.get(type).get();
  }
}
