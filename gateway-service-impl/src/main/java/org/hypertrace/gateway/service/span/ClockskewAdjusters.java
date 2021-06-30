package org.hypertrace.gateway.service.span;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory to return concrete instances of {@link ClockskewAdjuster} based on the supplied type */
class ClockskewAdjusters {

  private static final Map<String, Supplier<? extends ClockskewAdjuster>> REGISTRY =
      new HashMap<>();

  private static final Logger LOG = LoggerFactory.getLogger(ClockskewAdjusters.class);

  private static final String JAEGAR_BASED = "jaegar";
  private static final String NOOP_BASED = "noop";

  static {
    REGISTRY.put(JAEGAR_BASED, JaegarBasedClockskewAdjuster::new);
    REGISTRY.put(NOOP_BASED, NoOpClockskewAdjuster::new);
  }

  static ClockskewAdjuster getAdjuster(String type) {
    Supplier<? extends ClockskewAdjuster> supplier = REGISTRY.get(type);
    if (null == supplier) {
      LOG.warn("No clockskew adjuster for the supplied config, falling back to no-op adjuster");
      return REGISTRY.get(NOOP_BASED).get();
    }
    return supplier.get();
  }
}
