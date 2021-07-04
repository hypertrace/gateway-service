package org.hypertrace.gateway.service.span;

import com.typesafe.config.Config;
import java.time.Duration;
import java.time.Instant;
import lombok.Data;
import lombok.experimental.Accessors;
import org.hypertrace.gateway.service.v1.span.SpanEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ClockskewAdjuster implements SpanTransformationStage {

  private static final Logger LOG = LoggerFactory.getLogger(ClockskewAdjuster.class);

  private static final String CONFIG_PATH = "clockskew.adjuster";
  private static final String FALLBACK_TYPE = "noop";

  /**
   * Returns a concrete implementation of {@link ClockskewAdjuster} based on the supplied type. If
   * an incorrect configuration is supplied, then it fallbacks to {@link NoOpClockskewAdjuster}.
   * This method never throws an exception. Any exception propagated up from downstream is swallowed
   * and the fallback adjuster is returned
   *
   * @param appConfig the app configuration
   * @return a concrete implementation of {@link ClockskewAdjuster}
   */
  public static ClockskewAdjuster getAdjuster(Config appConfig) {
    String type;
    try {
      type = appConfig.getString(CONFIG_PATH);
      return ClockskewAdjusters.getAdjuster(type);
    } catch (Exception e) {
      LOG.warn(
          "Some exception occurred while trying to get the clockskew adjuster, falling back to no-op adjuster");
      return ClockskewAdjusters.getAdjuster(FALLBACK_TYPE);
    }
  }

  @Data
  @Accessors(chain = true, fluent = true)
  static class Span {
    private String id;
    private String parentSpanId;
    private Instant startTime;
    private Instant endTime;
    private Duration duration;
    // the mutable builder object
    private SpanEvent.Builder spanBuilder;
  }
}
