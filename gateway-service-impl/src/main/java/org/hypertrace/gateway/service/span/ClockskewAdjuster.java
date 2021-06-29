package org.hypertrace.gateway.service.span;

import com.typesafe.config.Config;

public interface ClockskewAdjuster extends SpanTransformationStage {
  static ClockskewAdjuster getAdjuster(Config appConfig) {
    return new NoOpClockskewAdjuster();
  }
}
