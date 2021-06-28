package org.hypertrace.gateway.service.span;

import com.typesafe.config.Config;

public interface ClockskewAdjuster extends SpanProcessingStage {
  static ClockskewAdjuster getAdjuster(Config appConfig) {
    return ClockskewAdjusters.getAdjuster("noop");
  }
}
