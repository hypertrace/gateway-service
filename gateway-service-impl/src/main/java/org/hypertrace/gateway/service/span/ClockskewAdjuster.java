package org.hypertrace.gateway.service.span;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public interface ClockskewAdjuster extends SpanTransformationStage {

  String CONFIG_PATH = "clockskew.adjuster";
  String FALLBACK_TYPE = "noop";

  static ClockskewAdjuster getAdjuster(Config appConfig) {
    String type;
    try {
      type = appConfig.getString(CONFIG_PATH);
      return ClockskewAdjusters.getAdjuster(type);
    } catch (ConfigException.Missing | ConfigException.WrongType e) {
      return ClockskewAdjusters.getAdjuster(FALLBACK_TYPE);
    }
  }
}
