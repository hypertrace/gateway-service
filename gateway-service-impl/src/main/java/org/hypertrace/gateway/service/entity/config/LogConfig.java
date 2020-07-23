package org.hypertrace.gateway.service.entity.config;

import com.typesafe.config.Config;

public class LogConfig {
  private static final String LOG_CONFIG = "log.config";
  private static final String MAX_QUERY_IN_MILLIS = "query.threshold.millis";
  private static final long DEFAULT_MAX_QUERY_IN_MILLIS = 2500L;
  private final long queryThresholdInMillis;

  public LogConfig(Config appConfig) {
    Config logConfig = appConfig.getConfig(LOG_CONFIG);
    if (logConfig == null) {
      this.queryThresholdInMillis = DEFAULT_MAX_QUERY_IN_MILLIS;
    } else {
      this.queryThresholdInMillis = logConfig.hasPath(MAX_QUERY_IN_MILLIS) ?
          logConfig.getLong(MAX_QUERY_IN_MILLIS) :
          DEFAULT_MAX_QUERY_IN_MILLIS;
    }
  }

  public long getQueryThresholdInMillis() {
    return this.queryThresholdInMillis;
  }
}
