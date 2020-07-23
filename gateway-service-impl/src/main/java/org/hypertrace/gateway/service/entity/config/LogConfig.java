package org.hypertrace.gateway.service.entity.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Configuration for Entity Service logging, outside the log4j configuration
 */
public class LogConfig {
  private static final String LOG_CONFIG = "entity.service.log.config";
  private static final String MAX_QUERY_IN_MILLIS = "query.threshold.millis";
  private static final long DEFAULT_MAX_QUERY_IN_MILLIS = 2500L;
  private final long queryThresholdInMillis;

  public LogConfig(Config appConfig) {
    Config logConfig = appConfig.hasPath(LOG_CONFIG) ? appConfig.getConfig(LOG_CONFIG) :
        ConfigFactory.empty();

    this.queryThresholdInMillis = logConfig.hasPath(MAX_QUERY_IN_MILLIS) ?
        logConfig.getLong(MAX_QUERY_IN_MILLIS) :
        DEFAULT_MAX_QUERY_IN_MILLIS;
  }

  public long getQueryThresholdInMillis() {
    return this.queryThresholdInMillis;
  }
}
