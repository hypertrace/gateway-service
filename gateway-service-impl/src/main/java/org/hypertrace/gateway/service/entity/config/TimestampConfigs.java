package org.hypertrace.gateway.service.entity.config;

import static java.util.stream.Collectors.toUnmodifiableMap;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;

public class TimestampConfigs {
  private static final String TIMESTAMP_CONFIG = "timestamp.config";
  private static final String SCOPE = "scope";
  private static final String TIMESTAMP = "timestamp";

  private static final Map<String, String> TIMESTAMP_MAP = new HashMap<>();

  public static void init(Config config) {
    TIMESTAMP_MAP.putAll(
        config.getConfigList(TIMESTAMP_CONFIG).stream()
            .collect(
                toUnmodifiableMap(
                    conf -> conf.getString(SCOPE), conf -> conf.getString(TIMESTAMP))));
  }

  public static String getTimestampColumn(String scope) {
    return TIMESTAMP_MAP.get(scope);
  }
}
