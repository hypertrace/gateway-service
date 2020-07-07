package org.hypertrace.gateway.service.entity.config;

import static java.util.stream.Collectors.toUnmodifiableMap;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;

/** Static class holding the column mappings for Interactions */
public class InteractionConfigs {

  private static final String INTERACTION_CONFIG = "interaction.config";
  private static final String SCOPE = "scope";
  private static final String CALLER_ATTRIBUTES = "callerAttributes";
  private static final String CALLEE_ATTRIBUTES = "calleeAttributes";

  private static final Map<String, InteractionConfig> CONFIG_MAP = new HashMap<>();

  public static void init(Config config) {
    CONFIG_MAP.putAll(
        config.getConfigList(INTERACTION_CONFIG).stream()
            .collect(
                toUnmodifiableMap(
                    conf -> conf.getString(SCOPE),
                    conf ->
                        new InteractionConfig(
                            conf.getString(SCOPE),
                            conf.getStringList(CALLER_ATTRIBUTES),
                            conf.getStringList(CALLEE_ATTRIBUTES)))));
  }

  public static InteractionConfig getInteractionAttributeConfig(String type) {
    return CONFIG_MAP.get(type);
  }
}
