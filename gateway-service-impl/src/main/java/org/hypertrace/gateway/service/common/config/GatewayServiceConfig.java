package org.hypertrace.gateway.service.common.config;

import com.typesafe.config.Config;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfig;
import org.hypertrace.gateway.service.entity.config.LogConfig;

public class GatewayServiceConfig {
  private static final String ENTITY_AND_FILTER_ENABLED_CONFIG_KEY = "filter.entity.and.enabled";
  private final EntityIdColumnsConfig entityIdColumnsConfig;
  private final ScopeFilterConfigs scopeFilterConfigs;
  private final LogConfig logConfig;
  private final boolean isEntityAndFilterEnabled;

  public GatewayServiceConfig(Config config) {
    this.entityIdColumnsConfig = EntityIdColumnsConfig.fromConfig(config);
    this.scopeFilterConfigs = new ScopeFilterConfigs(config);
    this.logConfig = new LogConfig(config);
    this.isEntityAndFilterEnabled =
        config.hasPath(ENTITY_AND_FILTER_ENABLED_CONFIG_KEY)
            ? config.getBoolean(ENTITY_AND_FILTER_ENABLED_CONFIG_KEY)
            : false;
  }

  public EntityIdColumnsConfig getEntityIdColumnsConfig() {
    return entityIdColumnsConfig;
  }

  public ScopeFilterConfigs getScopeFilterConfigs() {
    return scopeFilterConfigs;
  }

  public LogConfig getLogConfig() {
    return logConfig;
  }

  public boolean isEntityAndFilterEnabled() {
    return isEntityAndFilterEnabled;
  }
}
