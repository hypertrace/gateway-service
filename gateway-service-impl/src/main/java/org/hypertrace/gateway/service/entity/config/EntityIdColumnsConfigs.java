package org.hypertrace.gateway.service.entity.config;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EntityIdColumnsConfigs {
  private static final String ENTITY_ID_COLUMN_CONFIG = "entity.idcolumn.config";
  // Backwards compatibility.
  private static final String DOMAIN_OBJECT_CONFIG = "domainobject.config";
  private static final String SCOPE = "scope";
  private static final String KEY = "key";

  private final Map<String, String> scopeToKeyMap;

  public static EntityIdColumnsConfigs fromConfig(Config config) {
    if (config.hasPath(ENTITY_ID_COLUMN_CONFIG)) {
      return fromConfigList(config.getConfigList(ENTITY_ID_COLUMN_CONFIG));
    } else if (config.hasPath(DOMAIN_OBJECT_CONFIG)) {
      return fromConfigList(config.getConfigList(DOMAIN_OBJECT_CONFIG));
    } else {
      return new EntityIdColumnsConfigs(Map.of());
    }
  }

  private static EntityIdColumnsConfigs fromConfigList(List<? extends Config> configs) {
    Map<String, String> scopeToKeyMap = new HashMap<>();
    EntityIdColumnsConfigs entityIdColumnsConfigs = new EntityIdColumnsConfigs(scopeToKeyMap);
    for (Config scopeConfig : configs) {
      String scope = scopeConfig.getString(SCOPE);
      String key = scopeConfig.getString(KEY);
      scopeToKeyMap.put(scope, key);
    }

    return entityIdColumnsConfigs;
  }

  public EntityIdColumnsConfigs(Map<String, String> scopeToKeyMap) {
    this.scopeToKeyMap = scopeToKeyMap;
  }

  public Optional<String> getIdKey(String scope) {
    if (scopeToKeyMap.containsKey(scope)) {
      return Optional.of(scopeToKeyMap.get(scope));
    } else {
      return Optional.empty();
    }
  }
}
