package org.hypertrace.gateway.service.entity.config;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// TODO: Nuke class
/**
 * Static class holding all the configs for Domain Objects
 */
public class DomainObjectConfigs {

  private static final String DOMAIN_OBJECT_CONFIG = "domainobject.config";
  private static final String SCOPE = "scope";
  private static final String KEY = "key";
  private static final String PRIMARY_KEY = "primaryKey";
  private static final String FILTER = "filter";
  private static final String MAPPING = "mapping";

  private static final Map<String, DomainObjectConfig> CONFIG_MAP = new HashMap<>();

//  public static void init(Config config) {
//    if (!config.hasPath(DOMAIN_OBJECT_CONFIG)) {
//      return;
//    }
//
//    List<? extends Config> domainObjectsConfig = config.getConfigList(DOMAIN_OBJECT_CONFIG);
//    for (Config domainObjectConfig : domainObjectsConfig) {
//      String scope = domainObjectConfig.getString(SCOPE);
//      String key = domainObjectConfig.getString(KEY);
//      boolean isPrimaryKey =
//          domainObjectConfig.hasPath(PRIMARY_KEY) && domainObjectConfig.getBoolean(PRIMARY_KEY);
//      List<DomainObjectMapping> mapping =
//          domainObjectConfig.getConfigList(MAPPING).stream()
//              .map(
//                  c ->
//                      new DomainObjectMapping(
//                          c.getString(SCOPE),
//                          c.getString(KEY),
//                          c.hasPath(FILTER) ? c.getConfig(FILTER) : null))
//              .collect(Collectors.toList());
//
//      DomainObjectConfig.DomainAttribute attribute =
//          new DomainObjectConfig.DomainAttribute(scope, key, isPrimaryKey);
//      if (!CONFIG_MAP.containsKey(scope)) {
//        Map<DomainObjectConfig.DomainAttribute, List<DomainObjectMapping>> attributeMappings =
//            new HashMap<>();
//        attributeMappings.put(attribute, mapping);
//        CONFIG_MAP.put(scope, new DomainObjectConfig(scope, attributeMappings));
//      } else {
//        CONFIG_MAP.get(scope).getAttributeMappings().put(attribute, mapping);
//      }
//    }
//  }

//  public static Optional<DomainObjectConfig> getDomainObjectConfig(String type) {
//    return Optional.ofNullable(CONFIG_MAP.get(type));
//  }

  // This is used in tests to clear the domain configs after each test that initialized this static
  // class.
//  public static void clearDomainObjectConfigs() {
//    CONFIG_MAP.clear();
//  }
}
