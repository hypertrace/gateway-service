package org.hypertrace.gateway.service.entity;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.datafetcher.IEntityFetcher;

/**
 * Registry for all the different Entity Fetchers keyed by their corresponding {@link
 * AttributeSource sources}
 */
public class EntityQueryHandlerRegistry {

  private static final EntityQueryHandlerRegistry INSTANCE = new EntityQueryHandlerRegistry();
  // Key is the String representation of the AttributeSource
  private final Map<String, IEntityFetcher> entityFetcherMap;

  private EntityQueryHandlerRegistry() {
    entityFetcherMap = new HashMap<>();
  }

  public static EntityQueryHandlerRegistry get() {
    return INSTANCE;
  }

  public void registerEntityFetcher(String sourceName, IEntityFetcher entityFetcher) {
    entityFetcherMap.put(sourceName, entityFetcher);
  }

  public IEntityFetcher getEntityFetcher(String sourceName) {
    return entityFetcherMap.get(sourceName);
  }
}
