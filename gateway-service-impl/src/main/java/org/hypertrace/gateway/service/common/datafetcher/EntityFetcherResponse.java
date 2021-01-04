package org.hypertrace.gateway.service.common.datafetcher;

import java.util.LinkedHashMap;
import java.util.Map;

import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;

/** Response class that encapsulates the response from any of the {@link IEntityFetcher}s */
public class EntityFetcherResponse {
  private final Map<EntityKey, Builder> entityKeyBuilderMap;

  public EntityFetcherResponse() {
    entityKeyBuilderMap = new LinkedHashMap<>();
  }

  public EntityFetcherResponse(Map<EntityKey, Builder> entityKeyBuilderMap) {
    this.entityKeyBuilderMap = entityKeyBuilderMap;
  }

  public Map<EntityKey, Builder> getEntityKeyBuilderMap() {
    return entityKeyBuilderMap;
  }

  public int size() {
    return entityKeyBuilderMap.size();
  }

  public boolean isEmpty() {
    return entityKeyBuilderMap.isEmpty();
  }
}
