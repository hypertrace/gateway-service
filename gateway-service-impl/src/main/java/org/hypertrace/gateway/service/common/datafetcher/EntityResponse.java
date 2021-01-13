package org.hypertrace.gateway.service.common.datafetcher;

import org.hypertrace.gateway.service.entity.EntityKey;

import java.util.HashSet;
import java.util.Set;

public class EntityResponse {
  private final EntityFetcherResponse entityFetcherResponse;
  // set of entity keys, irrespective of offset and limit
  private final Set<EntityKey> entityKeys;

  public EntityResponse() {
    this.entityFetcherResponse = new EntityFetcherResponse();
    this.entityKeys = new HashSet<>();
  }

  public EntityResponse(EntityFetcherResponse entityFetcherResponse, Set<EntityKey> entityKeys) {
    this.entityFetcherResponse = entityFetcherResponse;
    this.entityKeys = entityKeys;
  }

  public EntityFetcherResponse getEntityFetcherResponse() {
    return entityFetcherResponse;
  }

  public Set<EntityKey> getEntityKeys() {
    return entityKeys;
  }
}
