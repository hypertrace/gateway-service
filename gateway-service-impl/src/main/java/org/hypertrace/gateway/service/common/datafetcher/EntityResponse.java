package org.hypertrace.gateway.service.common.datafetcher;

import org.hypertrace.gateway.service.entity.EntityKey;

import java.util.HashSet;
import java.util.Set;

public class EntityResponse {
  private final EntityFetcherResponse entityFetcherResponse;

  // total will always be set
  private final long total;

  // set of entity keys, irrespective of offset and limit
  // if this is not set, then the total has been fetched directly and independently
  private Set<EntityKey> entityKeys;

  public EntityResponse() {
    this.entityFetcherResponse = new EntityFetcherResponse();
    this.entityKeys = new HashSet<>();
    this.total = 0;
  }

  public EntityResponse(EntityFetcherResponse entityFetcherResponse, Set<EntityKey> entityKeys) {
    this.entityFetcherResponse = entityFetcherResponse;
    this.entityKeys = entityKeys;
    this.total = this.entityKeys.size();
  }

  public EntityResponse(EntityFetcherResponse entityFetcherResponse, long total) {
    this.entityFetcherResponse = entityFetcherResponse;
    this.total = total;
  }

  public boolean hasEntityKeys() {
    return entityKeys != null;
  }

  public EntityFetcherResponse getEntityFetcherResponse() {
    return entityFetcherResponse;
  }

  public Set<EntityKey> getEntityKeys() {
    return entityKeys;
  }

  public long getTotal() {
    return total;
  }
}
