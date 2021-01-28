package org.hypertrace.gateway.service.common.datafetcher;

public class EntityResponse {
  private final EntityFetcherResponse entityFetcherResponse;
  private final long total;

  public EntityResponse() {
    this.entityFetcherResponse = new EntityFetcherResponse();
    this.total = 0;
  }

  public EntityResponse(EntityFetcherResponse entityFetcherResponse, long total) {
    this.entityFetcherResponse = entityFetcherResponse;
    this.total = total;
  }

  public EntityFetcherResponse getEntityFetcherResponse() {
    return entityFetcherResponse;
  }

  public long getTotal() {
    return total;
  }
}
