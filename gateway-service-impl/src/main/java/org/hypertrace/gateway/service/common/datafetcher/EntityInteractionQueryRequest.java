package org.hypertrace.gateway.service.common.datafetcher;

import org.hypertrace.core.query.service.api.QueryRequest;

public class EntityInteractionQueryRequest {
  private boolean isIncoming;
  private String entityType;
  private QueryRequest request;

  public EntityInteractionQueryRequest(
      boolean isIncoming, String entityType, QueryRequest request) {
    this.isIncoming = isIncoming;
    this.entityType = entityType;
    this.request = request;
  }

  public boolean isIncoming() {
    return isIncoming;
  }

  public String getEntityType() {
    return entityType;
  }

  public QueryRequest getRequest() {
    return request;
  }
}
