package org.hypertrace.gateway.service.common.datafetcher;

import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.gateway.service.v1.entity.InteractionsRequest;

public class EntityInteractionQueryRequest {
  private boolean isIncoming;
  private String entityType;
  private InteractionsRequest interactionsRequest;
  private QueryRequest request;

  public EntityInteractionQueryRequest(
      boolean isIncoming,
      String entityType,
      InteractionsRequest interactionsRequest,
      QueryRequest request) {
    this.isIncoming = isIncoming;
    this.entityType = entityType;
    this.interactionsRequest = interactionsRequest;
    this.request = request;
  }

  public boolean isIncoming() {
    return isIncoming;
  }

  public String getEntityType() {
    return entityType;
  }

  public InteractionsRequest getInteractionsRequest() {
    return interactionsRequest;
  }

  public QueryRequest getRequest() {
    return request;
  }

  @Override
  public String toString() {
    return "EntityInteractionQueryRequest{"
        + "isIncoming="
        + isIncoming
        + ", entityType='"
        + entityType
        + '\''
        + ", interactionsRequest="
        + interactionsRequest
        + ", request="
        + request
        + '}';
  }
}
