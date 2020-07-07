package org.hypertrace.gateway.service.entity;

import java.util.Map;
import org.hypertrace.gateway.service.common.QueryRequestContext;

public class EntitiesRequestContext extends QueryRequestContext {
  private final String entityType;

  public EntitiesRequestContext(
      String tenantId,
      long startTimeMillis,
      long endTimeMillis,
      String entityType,
      Map<String, String> requestHeaders) {
    super(tenantId, startTimeMillis, endTimeMillis, requestHeaders);
    this.entityType = entityType;
  }

  public String getEntityType() {
    return this.entityType;
  }
}
