package org.hypertrace.gateway.service.entity;

import java.util.Map;
import java.util.Objects;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    EntitiesRequestContext that = (EntitiesRequestContext) o;
    return Objects.equals(entityType, that.entityType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), entityType);
  }
}
