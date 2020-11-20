package org.hypertrace.gateway.service.entity;

import java.util.Map;
import java.util.Objects;
import org.hypertrace.gateway.service.common.QueryRequestContext;

public class EntitiesRequestContext extends QueryRequestContext {
  private final String entityType;
  private final String timestampAttributeId;

  public EntitiesRequestContext(
      String tenantId,
      long startTimeMillis,
      long endTimeMillis,
      String entityType,
      String timestampAttributeId,
      Map<String, String> requestHeaders) {
    super(tenantId, startTimeMillis, endTimeMillis, requestHeaders);
    this.entityType = entityType;
    this.timestampAttributeId = timestampAttributeId;
  }

  public String getEntityType() {
    return this.entityType;
  }

  public String getTimestampAttributeId() {
    return timestampAttributeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    EntitiesRequestContext that = (EntitiesRequestContext) o;
    return entityType.equals(that.entityType) &&
        timestampAttributeId.equals(that.timestampAttributeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), entityType, timestampAttributeId);
  }
}
