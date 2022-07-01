package org.hypertrace.gateway.service.entity;

import java.util.Objects;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.gateway.service.common.QueryRequestContext;

public class EntitiesRequestContext extends QueryRequestContext {
  private final String entityType;
  private final String timestampAttributeId;

  public EntitiesRequestContext(
      RequestContext requestContext,
      long startTimeMillis,
      long endTimeMillis,
      String entityType,
      String timestampAttributeId) {
    super(requestContext, startTimeMillis, endTimeMillis);
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
    return entityType.equals(that.entityType)
        && timestampAttributeId.equals(that.timestampAttributeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), entityType, timestampAttributeId);
  }
}
