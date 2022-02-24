package org.hypertrace.gateway.service.common.converters;

import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.explore.ExploreRequestContext;

public class ExploreToEntityConverter {
  public static EntitiesRequestContext convert(
      AttributeMetadataProvider attributeMetadataProvider, ExploreRequestContext requestContext) {
    String entityType = requestContext.getContext();

    String timestampAttributeId =
        AttributeMetadataUtil.getTimestampAttributeId(
            attributeMetadataProvider, requestContext, entityType);
    return new EntitiesRequestContext(
        requestContext.getTenantId(),
        requestContext.getStartTimeMillis(),
        requestContext.getEndTimeMillis(),
        entityType,
        timestampAttributeId,
        requestContext.getHeaders());
  }
}
