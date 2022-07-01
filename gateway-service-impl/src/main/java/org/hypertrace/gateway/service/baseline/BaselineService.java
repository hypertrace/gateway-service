package org.hypertrace.gateway.service.baseline;

import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;

public interface BaselineService {
  BaselineEntitiesResponse getBaselineForEntities(
      RequestContext requestContext, BaselineEntitiesRequest originalRequest);
}
