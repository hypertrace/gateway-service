package org.hypertrace.gateway.service.baseline;

import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;

import java.util.Map;

public interface BaselineService {
  BaselineEntitiesResponse getBaselineForEntities(
          String tenantId, BaselineEntitiesRequest originalRequest, Map<String, String> requestHeaders);
}
