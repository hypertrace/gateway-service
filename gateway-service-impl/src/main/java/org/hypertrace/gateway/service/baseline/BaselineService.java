package org.hypertrace.gateway.service.baseline;

import java.util.Map;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;

public interface BaselineService {
  BaselineEntitiesResponse getBaselineForEntities(
      String tenantId, BaselineEntitiesRequest originalRequest, Map<String, String> requestHeaders);

  Baseline calculateBaseline(double[] values);
}
