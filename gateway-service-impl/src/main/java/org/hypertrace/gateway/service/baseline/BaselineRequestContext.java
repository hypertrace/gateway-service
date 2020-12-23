package org.hypertrace.gateway.service.baseline;

import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;

import java.util.HashMap;
import java.util.Map;

public class BaselineRequestContext extends RequestContext {

  private final Map<String, BaselineTimeAggregation> aliasToTimeAggregation = new HashMap();

  public BaselineRequestContext(String tenantId, Map<String, String> headers) {
    super(tenantId, headers);
  }

  public void mapAliasToTimeAggregation(String alias, BaselineTimeAggregation timeAggregation) {
    aliasToTimeAggregation.put(alias, timeAggregation);
  }

  public BaselineTimeAggregation getTimeAggregationByAlias(String alias) {
    return aliasToTimeAggregation.get(alias);
  }
}
