package org.hypertrace.gateway.service.baseline;

import java.util.HashMap;
import java.util.Map;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;

public class BaselineRequestContext extends RequestContext {

  private final Map<String, BaselineTimeAggregation> aliasToTimeAggregation = new HashMap<>();

  public BaselineRequestContext(
      org.hypertrace.core.grpcutils.context.RequestContext grpcRequestContext) {
    super(grpcRequestContext);
  }

  public void mapAliasToTimeAggregation(String alias, BaselineTimeAggregation timeAggregation) {
    aliasToTimeAggregation.put(alias, timeAggregation);
  }

  public BaselineTimeAggregation getTimeAggregationByAlias(String alias) {
    return aliasToTimeAggregation.get(alias);
  }
}
