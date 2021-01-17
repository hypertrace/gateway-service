package org.hypertrace.gateway.service.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;

// Hold some of request fields and mappings needed when parsing the query service response
public class QueryRequestContext extends RequestContext {
  private final Map<String, FunctionExpression> aliasToFunctionExpressionMap = new HashMap<>();
  private final long startTimeMillis;
  private final long endTimeMillis;
  private final Map<String, TimeAggregation> aliasToTimeAggregation = new HashMap();

  public QueryRequestContext(
      String tenantId, long startTimeMillis, long endTimeMillis, Map<String, String> headers) {
    super(tenantId, headers);
    this.startTimeMillis = startTimeMillis;
    this.endTimeMillis = endTimeMillis;
  }

  public void mapAliasToFunctionExpression(String alias, FunctionExpression functionExpression) {
    aliasToFunctionExpressionMap.put(alias, functionExpression);
  }

  public boolean containsFunctionExpression(String alias) {
    return aliasToFunctionExpressionMap.containsKey(alias);
  }

  public FunctionExpression getFunctionExpressionByAlias(String alias) {
    return aliasToFunctionExpressionMap.get(alias);
  }

  public long getStartTimeMillis() {
    return this.startTimeMillis;
  }

  public long getEndTimeMillis() {
    return this.endTimeMillis;
  }

  public void mapAliasToTimeAggregation(String alias, TimeAggregation timeAggregation) {
    aliasToTimeAggregation.put(alias, timeAggregation);
  }

  public TimeAggregation getTimeAggregationByAlias(String alias) {
    return aliasToTimeAggregation.get(alias);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    QueryRequestContext that = (QueryRequestContext) o;
    return startTimeMillis == that.startTimeMillis &&
        endTimeMillis == that.endTimeMillis &&
        aliasToFunctionExpressionMap.equals(that.aliasToFunctionExpressionMap) &&
        aliasToTimeAggregation.equals(that.aliasToTimeAggregation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), aliasToFunctionExpressionMap, startTimeMillis, endTimeMillis, aliasToTimeAggregation);
  }
}
