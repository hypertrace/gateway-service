package org.hypertrace.gateway.service.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hypertrace.gateway.service.v1.common.Expression;

/**
 * Base request context that contains data needed for a particular request during its lifetime. An
 * example is the incoming tenant id and request headers. Extend this class as you would like.
 */
public class RequestContext {
  private final String tenantId;
  private final Map<String, String> headers;
  private final List<Expression> entityLabelExpressions;

  public RequestContext(String tenantId, Map<String, String> headers) {
    this.tenantId = tenantId;
    this.headers = headers;
    this.entityLabelExpressions = new ArrayList<>();
  }

  public String getTenantId() {
    return tenantId;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void addEntityLabelExpression(Expression expression) {
    entityLabelExpressions.add(expression);
  }

  public List<Expression> getEntityLabelExpressions() {
    return entityLabelExpressions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RequestContext that = (RequestContext) o;
    return Objects.equals(tenantId, that.tenantId) &&
        Objects.equals(headers, that.headers) &&
        Objects.equals(entityLabelExpressions, that.entityLabelExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, headers, entityLabelExpressions);
  }
}
