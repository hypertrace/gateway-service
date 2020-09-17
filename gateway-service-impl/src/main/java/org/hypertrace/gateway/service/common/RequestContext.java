package org.hypertrace.gateway.service.common;

import java.util.Map;
import java.util.Objects;

/**
 * Base request context that contains data needed for a particular request during its lifetime. An
 * example is the incoming tenant id and request headers. Extend this class as you would like.
 */
public class RequestContext {
  private final String tenantId;
  private final Map<String, String> headers;

  public RequestContext(String tenantId, Map<String, String> headers) {
    this.tenantId = tenantId;
    this.headers = headers;
  }

  public String getTenantId() {
    return tenantId;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RequestContext that = (RequestContext) o;
    return Objects.equals(tenantId, that.tenantId) &&
        Objects.equals(headers, that.headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, headers);
  }
}
