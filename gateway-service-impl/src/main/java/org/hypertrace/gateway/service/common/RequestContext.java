package org.hypertrace.gateway.service.common;

import java.util.Map;
import java.util.Objects;

/**
 * Base request context that contains data needed for a particular request during its lifetime. An
 * example is the incoming tenant id and request headers. Extend this class as you would like.
 */
public class RequestContext {
  private org.hypertrace.core.grpcutils.context.RequestContext grpcContext;

  public RequestContext(org.hypertrace.core.grpcutils.context.RequestContext grpcContext) {
    this.grpcContext = grpcContext;
  }

  public String getTenantId() {
    return grpcContext.getTenantId().orElseThrow();
  }

  public Map<String, String> getHeaders() {
    return grpcContext.getRequestHeaders();
  }

  public org.hypertrace.core.grpcutils.context.RequestContext getGrpcContext() {
    return grpcContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RequestContext that = (RequestContext) o;
    return Objects.equals(this.getHeaders(), that.getHeaders());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getHeaders());
  }
}
