package org.hypertrace.gateway.service.common;

import java.util.Map;
import java.util.Objects;

public class AttributeCacheKey<K> {
  private final RequestContext requestContext;
  private final K dataKey;

  AttributeCacheKey(RequestContext requestContext, K dataKey) {
    this.requestContext = requestContext;
    this.dataKey = dataKey;
  }

  Map<String, String> getHeaders() {
    return this.requestContext.getHeaders();
  }

  K getDataKey() {
    return this.dataKey;
  }

  /**
   * Two AttributeCacheKeys are equal if they have equal dataKeys and equal
   * requestContext.tenantIds. This applies to their hashcodes as well of course.
   *
   * @param other
   * @return
   */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof AttributeCacheKey)) {
      return false;
    }

    // A cache key is equal to the other if the dataKeys are equal and the tenantIds in the request
    // contexts are equal.
    return this.dataKey.equals(((AttributeCacheKey) other).dataKey)
        && this.requestContext
            .getTenantId()
            .equals(((AttributeCacheKey) other).requestContext.getTenantId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.dataKey, this.requestContext.getTenantId());
  }

  @Override
  public String toString() {
    return "AttributeCacheKey{dataKey=" + dataKey + '}';
  }
}
