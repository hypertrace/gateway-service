package org.hypertrace.gateway.service.common;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AttributeCacheKeyTest {

  @Test
  public void testAttributeCacheKey() {
    RequestContext requestContext1 = buildContext("test-tenant-1", Map.of("a1", "v1", "a2", "v2"));
    RequestContext requestContext2 = buildContext("test-tenant-2", Map.of("a1", "v1", "a2", "v2"));
    RequestContext requestContext3 =
        buildContext("test-tenant-1", Map.of("a10", "v10", "a20", "v20"));
    RequestContext requestContext4 =
        buildContext("test-tenant-2", Map.of("a11", "v11", "a21", "v21"));

    AttributeCacheKey<String> testCacheKey1 =
        new AttributeCacheKey<>(requestContext1, "testDataKey");
    AttributeCacheKey<String> testCacheKey2 =
        new AttributeCacheKey<>(requestContext2, "testDataKey");
    AttributeCacheKey<String> testCacheKey3 =
        new AttributeCacheKey<>(requestContext3, "testDataKey");
    AttributeCacheKey<String> testCacheKey4 =
        new AttributeCacheKey<>(requestContext4, "testDataKey");
    AttributeCacheKey<String> testCacheKey5 =
        new AttributeCacheKey<>(requestContext1, "testDataKey2");

    Assertions.assertEquals(testCacheKey1.getDataKey(), "testDataKey");
    Assertions.assertEquals(
        Map.of("x-tenant-id", "test-tenant-1", "a1", "v1", "a2", "v2"), testCacheKey1.getHeaders());

    Assertions.assertEquals(testCacheKey1, testCacheKey1);
    Assertions.assertEquals(testCacheKey1, testCacheKey3);
    Assertions.assertNotEquals(testCacheKey1, testCacheKey2);
    Assertions.assertNotEquals(testCacheKey1, testCacheKey4);
    Assertions.assertNotEquals(testCacheKey1, testCacheKey5);

    Assertions.assertNotEquals(testCacheKey1, "different object");

    Assertions.assertEquals(testCacheKey1.hashCode(), testCacheKey1.hashCode());
    Assertions.assertEquals(testCacheKey1.hashCode(), testCacheKey3.hashCode());
    Assertions.assertNotEquals(testCacheKey1.hashCode(), testCacheKey2.hashCode());
    Assertions.assertNotEquals(testCacheKey1.hashCode(), testCacheKey4.hashCode());
    Assertions.assertNotEquals(testCacheKey1.hashCode(), testCacheKey5.hashCode());
  }

  private RequestContext buildContext(String tenantId, Map<String, String> headers) {
    org.hypertrace.core.grpcutils.context.RequestContext grpcContext =
        org.hypertrace.core.grpcutils.context.RequestContext.forTenantId(tenantId);
    headers.forEach(grpcContext::add);
    return new RequestContext(grpcContext);
  }
}
