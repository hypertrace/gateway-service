package org.hypertrace.gateway.service.common;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

public class AttributeMetadataProviderTest {

  @Test
  public void testGetAttributeMetadata() {
    AttributeServiceClient attributesServiceClient = mock(AttributeServiceClient.class);
    List<AttributeMetadata> attributesList1 =
        List.of(
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.API.name())
                .setKey("apiName")
                .setId("API.apiName")
                .build(),
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.API.name())
                .setKey("apiId")
                .setId("API.apiId")
                .build(),
            AttributeMetadata.newBuilder()
                .setScopeString(AttributeScope.API.name())
                .setKey("serviceId")
                .setId("API.serviceId")
                .build());
    when(attributesServiceClient.findAttributes(
        eq(Map.of("test-header-key", "test-header-value")),
        eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList1.iterator());

    AttributeMetadataProvider attributeMetadataProvider =
        new AttributeMetadataProvider(attributesServiceClient);

    RequestContext requestContext1 =
        new RequestContext("test-tenant-id", Map.of("test-header-key", "test-header-value"));

    AttributeMetadata attributeMetadata =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext1, AttributeScope.API.name(), "apiName")
            .get();
    Assertions.assertEquals(
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("apiName")
            .setId("API.apiName")
            .build(),
        attributeMetadata);
    attributeMetadata =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext1, AttributeScope.API.name(), "apiName")
            .get();
    Assertions.assertEquals(
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("apiName")
            .setId("API.apiName")
            .build(),
        attributeMetadata);
    attributeMetadata =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext1, AttributeScope.API.name(), "apiId")
            .get();
    Assertions.assertEquals(
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("apiId")
            .setId("API.apiId")
            .build(),
        attributeMetadata);

    // Since the cache keys on (scope, key), we expect 2 calls to attr svc. One for (Api, name) and
    // the other for (Api, id)
    verify(attributesServiceClient, times(2))
        .findAttributes(
            eq(Map.of("test-header-key", "test-header-value")),
            eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build()));

    // Different jwt token
    List<AttributeMetadata> attributesList2 =
        List.of(
            AttributeMetadata.newBuilder().setScopeString(AttributeScope.API.name()).setKey("name2").build(),
            AttributeMetadata.newBuilder().setScopeString(AttributeScope.API.name()).setKey("apiId").build());
    when(attributesServiceClient.findAttributes(
        eq(Map.of("test-header-key", "test-header-value-2")),
        eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList2.iterator());

    RequestContext requestContext2 =
        new RequestContext(
            "test-tenant-id-2", Map.of("test-header-key", "test-header-value-2"));

    attributeMetadata =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext2, AttributeScope.API.name(), "apiId")
            .get();
    Assertions.assertEquals(
        AttributeMetadata.newBuilder().setScopeString(AttributeScope.API.name()).setKey("apiId").build(),
        attributeMetadata);
    attributeMetadata =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext2, AttributeScope.API.name(), "apiId")
            .get();
    Assertions.assertEquals(
        AttributeMetadata.newBuilder().setScopeString(AttributeScope.API.name()).setKey("apiId").build(),
        attributeMetadata);
    attributeMetadata =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext2, AttributeScope.API.name(), "name2")
            .get();
    Assertions.assertEquals(
        AttributeMetadata.newBuilder().setScopeString(AttributeScope.API.name()).setKey("name2").build(),
        attributeMetadata);

    // 2 for a different tenant on (Api, id) and (Api, name2)
    verify(attributesServiceClient, times(2))
        .findAttributes(
            eq(Map.of("test-header-key", "test-header-value-2")),
            eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build()));
  }

  @Test
  public void testGetAttributesMetadata() {
    AttributeServiceClient attributesServiceClient = mock(AttributeServiceClient.class);
    AttributeMetadata attributeMetadata1 =
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("apiName")
            .setId("API.apiName")
            .build();
    AttributeMetadata attributeMetadata2 =
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("apiId")
            .setId("API.apiId")
            .build();
    AttributeMetadata attributeMetadata3 =
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("serviceId")
            .setId("API.serviceId")
            .build();
    List<AttributeMetadata> attributesList1 =
        List.of(attributeMetadata1, attributeMetadata2, attributeMetadata3);
    when(attributesServiceClient.findAttributes(
        eq(Map.of("test-header-key", "test-header-value")),
        eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList1.iterator());

    AttributeMetadataProvider attributeMetadataProvider =
        new AttributeMetadataProvider(attributesServiceClient);

    RequestContext requestContext1 =
        new RequestContext("test-tenant-id", Map.of("test-header-key", "test-header-value"));

    Map<String, AttributeMetadata> attributeIdToMetadata =
        attributeMetadataProvider.getAttributesMetadata(requestContext1, AttributeScope.API.name());
    Assertions.assertEquals(
        Map.of(
            "API.apiName",
            attributeMetadata1,
            "API.apiId",
            attributeMetadata2,
            "API.serviceId",
            attributeMetadata3),
        attributeIdToMetadata);
    attributeIdToMetadata =
        attributeMetadataProvider.getAttributesMetadata(requestContext1, AttributeScope.API.name());
    Assertions.assertEquals(
        Map.of(
            "API.apiName",
            attributeMetadata1,
            "API.apiId",
            attributeMetadata2,
            "API.serviceId",
            attributeMetadata3),
        attributeIdToMetadata);

    // Since the cache keys on scope, we expect 1 calls to attr svc. For the our second call the
    // value should be cached.
    verify(attributesServiceClient, times(1))
        .findAttributes(
            eq(Map.of("test-header-key", "test-header-value")),
            eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build()));

    // Different jwt token
    AttributeMetadata attributeMetadata4 =
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("name2")
            .setId("API.name2")
            .build();
    AttributeMetadata attributeMetadata5 =
        AttributeMetadata.newBuilder()
            .setScopeString(AttributeScope.API.name())
            .setKey("apiId")
            .setId("API.apiId")
            .build();
    List<AttributeMetadata> attributesList2 = List.of(attributeMetadata4, attributeMetadata5);
    when(attributesServiceClient.findAttributes(
        eq(Map.of("test-header-key", "test-header-value-2")),
        eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build())))
        .thenAnswer((Answer<Iterator<AttributeMetadata>>) invocation -> attributesList2.iterator());

    RequestContext requestContext2 =
        new RequestContext(
            "test-tenant-id-2", Map.of("test-header-key", "test-header-value-2"));

    attributeIdToMetadata =
        attributeMetadataProvider.getAttributesMetadata(requestContext2, AttributeScope.API.name());
    Assertions.assertEquals(
        Map.of("API.name2", attributeMetadata4, "API.apiId", attributeMetadata5),
        attributeIdToMetadata);
    attributeIdToMetadata =
        attributeMetadataProvider.getAttributesMetadata(requestContext2, AttributeScope.API.name());
    Assertions.assertEquals(
        Map.of("API.name2", attributeMetadata4, "API.apiId", attributeMetadata5),
        attributeIdToMetadata);

    // 1 call to attribute service for a different tenant
    verify(attributesServiceClient, times(1))
        .findAttributes(
            eq(Map.of("test-header-key", "test-header-value-2")),
            eq(AttributeMetadataFilter.newBuilder().addScopeString(AttributeScope.API.name()).build()));
  }
}
