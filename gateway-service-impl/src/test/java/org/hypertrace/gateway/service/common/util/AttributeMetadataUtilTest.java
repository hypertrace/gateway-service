package org.hypertrace.gateway.service.common.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class AttributeMetadataUtilTest {
  @Mock
  private EntityIdColumnsConfigs entityIdColumnsConfigs;

  @BeforeEach
  public void setup() {
    entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);
  }

  @Test
  public void testIdAttributesNotInEntityIdColumnsConfig() {
    when(entityIdColumnsConfigs.getIdKey("API")).thenReturn(Optional.of("apiId"));

    AttributeMetadataProvider provider = mock(AttributeMetadataProvider.class);
    String entityType = DomainEntityType.SERVICE.name();

    Assertions.assertEquals(
        List.of(),
        AttributeMetadataUtil.getIdAttributeIds(provider, entityIdColumnsConfigs, mock(RequestContext.class), entityType));
  }

  @Test
  public void testApiIdAttributeInEntityIdColumnsConfig() {
    when(entityIdColumnsConfigs.getIdKey("API")).thenReturn(Optional.of("apiId"));
    AttributeMetadataProvider provider = mock(AttributeMetadataProvider.class);
    String entityType = DomainEntityType.API.name();
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API.name()), eq("apiId")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("API.apiId").build()));

    Assertions.assertEquals(
        List.of("API.apiId"),
        AttributeMetadataUtil.getIdAttributeIds(provider, entityIdColumnsConfigs, mock(RequestContext.class), entityType));
  }

  @Test
  public void testKnownEntitiesIdAttributesInEntityIdColumnsConfig() {
    when(entityIdColumnsConfigs.getIdKey("API")).thenReturn(Optional.of("apiId"));
    when(entityIdColumnsConfigs.getIdKey("SERVICE")).thenReturn(Optional.of("id"));
    when(entityIdColumnsConfigs.getIdKey("BACKEND")).thenReturn(Optional.of("id"));

    AttributeMetadataProvider provider = mock(AttributeMetadataProvider.class);
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API.name()), eq("apiId")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("API.apiId").build()));
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.SERVICE.name()), eq("id")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("SERVICE.id").build()));
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.BACKEND.name()), eq("id")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("BACKEND.id").build()));

    Assertions.assertEquals(
        List.of("API.apiId"),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, entityIdColumnsConfigs, mock(RequestContext.class), DomainEntityType.API.name()));
    Assertions.assertEquals(
        List.of("SERVICE.id"),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, entityIdColumnsConfigs, mock(RequestContext.class), DomainEntityType.SERVICE.name()));
    Assertions.assertEquals(
        List.of("BACKEND.id"),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, entityIdColumnsConfigs, mock(RequestContext.class), DomainEntityType.BACKEND.name()));

    // Unsupported entities
    Assertions.assertEquals(
        List.of(),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, entityIdColumnsConfigs, mock(RequestContext.class), DomainEntityType.NAMESPACE.name()));
  }
}
