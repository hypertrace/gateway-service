package org.hypertrace.gateway.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.gateway.service.common.util.EntityTypeServiceClient;
import org.junit.jupiter.api.Test;

public class EntityTypeProviderTest {

  private static final String TEST_TENANT_ID = "test-tenant-id";

  @Test
  public void testGetEntityTypes() {
    EntityTypeServiceClient entityTypeServiceClient = mock(EntityTypeServiceClient.class);
    RequestContext requestContext = RequestContext.forTenantId(TEST_TENANT_ID);
    when(entityTypeServiceClient.getAllEntityTypes(requestContext))
        .thenReturn(List.of(EntityType.newBuilder().setName("SERVICE").build()));
    EntityTypesProvider entityTypesProvider = new EntityTypesProvider(entityTypeServiceClient);
    Set<String> entityType =
        entityTypesProvider.getEntityTypes(requestContext.buildInternalContextualKey());
    assertEquals(1, entityType.size());
    assertTrue(entityType.contains("SERvice"));
    verify(entityTypeServiceClient).getAllEntityTypes(requestContext);
  }
}
