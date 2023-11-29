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
import org.hypertrace.gateway.service.common.util.EntityTypeServiceV2Client;
import org.junit.jupiter.api.Test;

public class EntityTypeProviderTest {

  private static final String TEST_TENANT_ID = "test-tenant-id";

  @Test
  public void testGetEntityTypes() {
    EntityTypeServiceClient entityTypeServiceClient = mock(EntityTypeServiceClient.class);
    EntityTypeServiceV2Client entityTypeServiceV2Client = mock(EntityTypeServiceV2Client.class);
    RequestContext requestContext = RequestContext.forTenantId(TEST_TENANT_ID);
    when(entityTypeServiceClient.getAllEntityTypes(requestContext))
        .thenReturn(List.of(EntityType.newBuilder().setName("SERVICE").build()));
    when(entityTypeServiceV2Client.getAllEntityTypes(requestContext))
        .thenReturn(
            List.of(
                org.hypertrace.entity.type.service.v2.EntityType.newBuilder()
                    .setName("DOMAIN")
                    .build()));
    EntityTypesProvider entityTypesProvider =
        new EntityTypesProvider(entityTypeServiceClient, entityTypeServiceV2Client);
    Set<String> entityType =
        entityTypesProvider.getEntityTypes(requestContext.buildInternalContextualKey());
    assertEquals(2, entityType.size());
    assertTrue(entityType.contains("SERvice"));
    assertTrue(entityType.contains("domain"));
    verify(entityTypeServiceClient).getAllEntityTypes(requestContext);
  }
}
