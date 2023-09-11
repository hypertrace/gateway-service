package org.hypertrace.gateway.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.hypertrace.entity.type.service.client.EntityTypeServiceClient;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.junit.jupiter.api.Test;

public class EntityTypeProviderTest {

  private static final String TEST_TENANT_ID = "test-tenant-id";

  @Test
  public void testGetEntityTypes() {
    EntityTypeServiceClient entityTypeServiceClient = mock(EntityTypeServiceClient.class);
    when(entityTypeServiceClient.getAllEntityTypes(TEST_TENANT_ID))
        .thenReturn(List.of(EntityType.newBuilder().setName("SERVICE").build()));
    EntityTypesProvider entityTypesProvider = new EntityTypesProvider(entityTypeServiceClient);
    List<String> entityType = entityTypesProvider.getEntityTypes(TEST_TENANT_ID);
    assertEquals(1, entityType.size());
    assertEquals("SERVICE", entityType.get(0));
    verify(entityTypeServiceClient).getAllEntityTypes(TEST_TENANT_ID);
  }
}
