package org.hypertrace.gateway.service.common.datafetcher;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Map;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EntityDataServiceEntityFetcherTests {
  private EntityDataServiceEntityFetcher entityDataServiceEntityFetcher;
  private EntityQueryServiceClient entityQueryServiceClient;
  private AttributeMetadataProvider attributeMetadataProvider;
  private static final String TENANT_ID = "tenant-id";

  @BeforeEach
  public void setup() {
    entityQueryServiceClient = mock(EntityQueryServiceClient.class);
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    entityDataServiceEntityFetcher = new EntityDataServiceEntityFetcher(entityQueryServiceClient
        , attributeMetadataProvider);
  }

  @Test
  public void test_getEntitiesAndAggregatedMetrics() {
    assertThrows(UnsupportedOperationException.class, () -> {
      entityDataServiceEntityFetcher.getEntitiesAndAggregatedMetrics(
          new EntitiesRequestContext(TENANT_ID, 0, 1, "API", Map.of()),
          EntitiesRequest.newBuilder().build()
      );
    });
  }

  @Test
  public void test_getTotalEntities() {
    assertThrows(UnsupportedOperationException.class, () -> {
      entityDataServiceEntityFetcher.getTotalEntities(
          new EntitiesRequestContext(TENANT_ID, 0, 1, "API", Map.of()),
          EntitiesRequest.newBuilder().build()
      );
    });
  }

  @Test
  public void test_getAggregatedMetrics() {
    assertThrows(UnsupportedOperationException.class, () -> {
      entityDataServiceEntityFetcher.getAggregatedMetrics(
          new EntitiesRequestContext(TENANT_ID, 0, 1, "API", Map.of()),
          EntitiesRequest.newBuilder().build()
      );
    });
  }

  @Test
  public void test_getTimeAggregatedMetrics() {
    assertThrows(UnsupportedOperationException.class, () -> {
      entityDataServiceEntityFetcher.getTimeAggregatedMetrics(
          new EntitiesRequestContext(TENANT_ID, 0, 1, "API", Map.of()),
          EntitiesRequest.newBuilder().build()
      );
    });
  }
}
