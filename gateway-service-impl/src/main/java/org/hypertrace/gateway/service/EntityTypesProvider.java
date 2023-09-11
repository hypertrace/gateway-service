package org.hypertrace.gateway.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.entity.type.service.client.EntityTypeServiceClient;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityTypesProvider {
  private static final Logger LOG = LoggerFactory.getLogger(EntityTypesProvider.class);
  private static final int DEFAULT_CACHE_SIZE = 1024;
  private static final int DEFAULT_EXPIRE_DURATION_MIN = 30; // 30 minutes

  // TenantId to Entity types cache
  private final LoadingCache<String, List<String>> tenantIdToEntityTypesCache;

  public EntityTypesProvider(EntityTypeServiceClient entityTypeServiceClient) {
    CacheLoader<String, List<String>> cacheLoader =
        new CacheLoader<>() {
          @Override
          public List<String> load(@NonNull String tenantId) {
            return entityTypeServiceClient.getAllEntityTypes(tenantId).stream()
                .map(EntityType::getName)
                .collect(Collectors.toList());
          }
        };

    tenantIdToEntityTypesCache =
        CacheBuilder.newBuilder()
            .maximumSize(DEFAULT_CACHE_SIZE)
            .expireAfterWrite(DEFAULT_EXPIRE_DURATION_MIN, TimeUnit.MINUTES)
            .build(cacheLoader);
  }

  public List<String> getEntityTypes(String tenantId) {
    if (StringUtils.isBlank(tenantId)) {
      LOG.error("Tenant id not found while trying to fetch entity types");
    }
    try {
      return tenantIdToEntityTypesCache.get(tenantId);
    } catch (ExecutionException e) {
      LOG.error(String.format("Error retrieving entity types for tenant %s", tenantId));
      return Collections.emptyList();
    }
  }
}
