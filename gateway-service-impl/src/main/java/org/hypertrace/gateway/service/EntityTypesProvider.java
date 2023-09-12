package org.hypertrace.gateway.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.gateway.service.common.util.EntityTypeServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityTypesProvider {
  private static final Logger LOG = LoggerFactory.getLogger(EntityTypesProvider.class);
  private static final int DEFAULT_CACHE_SIZE = 1024;
  private static final int DEFAULT_EXPIRE_DURATION_MIN = 30; // 30 minutes

  // TenantId to Entity types cache
  private final LoadingCache<ContextualKey<Void>, Set<String>> tenantIdToEntityTypesCache;

  public EntityTypesProvider(EntityTypeServiceClient entityTypeServiceClient) {
    CacheLoader<ContextualKey<Void>, Set<String>> cacheLoader =
        new CacheLoader<>() {
          @Override
          public Set<String> load(@NonNull ContextualKey<Void> contextualKey) {
            Set<String> entityTypesSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            entityTypeServiceClient
                .getAllEntityTypes(contextualKey.getContext())
                .forEach(entityType -> entityTypesSet.add(entityType.getName()));
            return entityTypesSet;
          }
        };

    tenantIdToEntityTypesCache =
        CacheBuilder.newBuilder()
            .maximumSize(DEFAULT_CACHE_SIZE)
            .expireAfterWrite(DEFAULT_EXPIRE_DURATION_MIN, TimeUnit.MINUTES)
            .build(cacheLoader);
  }

  public Set<String> getEntityTypes(ContextualKey<Void> contextualKey) {
    try {
      return tenantIdToEntityTypesCache.get(contextualKey);
    } catch (ExecutionException e) {
      LOG.error(
          String.format(
              "Error retrieving entity types for tenant %s",
              contextualKey.getContext().getTenantId()));
      throw new RuntimeException(
          String.format(
              "Error retrieving Entity types for tenant:%s",
              contextualKey.getContext().getTenantId()));
    }
  }
}
