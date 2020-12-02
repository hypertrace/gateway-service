package org.hypertrace.gateway.service.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeMetadataFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caches the attribute metadata locally to avoid fetching it over and over. The cache is keyed on
 * the tenantId in the requestContext and the AttributeMetadata field(s) we are concerned with.
 */
public class AttributeMetadataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(AttributeMetadataProvider.class);

  private static final int DEFAULT_CACHE_SIZE = 4096;
  private static final int DEFAULT_EXPIRE_DURATION_MIN = 60; // 60 min
  // AttributeScope to Map<id, AttributeMetadata>
  private final LoadingCache<AttributeCacheKey<String>, Map<String, AttributeMetadata>>
      scopeToMapOfIdAndAttributeMetadataCache;
  // Pair<AttributeScope, key> to AttributeMetadata
  private final LoadingCache<AttributeCacheKey<Map.Entry<String, String>>, AttributeMetadata>
      scopeAndKeyToAttrMetadataCache;

  public AttributeMetadataProvider(AttributeServiceClient attributesServiceClient) {
    CacheLoader<AttributeCacheKey<String>, Map<String, AttributeMetadata>> cacheLoader =
        new CacheLoader<>() {
          @Override
          public Map<String, AttributeMetadata> load(
              AttributeCacheKey<String> scopeBasedCacheKey) {
            Iterator<AttributeMetadata> attributeMetadataIterator =
                attributesServiceClient.findAttributes(
                    scopeBasedCacheKey.getHeaders(),
                    AttributeMetadataFilter.newBuilder()
                        .addScopeString(scopeBasedCacheKey.getDataKey())
                        .build());

            Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
            attributeMetadataIterator.forEachRemaining(
                metadata -> attributeMetadataMap.put(metadata.getId(), metadata));

            return Collections.unmodifiableMap(attributeMetadataMap);
          }
        };

    CacheLoader<AttributeCacheKey<Map.Entry<String, String>>, AttributeMetadata>
        scopeAndAliasToAttrMetadataCacheLoader =
        new CacheLoader<>() {
          @Override
          public AttributeMetadata load(
              AttributeCacheKey<Map.Entry<String, String>>
                  scopeAndKeyPairBasedCacheKey) {
            Iterator<AttributeMetadata> attributeMetadataIterator =
                attributesServiceClient.findAttributes(
                    scopeAndKeyPairBasedCacheKey.getHeaders(),
                    AttributeMetadataFilter.newBuilder()
                        .addScopeString(scopeAndKeyPairBasedCacheKey.getDataKey().getKey())
                        .build());

            while (attributeMetadataIterator.hasNext()) {
              AttributeMetadata metadata = attributeMetadataIterator.next();
              if (metadata
                  .getKey()
                  .equals(scopeAndKeyPairBasedCacheKey.getDataKey().getValue())) {
                return metadata;
              }
            }
            return null;
          }
        };

    scopeToMapOfIdAndAttributeMetadataCache =
        CacheBuilder.newBuilder()
            .maximumSize(DEFAULT_CACHE_SIZE)
            .expireAfterWrite(DEFAULT_EXPIRE_DURATION_MIN, TimeUnit.MINUTES)
            .build(cacheLoader);
    scopeAndKeyToAttrMetadataCache =
        CacheBuilder.newBuilder()
            .maximumSize(DEFAULT_CACHE_SIZE)
            .expireAfterWrite(DEFAULT_EXPIRE_DURATION_MIN, TimeUnit.MINUTES)
            .build(scopeAndAliasToAttrMetadataCacheLoader);
  }

  public Map<String, AttributeMetadata> getAttributesMetadata(
      RequestContext requestContext, String attributeScope) {
    try {
      AttributeCacheKey<String> cacheKey = new AttributeCacheKey<>(requestContext, attributeScope);
      return scopeToMapOfIdAndAttributeMetadataCache.get(cacheKey);
    } catch (ExecutionException e) {
      LOG.error(String.format("Error retrieving attribute metadata for %s", attributeScope), e);
      throw new RuntimeException(e);
    }
  }

  public Optional<AttributeMetadata> getAttributeMetadata(
      RequestContext requestContext, String scope, String key) {
    try {
      AttributeCacheKey<Map.Entry<String, String>> cacheKey =
          new AttributeCacheKey<>(requestContext, new AbstractMap.SimpleEntry<>(scope, key));
      return Optional.ofNullable(scopeAndKeyToAttrMetadataCache.get(cacheKey));
    } catch (ExecutionException e) {
      LOG.error("Error retrieving AttributeMetadata for scope:{}, key:{}", scope, key);
      throw new RuntimeException(
          String.format("Error retrieving AttributeMetadata for scope:%s, key:%s", scope, key));
    }
  }
}
