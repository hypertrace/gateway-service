package org.hypertrace.gateway.service.common.util;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.exp.UnknownScopeAndKeyForAttributeException;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfig;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.entity.config.DomainObjectMapping;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;

/** Utility class for fetching AttributeMetadata */
public class AttributeMetadataUtil {
  private static final String START_TIME_ATTRIBUTE_KEY = "startTime";
  private static final String SPACE_IDS_ATTRIBUTE_KEY = "spaceIds";

  /**
   * Returns a map of domain object id to DomainObjectMapping with their filter
   * {
   *   scope = EVENT
   *   key = id
   *   mapping = [
   *     {
   *       scope = SERVICE
   *       key = id
   *     },
   *     {
   *       scope = API
   *       key = isExternal
   *       filter {
   *         value = true
   *       }
   *     }
   *   ]
   * }
   *
   * will return
   * EVENT.id -> [
   *    <"SERVICE", "id", null>,
   *    <"API", "isExternal", DomainObjectFilter{value = true}>
   * ]
   */
  public static Map<String, List<DomainObjectMapping>> getAttributeIdMappings(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      String entityType) {
    Optional<DomainObjectConfig> domainObjectConfig =
        DomainObjectConfigs.getDomainObjectConfig(entityType);

    // Return a map of source attribute id to target attribute ids by mapping the attribute keys in
    // DomainObjectConfig to its corresponding id
    return domainObjectConfig
        .map(
            objectConfig ->
                objectConfig.getAttributeMappings().entrySet().stream()
                    .collect(
                        Collectors.toMap(
                            entry ->
                                getAttributeMetadata(
                                        attributeMetadataProvider,
                                        requestContext,
                                        entry.getKey().getScope(),
                                        entry.getKey().getKey())
                                    .getId(),
                            Map.Entry::getValue)))
        // Return empty map if DomainObjectConfig doesn't exist for the entityType
        .orElse(Collections.emptyMap());
  }

  /**
   *  This method will return an empty list for unsupported entities.
   *  If you need to support a new entity type, add it to the
   *    application.conf eg. For SERVICE id the config under domainobject.config is
   *      {
   *       scope = SERVICE
   *       key = id
   *       primaryKey = true
   *       mapping = [
   *         {
   *           scope = SERVICE
   *           key = id
   *         }
   *       ]
   *     },
   * @param attributeMetadataProvider
   * @param requestContext
   * @param entityType
   * @return List of columns(AttributeMetadata ids) used to identify the id of the entity.
   */
  public static List<String> getIdAttributeIds(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      String entityType) {
    Optional<DomainObjectConfig> domainObjectConfig =
        DomainObjectConfigs.getDomainObjectConfig(entityType);
    if (domainObjectConfig.isEmpty() || domainObjectConfig.get().getIdAttributes() == null) {
      return Collections.emptyList();
    }

    // If DomainObjectConfig found for an entity type fetch the id attributes from it
    DomainObjectConfig objectConfig = domainObjectConfig.get();
    return objectConfig.getIdAttributes().stream()
        .map(
            domainObjectMapping ->
                attributeMetadataProvider.getAttributeMetadata(
                    requestContext, domainObjectMapping.getScope(), domainObjectMapping.getKey()))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(AttributeMetadata::getId)
        .collect(Collectors.toList());
  }

  public static String getTimestampAttributeId(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      String attributeScope) {
    Map<String, List<DomainObjectMapping>> attributeIdMappings =
        getAttributeIdMappings(attributeMetadataProvider, requestContext, attributeScope);

    String key = getStartTimeAttributeKeyName(attributeScope);
    AttributeMetadata timeId =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext, attributeScope, key)
            .orElseThrow(() -> new UnknownScopeAndKeyForAttributeException(attributeScope, key));
    List<DomainObjectMapping> mappedIds = attributeIdMappings.get(timeId.getId());
    if (mappedIds != null && mappedIds.size() == 1) {
      return AttributeMetadataUtil.getAttributeMetadata(
              attributeMetadataProvider, requestContext, mappedIds.get(0))
          .getId();
    } else {
      return timeId.getId();
    }
  }

  private static String getStartTimeAttributeKeyName(String attributeScope) {
    return Optional.ofNullable(TimestampConfigs.getTimestampColumn(attributeScope))
        .orElse(START_TIME_ATTRIBUTE_KEY);
  }


  public static String getSpaceAttributeId(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      String attributeScope) {
    // Trace types have a space ID attribute
    if (List.of(
            AttributeScope.TRACE.name(),
            AttributeScope.API_TRACE.name(),
            AttributeScope.BACKEND_TRACE.name())
        .contains(attributeScope)) {
      return attributeMetadataProvider
          .getAttributeMetadata(requestContext, attributeScope, SPACE_IDS_ATTRIBUTE_KEY)
          .orElseThrow()
          .getId();
    }
    if (AttributeScope.INTERACTION.equals(attributeScope)) {
      throw new RuntimeException("Interaction space attribute must disambiguate between caller and callee");
    }
    // Everything else is based off the span space
    return attributeMetadataProvider
        .getAttributeMetadata(requestContext, AttributeScope.EVENT.name(), SPACE_IDS_ATTRIBUTE_KEY)
        .orElseThrow()
        .getId();
  }

  public static AttributeMetadata getAttributeMetadata(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      DomainObjectMapping mapping) {
    return getAttributeMetadata(
        attributeMetadataProvider, requestContext, mapping.getScope(), mapping.getKey());
  }

  private static AttributeMetadata getAttributeMetadata(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      String scope,
      String key) {
    return attributeMetadataProvider
        .getAttributeMetadata(requestContext, scope, key)
        .orElseThrow(() -> new UnknownScopeAndKeyForAttributeException(scope, key));
  }
}
