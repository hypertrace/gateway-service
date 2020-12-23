package org.hypertrace.gateway.service.common.util;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.exp.UnknownScopeAndKeyForAttributeException;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;

/** Utility class for fetching AttributeMetadata */
public class AttributeMetadataUtil {
  private static final String START_TIME_ATTRIBUTE_KEY = "startTime";
  private static final String SPACE_IDS_ATTRIBUTE_KEY = "spaceIds";
  private static final Set<String> RECORD_BASED_SCOPES =
      Set.of(
          AttributeScope.TRACE.name(),
          AttributeScope.API_TRACE.name(),
          AttributeScope.BACKEND_TRACE.name(),
          AttributeScope.EVENT.name());

  /**
   *  This method will return an empty list for unsupported entities.
   *  If you need to support a new entity type, add it to the
   *    application.conf eg. For SERVICE id the config under entity.idcolumn.config is
   *      {
   *       scope = SERVICE
   *       key = id
   *     },
   * @param attributeMetadataProvider
   * @param entityIdColumnsConfigs
   * @param requestContext
   * @param entityType
   * @return List of columns(AttributeMetadata ids) used to identify the id of the entity.
   */
  public static List<String> getIdAttributeIds(
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      RequestContext requestContext,
      String entityType) {
    return entityIdColumnsConfigs.getIdKey(entityType)
        .stream()
        .map(idKey -> attributeMetadataProvider.getAttributeMetadata(requestContext, entityType, idKey))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(AttributeMetadata::getId)
        .collect(Collectors.toList());
  }

  public static String getTimestampAttributeId(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      String attributeScope) {
    String key = getStartTimeAttributeKeyName(attributeScope);
    AttributeMetadata timeId =
        attributeMetadataProvider
            .getAttributeMetadata(requestContext, attributeScope, key)
            .orElseThrow(() -> new UnknownScopeAndKeyForAttributeException(attributeScope, key));
    return timeId.getId();
  }

  private static String getStartTimeAttributeKeyName(String attributeScope) {
    return Optional.ofNullable(TimestampConfigs.getTimestampColumn(attributeScope))
        .orElse(START_TIME_ATTRIBUTE_KEY);
  }


  public static String getSpaceAttributeId(
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext,
      String attributeScope) {
    // Record scopes have their own space attribute
    if (RECORD_BASED_SCOPES.contains(attributeScope)) {
      return attributeMetadataProvider
          .getAttributeMetadata(requestContext, attributeScope, SPACE_IDS_ATTRIBUTE_KEY)
          .orElseThrow()
          .getId();
    }
    // Interactions have two space attributes, and is handled directly by interaction code
    if (AttributeScope.INTERACTION.equals(attributeScope)) {
      throw new RuntimeException("Interaction space attribute must disambiguate between caller and callee");
    }
    // Every other scope is an aggregate of spans
    return attributeMetadataProvider
        .getAttributeMetadata(requestContext, AttributeScope.EVENT.name(), SPACE_IDS_ATTRIBUTE_KEY)
        .orElseThrow()
        .getId();
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
