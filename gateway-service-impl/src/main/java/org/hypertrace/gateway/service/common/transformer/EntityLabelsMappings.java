package org.hypertrace.gateway.service.common.transformer;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * labelsConfig = [
 *   {
 *     scope = API
 *     labelsColumnName = API.labels
 *     entityIdKey = id
 *     mapping = {
 *       scope = API
 *       labelsKey = labels
 *       entityIdKey = id
 *     }
 *   },
 *   {
 *     scope = API_TRACE
 *     labelsColumnName = API_TRACE.apiLabels
 *     entityIdKey = apiId
 *     mapping = {
 *       scope = API
 *       labelsKey = labels
 *       entityIdKey = id
 *     }
 *   },
 * ]
 */

public class EntityLabelsMappings {
  private static final Logger LOG = LoggerFactory.getLogger(EntityLabelsMappings.class);

  private static final String LABELS_CONFIG = "labelsConfig";
  private static final String SCOPE_CONFIG = "scope";
  // Note this is the full labels column name: scope.key
  private static final String LABELS_COLUMN_NAME_CONFIG = "labelsColumnName";
  private static final String MAPPING_CONFIG = "mapping";
  private static final String LABELS_KEY_CONFIG = "labelsKey";
  private static final String ENTITY_ID_KEY_CONFIG = "entityIdKey";

  private final Map<String, ScopeEntityLabelsMapping> entityLabelsMappingsByLabelColumnName = new HashMap<>();

  public EntityLabelsMappings(Config config) {
    if (!config.hasPath(LABELS_CONFIG)) {
      return;
    }

    List<? extends Config> configList = config.getConfigList(LABELS_CONFIG);
    for (Config subConfig : configList) {
      addEntityLabelsConfig(subConfig);
    }
  }

  private void addEntityLabelsConfig(Config config) {
    try {
      AttributeScope attributeScope = AttributeScope.valueOf(config.getString(SCOPE_CONFIG));
      // Note this is the full labels column name: scope.key
      String labelsColumnName = config.getString(LABELS_COLUMN_NAME_CONFIG);
      String entityIdKey = config.getString(ENTITY_ID_KEY_CONFIG);
      Config labelsMapping = config.getConfig(MAPPING_CONFIG);

      AttributeScope mappedScope = AttributeScope.valueOf(labelsMapping.getString(SCOPE_CONFIG));
      String mappedLabelsKey = labelsMapping.getString(LABELS_KEY_CONFIG);
      String mappedEntityIdKey = labelsMapping.getString(ENTITY_ID_KEY_CONFIG);

      ScopeEntityLabelsMapping scopeEntityLabelsMapping = new ScopeEntityLabelsMapping(attributeScope,
          labelsColumnName, entityIdKey, mappedScope, mappedLabelsKey, mappedEntityIdKey);

      entityLabelsMappingsByLabelColumnName.put(labelsColumnName, scopeEntityLabelsMapping);
    } catch (Exception ex) {
      LOG.error("Exception while reading scope filter configs for config: {}", config, ex);
    }
  }

  public boolean isEntityLabelsColumn(String columnName) {
    return entityLabelsMappingsByLabelColumnName.containsKey(columnName);
  }

  public Optional<ScopeEntityLabelsMapping> getEntityLabelsMapping(String columnName) {
    if (isEntityLabelsColumn(columnName)) {
      return Optional.of(entityLabelsMappingsByLabelColumnName.get(columnName));
    }

    return Optional.empty();
  }
}
