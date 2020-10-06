package org.hypertrace.gateway.service.common.transformer;

import org.hypertrace.core.attribute.service.v1.AttributeScope;

public class ScopeEntityLabelsMapping {
  /**
   *
   * {
   *   scope = API
   *   labelsColumnName = API.labels
   *   entityIdKey = id
   *   mapping = {
   *     scope = API
   *     labelsKey = labels
   *     entityIdKey = id
   *   }
   * },
   */

  private final AttributeScope scope;
  private final String labelsColumnName;
  private final String entityIdKey;
  private final AttributeScope mappedScope;
  private final String mappedLabelsKey;
  private final String mappedEntityIdKey;

  public ScopeEntityLabelsMapping(AttributeScope scope,
                                  String labelsColumnName,
                                  String entityIdKey,
                                  AttributeScope mappedScope,
                                  String mappedLabelsKey,
                                  String mappedEntityIdKey) {
    this.scope = scope;
    this.labelsColumnName = labelsColumnName;
    this.entityIdKey = entityIdKey;
    this.mappedScope = mappedScope;
    this.mappedLabelsKey = mappedLabelsKey;
    this.mappedEntityIdKey = mappedEntityIdKey;
  }

  public AttributeScope getScope() {
    return scope;
  }

  public String getLabelsColumnName() {
    return labelsColumnName;
  }

  public String getEntityIdKey() {
    return entityIdKey;
  }

  public AttributeScope getMappedScope() {
    return mappedScope;
  }

  public String getMappedLabelsKey() {
    return mappedLabelsKey;
  }

  public String getMappedEntityIdKey() {
    return mappedEntityIdKey;
  }
}
