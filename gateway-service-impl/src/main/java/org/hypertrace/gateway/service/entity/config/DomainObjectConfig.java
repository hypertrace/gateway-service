package org.hypertrace.gateway.service.entity.config;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;

/**
 * Class that encapsulates the configuration for a Domain Object
 *
 * <p>{@link #type} is mapped to {@link EntitiesRequest#getEntityType()} {@link #attributeMappings}
 * are the mappings from one Attribute to one or many other Attributes
 */
public class DomainObjectConfig {
  private final String type;
  private final Map<DomainAttribute, List<DomainObjectMapping>> attributeMappings;

  public DomainObjectConfig(
      String type, Map<DomainAttribute, List<DomainObjectMapping>> attributeMappings) {
    this.type = type;
    this.attributeMappings = attributeMappings;
  }

  public String getType() {
    return type;
  }

  public Map<DomainAttribute, List<DomainObjectMapping>> getAttributeMappings() {
    return attributeMappings;
  }

  public List<DomainObjectMapping> getIdAttributes() {
    // We expect only one primary key for each domain object.
    return attributeMappings.entrySet().stream()
        .filter(e -> e.getKey().isPrimaryKey)
        .findFirst()
        .map(Map.Entry::getValue)
        .orElse(Collections.emptyList());
  }

  public static class DomainAttribute {
    private final AttributeScope scope;
    private final String key;
    private final boolean isPrimaryKey;

    public DomainAttribute(AttributeScope scope, String key, boolean isPrimaryKey) {
      this.scope = scope;
      this.key = key;
      this.isPrimaryKey = isPrimaryKey;
    }

    public AttributeScope getScope() {
      return scope;
    }

    public String getKey() {
      return key;
    }

    public boolean isPrimaryKey() {
      return isPrimaryKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DomainAttribute that = (DomainAttribute) o;

      if (isPrimaryKey != that.isPrimaryKey) return false;
      if (scope != that.scope) return false;
      return key.equals(that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scope, key, isPrimaryKey);
    }
  }
}
