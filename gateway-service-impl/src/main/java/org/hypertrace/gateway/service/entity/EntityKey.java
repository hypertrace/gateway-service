package org.hypertrace.gateway.service.entity;

import java.util.List;
import java.util.Objects;

/** Composite Key that acts as an identifier for an Entity */
public class EntityKey {
  private static final String DELIMITER = ":::";

  private final List<String> attributes;

  private EntityKey(List<String> attributes) {
    this.attributes = attributes;
  }

  public static EntityKey of(String... attributes) {
    return new EntityKey(List.of(attributes));
  }

  public static EntityKey from(String str) {
    return of(str.split(DELIMITER));
  }

  public List<String> getAttributes() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityKey that = (EntityKey) o;
    return Objects.equals(attributes, that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes);
  }

  @Override
  public String toString() {
    return String.join(DELIMITER, attributes);
  }

  public int size() {
    return attributes.size();
  }

  public String get(int i) {
    return attributes.get(i);
  }
}
