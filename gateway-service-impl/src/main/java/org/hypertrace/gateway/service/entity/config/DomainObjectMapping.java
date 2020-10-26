package org.hypertrace.gateway.service.entity.config;

import com.typesafe.config.Config;

public class DomainObjectMapping {
  private static final String VALUE = "value";
  private final String scope;
  private final String key;
  private final DomainObjectFilter filter;

  public DomainObjectMapping(String scope, String key, Config filterConfig) {
    this.scope = scope;
    this.key = key;
    if (filterConfig != null) {
      String filterValue = filterConfig.getString(VALUE);
      this.filter = new DomainObjectFilter(filterValue);
    } else {
      this.filter = null;
    }
  }

  public String getScope() {
    return scope;
  }

  public String getKey() {
    return key;
  }

  public DomainObjectFilter getFilter() {
    return filter;
  }

  @Override
  public String toString() {
    return "DomainObjectMapping{"
        + "scope="
        + scope
        + ", key='"
        + key
        + '\''
        + ", filter="
        + filter
        + '}';
  }
}
