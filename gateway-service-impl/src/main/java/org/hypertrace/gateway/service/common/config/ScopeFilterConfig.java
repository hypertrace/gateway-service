package org.hypertrace.gateway.service.common.config;

import java.util.List;
import org.hypertrace.core.attribute.service.v1.AttributeScope;

public class ScopeFilterConfig {
  private final AttributeScope scope;
  private final List<ScopeFilter> scopeFilters;

  public ScopeFilterConfig(AttributeScope scope, List<ScopeFilter> scopeFilters) {
    this.scope = scope;
    this.scopeFilters = scopeFilters;
  }

  public AttributeScope getScope() {
    return scope;
  }

  public List<ScopeFilter> getScopeFilters() {
    return scopeFilters;
  }
}
