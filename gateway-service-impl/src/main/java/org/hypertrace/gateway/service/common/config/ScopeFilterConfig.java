package org.hypertrace.gateway.service.common.config;

import java.util.List;

public class ScopeFilterConfig {
  private final String scope;
  private final List<ScopeFilter<String>> scopeFilters;

  public ScopeFilterConfig(String scope, List<ScopeFilter<String>> scopeFilters) {
    this.scope = scope;
    this.scopeFilters = scopeFilters;
  }

  public String getScope() {
    return scope;
  }

  public List<ScopeFilter<String>> getScopeFilters() {
    return scopeFilters;
  }
}
