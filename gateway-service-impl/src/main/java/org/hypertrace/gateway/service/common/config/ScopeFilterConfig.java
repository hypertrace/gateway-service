package org.hypertrace.gateway.service.common.config;

import java.util.List;

public class ScopeFilterConfig {
  private final String scope;
  private final List<ScopeFilter> scopeFilters;

  public ScopeFilterConfig(String scope, List<ScopeFilter> scopeFilters) {
    this.scope = scope;
    this.scopeFilters = scopeFilters;
  }

  public String getScope() {
    return scope;
  }

  public List<ScopeFilter> getScopeFilters() {
    return scopeFilters;
  }
}
