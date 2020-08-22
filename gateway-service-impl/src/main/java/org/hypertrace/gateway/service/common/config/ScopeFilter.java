package org.hypertrace.gateway.service.common.config;

import org.hypertrace.gateway.service.v1.common.Operator;

public class ScopeFilter<T> {
  private final String scope;
  private final String key;
  private final Operator operator;
  private final T filterValue;

  public ScopeFilter(String scope, String key, Operator operator, T filterValue) {
    this.scope = scope;
    this.key = key;
    this.operator = operator;
    this.filterValue = filterValue;
  }

  public String getScope() {
    return scope;
  }

  public String getKey() {
    return key;
  }

  public Operator getOperator() {
    return operator;
  }

  public T getFilterValue() {
    return filterValue;
  }
}
