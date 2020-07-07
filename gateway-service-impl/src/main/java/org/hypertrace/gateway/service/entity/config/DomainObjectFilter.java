package org.hypertrace.gateway.service.entity.config;

public class DomainObjectFilter {
  private final String value;

  public DomainObjectFilter(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "DomainObjectFilter{" + "value='" + value + '\'' + '}';
  }
}
