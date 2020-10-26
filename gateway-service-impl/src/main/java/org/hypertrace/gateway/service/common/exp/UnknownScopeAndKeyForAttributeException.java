package org.hypertrace.gateway.service.common.exp;

public class UnknownScopeAndKeyForAttributeException extends RuntimeException {
  public UnknownScopeAndKeyForAttributeException(String scope, String key) {
    super("Unknown AttributeScope=" + scope + " and key=" + key);
  }
}
