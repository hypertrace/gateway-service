package org.hypertrace.gateway.service.common.exp;

import org.hypertrace.core.attribute.service.v1.AttributeScope;

public class UnknownScopeAndKeyForAttributeException extends RuntimeException {
  public UnknownScopeAndKeyForAttributeException(AttributeScope scope, String key) {
    super("Unkonwn AttributeScope=" + scope.name() + " and key=" + key);
  }
}
