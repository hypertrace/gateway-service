package org.hypertrace.gateway.service.trace;

import org.hypertrace.core.attribute.service.v1.AttributeScope;

public class TraceScopeConverter {

  public static TraceScope fromAttributeScope(AttributeScope scope) {
    return TraceScope.valueOf(scope.name());
  }

  public static AttributeScope toAttributeScope(TraceScope scope) {
    return AttributeScope.valueOf(scope.name());
  }
}
