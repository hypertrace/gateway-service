package org.hypertrace.gateway.service.common.converters;

import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;

public abstract class ToAttributeKindConverter<T> {

  public Value convert(T value, AttributeKind attributeKind) {
    org.hypertrace.gateway.service.v1.common.Value.Builder valueBuilder =
        org.hypertrace.gateway.service.v1.common.Value.newBuilder();

    return doConvert(value, attributeKind, valueBuilder);
  }

  protected abstract Value doConvert(
      T value, AttributeKind attributeKind, Value.Builder valueBuilder);
}
