package org.hypertrace.gateway.service.common.converters;

import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class NullStringToAttributeKindConverter extends ToAttributeKindConverter<String> {

  public static final NullStringToAttributeKindConverter INSTANCE =
      new NullStringToAttributeKindConverter();

  private NullStringToAttributeKindConverter() {}

  public Value doConvert(String value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value);
        return valueBuilder.build();
      default:
        break;
    }
    return null;
  }
}
