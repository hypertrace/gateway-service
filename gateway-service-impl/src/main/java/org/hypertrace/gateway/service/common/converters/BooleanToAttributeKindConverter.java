package org.hypertrace.gateway.service.common.converters;

import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class BooleanToAttributeKindConverter extends ToAttributeKindConverter<Boolean> {
  public static BooleanToAttributeKindConverter INSTANCE = new BooleanToAttributeKindConverter();

  private BooleanToAttributeKindConverter() {}

  public Value doConvert(Boolean value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_BOOL:
        valueBuilder.setValueType(ValueType.BOOL);
        valueBuilder.setBoolean(value);
        return valueBuilder.build();

      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value.toString());
        return valueBuilder.build();

      default:
        break;
    }
    return null;
  }
}
