package org.hypertrace.gateway.service.common.converters;

import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class IntegerToAttributeKindConverter extends ToAttributeKindConverter<Integer> {
  public static IntegerToAttributeKindConverter INSTANCE = new IntegerToAttributeKindConverter();

  private IntegerToAttributeKindConverter() {}

  public Value doConvert(Integer value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_INT64:
        valueBuilder.setValueType(ValueType.LONG);
        valueBuilder.setLong(value.longValue());
        return valueBuilder.build();

      case TYPE_DOUBLE:
        valueBuilder.setValueType(ValueType.DOUBLE);
        valueBuilder.setDouble(value.doubleValue());
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
