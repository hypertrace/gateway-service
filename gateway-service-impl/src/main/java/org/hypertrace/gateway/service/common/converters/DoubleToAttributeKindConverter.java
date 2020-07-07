package org.hypertrace.gateway.service.common.converters;

import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class DoubleToAttributeKindConverter extends ToAttributeKindConverter<Double> {
  public static DoubleToAttributeKindConverter INSTANCE = new DoubleToAttributeKindConverter();

  private DoubleToAttributeKindConverter() {}

  public Value doConvert(Double value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_INT64:
        valueBuilder.setValueType(ValueType.LONG);
        valueBuilder.setLong(value.longValue());
        return valueBuilder.build();

      case TYPE_DOUBLE:
        valueBuilder.setValueType(ValueType.DOUBLE);
        valueBuilder.setDouble(value);
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
