package org.hypertrace.gateway.service.common.converters;

import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class TimestampToAttributeKindConverter extends ToAttributeKindConverter<Long> {
  public static TimestampToAttributeKindConverter INSTANCE =
      new TimestampToAttributeKindConverter();

  private TimestampToAttributeKindConverter() {}

  public Value doConvert(Long value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_TIMESTAMP:
        valueBuilder.setValueType(ValueType.TIMESTAMP);
        valueBuilder.setTimestamp(value);
        return valueBuilder.build();
      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value.toString());
        return valueBuilder.build();
      case TYPE_INT64:
        valueBuilder.setValueType(ValueType.LONG);
        valueBuilder.setLong(value);
        return valueBuilder.build();
      default:
        break;
    }
    return null;
  }
}
