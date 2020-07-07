package org.hypertrace.gateway.service.common.converters;

import java.util.List;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class StringArrayToAttributeKindConverter extends ToAttributeKindConverter<List<String>> {
  public static StringArrayToAttributeKindConverter INSTANCE =
      new StringArrayToAttributeKindConverter();

  private StringArrayToAttributeKindConverter() {}

  public Value doConvert(
      List<String> value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value.toString());
        return valueBuilder.build();
      case TYPE_STRING_ARRAY:
        valueBuilder.setValueType(ValueType.STRING_ARRAY);
        valueBuilder.addAllStringArray(value);
        return valueBuilder.build();
      default:
        break;
    }
    return null;
  }
}
