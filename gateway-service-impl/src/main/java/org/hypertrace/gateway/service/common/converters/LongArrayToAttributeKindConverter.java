package org.hypertrace.gateway.service.common.converters;

import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class LongArrayToAttributeKindConverter extends ToAttributeKindConverter<List<Long>> {
  public static final LongArrayToAttributeKindConverter INSTANCE =
      new LongArrayToAttributeKindConverter();

  private LongArrayToAttributeKindConverter() {}

  public Value doConvert(
      List<Long> value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value.toString());
        return valueBuilder.build();

      case TYPE_INT64_ARRAY:
        valueBuilder.setValueType(ValueType.LONG_ARRAY);
        valueBuilder.addAllLongArray(value);
        return valueBuilder.build();

      case TYPE_STRING_ARRAY:
        valueBuilder.setValueType(ValueType.STRING_ARRAY);
        List<String> strList = value.stream().map(Object::toString).collect(Collectors.toList());
        valueBuilder.addAllStringArray(strList);
        return valueBuilder.build();

      default:
        break;
    }
    return null;
  }
}
