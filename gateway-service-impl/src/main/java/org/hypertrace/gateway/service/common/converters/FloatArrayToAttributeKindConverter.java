package org.hypertrace.gateway.service.common.converters;

import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class FloatArrayToAttributeKindConverter extends ToAttributeKindConverter<List<Float>> {
  public static final FloatArrayToAttributeKindConverter INSTANCE =
      new FloatArrayToAttributeKindConverter();

  private FloatArrayToAttributeKindConverter() {}

  public Value doConvert(
      List<Float> value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value.toString());
        return valueBuilder.build();

      case TYPE_DOUBLE_ARRAY:
        valueBuilder.setValueType(ValueType.DOUBLE_ARRAY);
        List<Double> list = value.stream().map(Float::doubleValue).collect(Collectors.toList());
        valueBuilder.addAllDoubleArray(list);
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
