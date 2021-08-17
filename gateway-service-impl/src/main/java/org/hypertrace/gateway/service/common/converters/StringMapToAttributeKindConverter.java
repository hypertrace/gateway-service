package org.hypertrace.gateway.service.common.converters;

import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.Value.Builder;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class StringMapToAttributeKindConverter
    extends ToAttributeKindConverter<Map<String, String>> {
  public static final StringMapToAttributeKindConverter INSTANCE =
      new StringMapToAttributeKindConverter();

  private StringMapToAttributeKindConverter() {}

  @Override
  protected Value doConvert(
      Map<String, String> value, AttributeKind attributeKind, Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_STRING:
        valueBuilder.setString(value.toString());
        valueBuilder.setValueType(ValueType.STRING);
        return valueBuilder.build();
      case TYPE_STRING_MAP:
        valueBuilder.putAllStringMap(value);
        valueBuilder.setValueType(ValueType.STRING_MAP);
        return valueBuilder.build();
      default:
        break;
    }
    return null;
  }
}
