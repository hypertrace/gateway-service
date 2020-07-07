package org.hypertrace.gateway.service.common.converters;

import java.util.List;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public class BooleanArrayToAttributeKindConverter extends ToAttributeKindConverter<List<Boolean>> {
  public static final BooleanArrayToAttributeKindConverter INSTANCE =
      new BooleanArrayToAttributeKindConverter();

  private BooleanArrayToAttributeKindConverter() {}

  public Value doConvert(
      List<Boolean> value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value.toString());
        return valueBuilder.build();

      case TYPE_BOOL_ARRAY:
        valueBuilder.setValueType(ValueType.BOOLEAN_ARRAY);
        valueBuilder.addAllBooleanArray(value);
        return valueBuilder.build();

      default:
        break;
    }
    return null;
  }
}
