package org.hypertrace.gateway.service.common.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.common.converters.StringToAttributeKindConverter;
import org.hypertrace.gateway.service.v1.common.Value;
import org.junit.jupiter.api.Test;

public class StringToAttributeKindConverterTest {

  @Test
  public void test_defaultProtoEmptyString_ToNumerical_setToDefault() {
    StringToAttributeKindConverter converter = StringToAttributeKindConverter.INSTANCE;
    Value actualValue =
        converter.doConvert("", AttributeKind.TYPE_DOUBLE, Value.newBuilder());
    assertEquals(0.0d, actualValue.getDouble());

    actualValue =
        converter.doConvert("", AttributeKind.TYPE_DOUBLE, Value.newBuilder());
    assertEquals(0L, actualValue.getLong());
  }

  @Test
  public void test_blankString_ToNumerical_throwsException() {
    StringToAttributeKindConverter converter = StringToAttributeKindConverter.INSTANCE;
    assertThrows(NumberFormatException.class, () ->
        converter.doConvert("      ", AttributeKind.TYPE_DOUBLE, Value.newBuilder()));
    assertThrows(NumberFormatException.class, () ->
        converter.doConvert("      ", AttributeKind.TYPE_INT64, Value.newBuilder()));
  }
}
