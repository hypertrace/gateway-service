package org.hypertrace.gateway.service.entity;

import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link CompositeValue} */
public class CompositeValueTest {

  @Test
  public void testCompositeValueToFromString() {
    Value value1 = Value.newBuilder().setValueType(ValueType.STRING).setString("abc").build();
    Value value2 = Value.newBuilder().setValueType(ValueType.LONG).setLong(5).build();
    CompositeValue compositeValue = CompositeValue.of(value1, value2);
    CompositeValue newCompositeValue = CompositeValue.from(compositeValue.toString());
    Assertions.assertEquals(compositeValue, newCompositeValue);
    Assertions.assertEquals(2, compositeValue.size());
    Assertions.assertEquals(value1, compositeValue.get(0));
    Assertions.assertEquals(value2, compositeValue.get(1));
  }
}
