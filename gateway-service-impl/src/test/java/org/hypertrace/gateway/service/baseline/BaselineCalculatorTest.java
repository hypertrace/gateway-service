package org.hypertrace.gateway.service.baseline;

import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class BaselineCalculatorTest {

  @Test
  public void testBaselineForDoubleValues() {
    List<Value> values = new ArrayList<>();
    values.add(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(2).build());
    values.add(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(10).build());
    values.add(Value.newBuilder().setValueType(ValueType.DOUBLE).setDouble(100).build());
    Baseline baseline = BaselineCalculator.getBaseline(values);
    Assertions.assertEquals(10.0, baseline.getValue().getDouble());
    // Lower bound will be negative so it should be zero.
    Assertions.assertEquals(0, baseline.getLowerBound().getDouble());
  }

  @Test
  public void testBaselineForLongValues() {
    List<Value> values = new ArrayList<>();
    values.add(Value.newBuilder().setValueType(ValueType.LONG).setLong(8).build());
    values.add(Value.newBuilder().setValueType(ValueType.LONG).setLong(10).build());
    values.add(Value.newBuilder().setValueType(ValueType.LONG).setLong(12).build());
    Baseline baseline = BaselineCalculator.getBaseline(values);
    Assertions.assertEquals(10.0, baseline.getValue().getDouble());
    Assertions.assertEquals(6.0, baseline.getLowerBound().getDouble());
  }

  @Test
  public void testBaselineUnsupportedType() {
    List<Value> values = new ArrayList<>();
    values.add(Value.newBuilder().setValueType(ValueType.STRING).setString("2").build());
    values.add(Value.newBuilder().setValueType(ValueType.STRING).setString("abc").build());
    values.add(Value.newBuilder().setValueType(ValueType.STRING).setString("s34").build());
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> BaselineCalculator.getBaseline(values),
        "Baseline cannot be calculated for String values");
  }
}
