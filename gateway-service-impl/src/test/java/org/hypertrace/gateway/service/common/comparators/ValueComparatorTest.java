package org.hypertrace.gateway.service.common.comparators;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Test;

class ValueComparatorTest {

  @Test
  void comparesStringArrayValues() {

    List<Value> input =
        Arrays.asList(
            buildStringArrayValue("a"),
            buildStringArrayValue("b", "a"),
            buildStringArrayValue("b"),
            buildStringArrayValue("ab"),
            buildStringArrayValue("a", "b"),
            buildStringArrayValue());

    List<Value> expected =
        Arrays.asList(
            buildStringArrayValue(),
            buildStringArrayValue("a"),
            buildStringArrayValue("a", "b"),
            buildStringArrayValue("ab"),
            buildStringArrayValue("b"),
            buildStringArrayValue("b", "a"));

    input.sort(ValueComparator::compare);
    assertEquals(expected, input);
  }

  private Value buildStringArrayValue(String... strings) {
    return Value.newBuilder()
        .addAllStringArray(Arrays.asList(strings))
        .setValueType(ValueType.STRING_ARRAY)
        .build();
  }
}
