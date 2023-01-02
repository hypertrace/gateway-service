package org.hypertrace.gateway.service.entity.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

class UpdateOperatorConverterTest {

  private UpdateOperatorConverter updateOperatorConverter;

  @BeforeEach
  void setUp() {
    updateOperatorConverter = new UpdateOperatorConverter();
  }

  @ParameterizedTest
  @EnumSource(
      value = Operator.class,
      mode = Mode.EXCLUDE,
      names = {"OPERATION_UNSPECIFIED", "UNRECOGNIZED"})
  void testConvertCovered(final Operator operator) {
    assertEquals(
        AttributeUpdateOperator.valueOf(
            operator.name().replace("OPERATOR", "ATTRIBUTE_UPDATE_OPERATOR")),
        updateOperatorConverter.convert(operator));
  }
}
