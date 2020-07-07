package org.hypertrace.gateway.service.common.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricAggregationFunctionUtilTest {
  private static final String BYTES_RECEIVED = "Api.metrics.bytes_received";
  private static final String THRESHOLD = "Api.metrics.threshold";
  private static final String NAME = "Api.name";
  private static final AttributeKind BYTES_RECEIVED_KIND = AttributeKind.TYPE_INT64;
  private static final AttributeKind THRESHOLD_KIND = AttributeKind.TYPE_DOUBLE;
  private static final AttributeKind NAME_KIND = AttributeKind.TYPE_STRING;
  Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
  private AttributeMetadata stringMetadata;
  private AttributeMetadata longMetadata;
  private AttributeMetadata doubleMetadata;

  @BeforeEach
  public void setup() {
    stringMetadata = AttributeMetadata.newBuilder().setValueKind(AttributeKind.TYPE_STRING).build();
    longMetadata = AttributeMetadata.newBuilder().setValueKind(AttributeKind.TYPE_INT64).build();
    doubleMetadata = AttributeMetadata.newBuilder().setValueKind(AttributeKind.TYPE_DOUBLE).build();
    attributeMetadataMap.put(BYTES_RECEIVED, longMetadata);
    attributeMetadataMap.put(THRESHOLD, doubleMetadata);
    attributeMetadataMap.put(NAME, stringMetadata);
  }

  @Test
  public void test_getValueTypeFromFunction_SUMWithLong_shouldReturnLong() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED, FunctionType.SUM, "SUM_bytes_received", BYTES_RECEIVED_KIND);
  }

  @Test
  public void test_getValueTypeFromFunction_SUMWithDouble_shouldReturnDouble() {
    createFunctionAndVerifyKind(THRESHOLD, FunctionType.SUM, "SUM_threshold", THRESHOLD_KIND);
  }

  @Test
  public void test_getValueTypeFromFunction_SUMWithString_shouldThrowException() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          createFunctionAndVerifyKind(NAME, FunctionType.SUM, "SUM_name", NAME_KIND);
        });
  }

  @Test
  public void test_getValueTypeFromFunction_AVG_shouldReturnDouble() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED, FunctionType.AVG, "AVG_bytes_received", AttributeKind.TYPE_DOUBLE);
  }

  @Test
  public void test_getValueTypeFromFunction_AVGRATE_shouldReturnDouble() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED, FunctionType.AVGRATE, "AVGRATE_bytes_received", AttributeKind.TYPE_DOUBLE);
  }

  @Test
  public void test_getValueTypeFromFunction_PERCENTILE_shouldReturnDouble() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED,
        FunctionType.PERCENTILE,
        "PERCENTILE_bytes_received",
        AttributeKind.TYPE_DOUBLE);
  }

  @Test
  public void test_getValueTypeFromFunction_COUNT_shouldReturnLong() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED, FunctionType.COUNT, "COUNT_bytes_received", AttributeKind.TYPE_INT64);
  }

  @Test
  public void test_getValueTypeFromFunction_DISTINCTCOUNT_shouldReturnLong() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED,
        FunctionType.DISTINCTCOUNT,
        "DISTINCTCOUNT_bytes_received",
        AttributeKind.TYPE_INT64);
  }

  @Test
  public void test_getValueTypeFromFunction_MIN_shouldReturnOriginal() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED, FunctionType.MIN, "MIM_bytes_received", BYTES_RECEIVED_KIND);
  }

  @Test
  public void test_getValueTypeFromFunction_MAX_shouldReturnLong() {
    createFunctionAndVerifyKind(
        BYTES_RECEIVED, FunctionType.MAX, "MAX_bytes_received", BYTES_RECEIVED_KIND);
  }

  private void createFunctionAndVerifyKind(
      String metricName, FunctionType functionType, String alias, AttributeKind expectedKind) {
    Expression expr =
        QueryExpressionUtil.getAggregateFunctionExpression(metricName, functionType, alias, true)
            .build();
    Map<String, AttributeKind> aliasToKind =
        MetricAggregationFunctionUtil.getValueTypeFromFunction(
            Collections.singletonMap(alias, expr.getFunction()), attributeMetadataMap);
    Assertions.assertEquals(expectedKind, aliasToKind.get(alias));
  }
}
