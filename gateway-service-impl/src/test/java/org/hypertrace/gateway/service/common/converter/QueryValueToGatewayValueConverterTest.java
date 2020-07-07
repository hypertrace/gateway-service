package org.hypertrace.gateway.service.common.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryValueToGatewayValueConverterTest {
  private static final String WEIGHT_METRIC_NAME = "weight";
  private AttributeMetadata weightDoubleMetadata;
  private ColumnMetadata weightColumnMetadata;
  private Map<String, AttributeMetadata> attributeMetadataMap;

  @BeforeEach
  public void setup() {
    weightDoubleMetadata = createMetadata(WEIGHT_METRIC_NAME, AttributeKind.TYPE_DOUBLE);
    weightColumnMetadata = ColumnMetadata.newBuilder().setColumnName(WEIGHT_METRIC_NAME).build();
    attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(WEIGHT_METRIC_NAME, weightDoubleMetadata);
  }

  @Test
  public void whenValueTypeString_AttributeKindString_thenExpectStringType() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("foo")
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("fName")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals("foo", retVal.getString());
  }

  @Test
  public void whenValueTypeString_AttributeKindBool_thenExpectBoolType() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("true")
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("new_member")
            .setValueKind(AttributeKind.TYPE_BOOL)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.BOOL);
    assertTrue(retVal.getBoolean());
  }

  @Test
  public void whenValueTypeString_AttributeKindDouble_thenExpectBoolType() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("10.24")
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_DOUBLE)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.DOUBLE);
    assertEquals(10.24, retVal.getDouble(), 0);
  }

  @Test
  public void whenValueTypeString_AttributeKindInt64_thenExpectLongType() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("10")
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_INT64)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG);
    assertEquals(10, retVal.getLong());
  }

  @Test
  public void whenValueTypeString_AttributeKindTimestamp_thenExpectTimestampType() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("10")
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("startTime")
            .setValueKind(AttributeKind.TYPE_TIMESTAMP)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.TIMESTAMP);
    assertEquals(10, retVal.getTimestamp());
  }

  @Test
  public void whenValueTypeString_AttributeKindStringStringMap_expectsStringStringMap()
      throws JsonProcessingException {
    String mapKey = "KEY1";
    String mapValue = "VAL1";
    Map<String, String> mapData = new HashMap<>();
    mapData.put(mapKey, mapValue);

    ObjectMapper objectMapper = new ObjectMapper();
    String mapDataInJson =
        objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapData);

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString(mapDataInJson)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("mapData")
            .setValueKind(AttributeKind.TYPE_STRING_MAP)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING_MAP);
    assertEquals(mapData, retVal.getStringMapMap());
  }

  @Test
  public void whenValueTypeString_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("10")
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeStringMap_AttributeKindStringStringMap_expectsStringStringMap() {
    String mapKey = "KEY1";
    String mapValue = "VAL1";
    Map<String, String> mapData = new HashMap<>();
    mapData.put(mapKey, mapValue);

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_MAP)
            .putAllStringMap(Map.of(mapKey, mapValue))
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("mapData")
            .setValueKind(AttributeKind.TYPE_STRING_MAP)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING_MAP);
    assertEquals(mapData, retVal.getStringMapMap());
  }

  @Test
  public void whenValueTypeStringMap_AttributeKindString_expectsString() {
    String mapKey = "KEY1";
    String mapValue = "VAL1";
    Map<String, String> mapData = new HashMap<>();
    mapData.put(mapKey, mapValue);

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_MAP)
            .putAllStringMap(Map.of(mapKey, mapValue))
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("stringData")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals(mapData.toString(), retVal.getString());
  }

  @Test
  public void whenValueTypeInteger_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT)
            .setInt(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeInteger_AttributeKindInt64_thenExpectValueTypeLong() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT)
            .setInt(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_INT64)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG);
    assertEquals(10, retVal.getLong());
  }

  @Test
  public void whenValueTypeInteger_AttributeKindDouble_thenExpectValueTypeDouble() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT)
            .setInt(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_DOUBLE)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.DOUBLE);
    assertEquals(10, retVal.getDouble(), 0);
  }

  @Test
  public void whenValueTypeInteger_AttributeKindString_thenExpectValueTypeString() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT)
            .setInt(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals("10", retVal.getString());
  }

  @Test
  public void whenValueTypeBoolean_AttributeKindString_thenExpectValueTypeString() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.BOOL)
            .setBoolean(true)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("new_member")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals("true", retVal.getString());
  }

  @Test
  public void whenValueTypeBoolean_AttributeKindBool_thenExpectValueTypeBool() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.BOOL)
            .setBoolean(true)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("new_member")
            .setValueKind(AttributeKind.TYPE_BOOL)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.BOOL);
    assertEquals(true, retVal.getBoolean());
  }

  @Test
  public void whenValueTypeBoolean_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.BOOL)
            .setBoolean(true)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("new_member")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallbackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(retVal, fallbackVal);
  }

  @Test
  public void whenValueTypeLong_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
            .setLong(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeLong_AttributeKindInt64_thenExpectValueTypeLong() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
            .setLong(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_INT64)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG);
    assertEquals(10, retVal.getLong());
  }

  @Test
  public void whenValueTypeLong_AttributeKindDouble_thenExpectValueTypeDouble() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
            .setLong(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_DOUBLE)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.DOUBLE);
    assertEquals(10, retVal.getDouble(), 0);
  }

  @Test
  public void whenValueTypeLong_AttributeKindString_thenExpectValueTypeString() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
            .setLong(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals("10", retVal.getString());
  }

  @Test
  public void whenValueTypeFloat_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT)
            .setFloat(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeFloat_AttributeKindInt64_thenExpectValueTypeLong() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT)
            .setFloat(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_INT64)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG);
    assertEquals(10, retVal.getLong());
  }

  @Test
  public void whenValueTypeFloat_AttributeKindDouble_thenExpectValueTypeDouble() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT)
            .setFloat(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_DOUBLE)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.DOUBLE);
    assertEquals(10.25, retVal.getDouble(), 0);
  }

  @Test
  public void whenValueTypeFloat_AttributeKindString_thenExpectValueTypeString() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT)
            .setFloat(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals("10.25", retVal.getString());
  }

  @Test
  public void whenValueTypeDouble_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE)
            .setDouble(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeDouble_AttributeKindInt64_thenExpectValueTypeLong() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE)
            .setDouble(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_INT64)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG);
    assertEquals(10, retVal.getLong());
  }

  @Test
  public void whenValueTypeDouble_AttributeKindDouble_thenExpectValueTypeDouble() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE)
            .setDouble(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_DOUBLE)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.DOUBLE);
    assertEquals(10.25, retVal.getDouble(), 0);
  }

  @Test
  public void whenValueTypeDouble_AttributeKindString_thenExpectValueTypeString() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE)
            .setDouble(10.25f)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals("10.25", retVal.getString());
  }

  @Test
  public void whenValueTypeTimestamp_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.TIMESTAMP)
            .setTimestamp(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeTimestamp_AttributeKindInt64_thenExpectValueTypeLong() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.TIMESTAMP)
            .setTimestamp(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_INT64)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG);
    assertEquals(10, retVal.getLong());
  }

  @Test
  public void whenValueTypeTimestamp_AttributeKindString_thenExpectValueTypeString() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.TIMESTAMP)
            .setTimestamp(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals("10", retVal.getString());
  }

  @Test
  public void whenValueTypeTimestamp_AttributeKindTimestamp_thenExpectValueTypeTimestamp() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.TIMESTAMP)
            .setTimestamp(10)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.TYPE_TIMESTAMP)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.TIMESTAMP);
    assertEquals(10, retVal.getTimestamp());
  }

  @Test
  public void whenValueTypeIntegerArray_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT_ARRAY)
            .addAllIntArray(IntStream.range(1, 11).boxed().collect(Collectors.toList()))
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeIntegerArray_AttributeKindInt64Array_thenExpectValueTypeLongArray() {
    List<Integer> input = IntStream.range(1, 11).boxed().collect(Collectors.toList());
    List<Long> expected = input.stream().map(Integer::longValue).collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT_ARRAY)
            .addAllIntArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_INT64_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG_ARRAY);
    assertEquals(expected, retVal.getLongArrayList());
  }

  @Test
  public void whenValueTypeIntegerArray_AttributeKindString_thenExpectValueTypeString() {
    List<Integer> input = IntStream.range(1, 11).boxed().collect(Collectors.toList());
    String expected = input.toString();

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT_ARRAY)
            .addAllIntArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals(expected, retVal.getString());
  }

  @Test
  public void whenValueTypeIntegerArray_AttributeKindStringArray_thenExpectValueTypeStringArray() {
    List<Integer> input = IntStream.range(1, 11).boxed().collect(Collectors.toList());
    List<String> expected = input.stream().map(Object::toString).collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.INT_ARRAY)
            .addAllIntArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING_ARRAY);
    assertEquals(expected, retVal.getStringArrayList());
  }

  @Test
  public void whenValueTypeLongArray_AttributeKindUndefined_thenExpectValueFromFallback() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG_ARRAY)
            .addAllLongArray(LongStream.range(1, 11).boxed().collect(Collectors.toList()))
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeLongArray_AttributeKindInt64Array_thenExpectValueTypeLongArray() {
    List<Long> input = LongStream.range(1, 11).boxed().collect(Collectors.toList());
    List<Long> expected = input;

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG_ARRAY)
            .addAllLongArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_INT64_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.LONG_ARRAY);
    assertEquals(expected, retVal.getLongArrayList());
  }

  @Test
  public void whenValueTypeLongArray_AttributeKindString_thenExpectValueTypeString() {
    List<Long> input = LongStream.range(1, 11).boxed().collect(Collectors.toList());
    String expected = input.toString();

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG_ARRAY)
            .addAllLongArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals(expected, retVal.getString());
  }

  @Test
  public void whenValueTypeLongArray_AttributeKindStringArray_thenExpectValueTypeStringArray() {
    List<Long> input = LongStream.range(1, 11).boxed().collect(Collectors.toList());
    List<String> expected = input.stream().map(Object::toString).collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG_ARRAY)
            .addAllLongArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING_ARRAY);
    assertEquals(expected, retVal.getStringArrayList());
  }

  @Test
  public void whenValueTypeFloatArray_AttributeKindUndefined_thenExpectValueFromFallback() {
    List<Float> input =
        DoubleStream.of(10.1, 10.2).boxed().map(Double::floatValue).collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT_ARRAY)
            .addAllFloatArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeFloatArray_AttributeKindDoubleArray_thenExpectValueTypeDoubleArray() {
    List<Float> input =
        DoubleStream.of(10.1, 10.2).boxed().map(Double::floatValue).collect(Collectors.toList());
    List<Double> expected = input.stream().map(Float::doubleValue).collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT_ARRAY)
            .addAllFloatArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_DOUBLE_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.DOUBLE_ARRAY);
    assertEquals(expected, retVal.getDoubleArrayList());
  }

  @Test
  public void whenValueTypeFloatArray_AttributeKindString_thenExpectValueTypeString() {
    List<Float> input =
        DoubleStream.of(10.1, 10.2).boxed().map(Double::floatValue).collect(Collectors.toList());
    String expected = input.toString();

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT_ARRAY)
            .addAllFloatArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals(expected, retVal.getString());
  }

  @Test
  public void whenValueTypeFloatArray_AttributeKindStringArray_thenExpectValueTypeStringArray() {
    List<Float> input =
        DoubleStream.of(10.1, 10.2).boxed().map(Double::floatValue).collect(Collectors.toList());
    List<String> expected = input.stream().map(Object::toString).collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.FLOAT_ARRAY)
            .addAllFloatArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING_ARRAY);
    assertEquals(expected, retVal.getStringArrayList());
  }

  @Test
  public void whenValueTypeDoubleArray_AttributeKindUndefined_thenExpectValueFromFallback() {
    List<Double> input = DoubleStream.of(10.1, 10.2).boxed().collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE_ARRAY)
            .addAllDoubleArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weight")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(fallBackVal, retVal);
  }

  @Test
  public void whenValueTypeDoubleArray_AttributeKindDoubleArray_thenExpectValueTypeDoubleArray() {
    List<Double> input = DoubleStream.of(10.1, 10.2).boxed().collect(Collectors.toList());
    List<Double> expected = input;

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE_ARRAY)
            .addAllDoubleArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_DOUBLE_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.DOUBLE_ARRAY);
    assertEquals(expected, retVal.getDoubleArrayList());
  }

  @Test
  public void whenValueTypeDoubleArray_AttributeKindString_thenExpectValueTypeString() {
    List<Double> input = DoubleStream.of(10.1, 10.2).boxed().collect(Collectors.toList());
    String expected = input.toString();

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE_ARRAY)
            .addAllDoubleArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals(expected, retVal.getString());
  }

  @Test
  public void whenValueTypeDoubleArray_AttributeKindStringArray_thenExpectValueTypeStringArray() {
    List<Double> input = DoubleStream.of(10.1, 10.2).boxed().collect(Collectors.toList());
    List<String> expected = input.stream().map(Object::toString).collect(Collectors.toList());

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE_ARRAY)
            .addAllDoubleArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING_ARRAY);
    assertEquals(expected, retVal.getStringArrayList());
  }

  @Test
  public void whenValueTypeStringArray_AttributeKindString_thenExpectValueTypeString() {
    List<String> input = Arrays.asList("one", "two");
    String expected = input.toString();

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_ARRAY)
            .addAllStringArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals(expected, retVal.getString());
  }

  @Test
  public void whenValueTypeStringArray_AttributeKindStringArray_thenExpectValueTypeStringArray() {
    List<String> input = Arrays.asList("one", "two");
    List<String> expected = input;

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_ARRAY)
            .addAllStringArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING_ARRAY);
    assertEquals(expected, retVal.getStringArrayList());
  }

  @Test
  public void whenValueTypeStringArray_AttributeKindUndefined_thenExpectValueFromFallback() {
    List<String> input = Arrays.asList("one", "two");
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING_ARRAY)
            .addAllStringArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallbackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(retVal, fallbackVal);
  }

  @Test
  public void whenValueTypeBooleanArray_AttributeKindString_thenExpectValueTypeString() {
    List<Boolean> input = Arrays.asList(true, true);
    String expected = input.toString();

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.BOOLEAN_ARRAY)
            .addAllBooleanArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_STRING)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.STRING);
    assertEquals(expected, retVal.getString());
  }

  @Test
  public void
      whenValueTypeBooleanArray_AttributeKindBooleanArray_thenExpectValueTypeBooleanArray() {
    List<Boolean> input = Arrays.asList(true, true);

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.BOOLEAN_ARRAY)
            .addAllBooleanArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.TYPE_BOOL_ARRAY)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    assertSame(retVal.getValueType(), ValueType.BOOLEAN_ARRAY);
    assertEquals(input, retVal.getBooleanArrayList());
  }

  @Test
  public void whenValueTypeBooleanArray_AttributeKindUndefined_thenExpectValueFromFallback() {
    List<Boolean> input = Arrays.asList(true, true);

    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.BOOLEAN_ARRAY)
            .addAllBooleanArray(input)
            .build();
    AttributeMetadata attributeMetadata =
        AttributeMetadata.newBuilder()
            .setFqn("weights")
            .setValueKind(AttributeKind.KIND_UNDEFINED)
            .build();

    org.hypertrace.gateway.service.v1.common.Value retVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value, attributeMetadata);
    org.hypertrace.gateway.service.v1.common.Value fallBackVal =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);
    assertEquals(retVal, fallBackVal);
  }

  @Test
  public void whenMetricValueTypeAlreadyIsDoubleOrFloat_expectsSameType() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.DOUBLE)
            .setDouble(10.25f)
            .build();

    Map<String, AttributeKind> aliasToAttributeKind = new HashMap<>();
    aliasToAttributeKind.put(WEIGHT_METRIC_NAME, AttributeKind.TYPE_DOUBLE);

    org.hypertrace.gateway.service.v1.common.Value actual =
        QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
            aliasToAttributeKind, attributeMetadataMap, weightColumnMetadata, value);

    org.hypertrace.gateway.service.v1.common.Value expected =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(value);

    assertEquals(expected, actual);
  }

  @Test
  public void
      whenMetricValueTypeIsString_AndAttributeeMetadataHasTypeINT64_thenExpectValueTypeToBeConvertedToDouble() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("10")
            .build();

    Map<String, AttributeKind> aliasToAttributeKind = new HashMap<>();
    aliasToAttributeKind.put(WEIGHT_METRIC_NAME, AttributeKind.TYPE_DOUBLE);

    org.hypertrace.gateway.service.v1.common.Value actual =
        QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
            aliasToAttributeKind, attributeMetadataMap, weightColumnMetadata, value);

    AttributeMetadata attributeMetadataOverride =
        AttributeMetadata.newBuilder()
            .setFqn(WEIGHT_METRIC_NAME)
            .setValueKind(AttributeKind.TYPE_DOUBLE)
            .build();
    org.hypertrace.gateway.service.v1.common.Value expected =
        QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(
            value, attributeMetadataOverride);

    assertEquals(expected, actual);
  }

  @Test
  public void
      whenMetricValueTypeIsString_AndNullOverrideMapping_thenExpectValueTypeToBeConvertedToDouble() {
    Value value =
        Value.newBuilder()
            .setValueType(org.hypertrace.core.query.service.api.ValueType.STRING)
            .setString("10.25")
            .build();

    org.hypertrace.gateway.service.v1.common.Value actual =
        QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
            Collections.emptyMap(), attributeMetadataMap, weightColumnMetadata, value);

    org.hypertrace.gateway.service.v1.common.Value expected =
        org.hypertrace.gateway.service.v1.common.Value.newBuilder()
            .setDouble(10.25f)
            .setValueType(ValueType.DOUBLE)
            .build();

    assertEquals(expected, actual);
  }

  private AttributeMetadata createMetadata(String name, AttributeKind kind) {
    return AttributeMetadata.newBuilder().setFqn(name).setValueKind(kind).build();
  }
}
