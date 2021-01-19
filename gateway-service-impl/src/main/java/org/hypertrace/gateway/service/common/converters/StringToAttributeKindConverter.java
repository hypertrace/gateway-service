package org.hypertrace.gateway.service.common.converters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringToAttributeKindConverter extends ToAttributeKindConverter<String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StringToAttributeKindConverter.class);
  private static final TypeReference<Map<String, String>> MAP_TYPE_REFERENCE = new TypeReference<>() {};
  private static final TypeReference<List<String>> LIST_TYPE_REFERENCE = new TypeReference<>() {};
  public static StringToAttributeKindConverter INSTANCE = new StringToAttributeKindConverter();
  private final ObjectMapper objectMapper = new ObjectMapper();

  private StringToAttributeKindConverter() {}

  public Value doConvert(String value, AttributeKind attributeKind, Value.Builder valueBuilder) {
    switch (attributeKind) {
      case TYPE_BOOL:
        valueBuilder.setValueType(ValueType.BOOL);
        valueBuilder.setBoolean(Boolean.parseBoolean(value));
        return valueBuilder.build();

      case TYPE_DOUBLE:
        valueBuilder.setValueType(ValueType.DOUBLE);
        /*
        * In proto, by default, field with missing value will be set to empty string. As a result,
        * optional attributes that could return empty string will cause java.lang.NumberFormatException.
        * Instead, convert this to 0, which is default value for double and long in proto if
        * field is missing.
        */
        if (value.isEmpty()) {
          valueBuilder.setDouble(0.0d);
        } else {
          valueBuilder.setDouble(Double.parseDouble(value));
        }
        return valueBuilder.build();

      case TYPE_INT64:
        valueBuilder.setValueType(ValueType.LONG);
        // Attributes with missing values are converted to empty string proto. And to be equivalent,
        // missing long or double is set to 0 as well in proto
        if (value.isEmpty()) {
          valueBuilder.setLong(0L);
        } else {
          // By default, aggregation is returned as String with Decimal-Value
          // parse as double first before converting to Long
          valueBuilder.setLong((long) Double.parseDouble(value));
        }
        return valueBuilder.build();

      case TYPE_STRING:
        valueBuilder.setValueType(ValueType.STRING);
        valueBuilder.setString(value);
        return valueBuilder.build();

      case TYPE_TIMESTAMP:
        valueBuilder.setValueType(ValueType.TIMESTAMP);
        valueBuilder.setTimestamp(Long.parseLong(value));
        return valueBuilder.build();

      case TYPE_STRING_MAP:
        valueBuilder.setValueType(ValueType.STRING_MAP);
        valueBuilder.putAllStringMap(convertToMap(value));
        return valueBuilder.build();
      case TYPE_STRING_ARRAY:
        valueBuilder.setValueType(ValueType.STRING_ARRAY);
        valueBuilder.addAllStringArray(convertToArray(value));
        return valueBuilder.build();
      default:
        break;
    }
    return null;
  }

  private List<String> convertToArray(String jsonString) {
    if (StringUtils.isEmpty(jsonString)) {
      return List.of();
    }

    // Check if the string is already in a list format.
    try {
      return Optional.ofNullable(objectMapper.readValue(jsonString, LIST_TYPE_REFERENCE))
          .orElse(Collections.emptyList());
    } catch (JsonProcessingException e) {
      // If not, it might be a single string, so return a list with one element.
      return List.of(jsonString);
    }
  }

  private Map<String, String> convertToMap(String jsonString) {
    Map<String, String> mapData = new HashMap<>();
    try {
      if (!StringUtils.isEmpty(jsonString)) {
        mapData = objectMapper.readValue(jsonString, MAP_TYPE_REFERENCE);
      }
    } catch (IOException e) {
      LOGGER.warn("Unable to read Map JSON String data from: {}. Setting data as empty map instead. With error:", jsonString, e);
    }
    return mapData;
  }
}
