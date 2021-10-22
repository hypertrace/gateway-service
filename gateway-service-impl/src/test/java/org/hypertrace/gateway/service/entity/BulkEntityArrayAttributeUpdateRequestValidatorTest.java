package org.hypertrace.gateway.service.entity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.BulkEntityArrayAttributeUpdateRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BulkEntityArrayAttributeUpdateRequestValidatorTest {

  BulkEntityArrayAttributeUpdateRequestValidator validator;
  Map<String, AttributeMetadata> attributeMetadataMap;

  @BeforeEach
  void setup() {
    validator = new BulkEntityArrayAttributeUpdateRequestValidator();
    attributeMetadataMap = new HashMap<>();
    AttributeMetadata attributeMetadata = mock(AttributeMetadata.class);
    when(attributeMetadata.getSourcesCount()).thenReturn(1);
    when(attributeMetadata.getSources(0)).thenReturn(AttributeSource.EDS);
    attributeMetadataMap.put("labels", attributeMetadata);
  }

  @Test
  void validate_success() {
    BulkEntityArrayAttributeUpdateRequest request =
        BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkEntityArrayAttributeUpdateRequest.Operation.OPERATION_ADD)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("labels").build())
            .addAllValues(
                List.of(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString("test-value")
                                .build())
                        .build()))
            .build();
    assertDoesNotThrow(() -> validator.validate(request, attributeMetadataMap));
  }

  @Test
  void validate_emptyEntityType() {
    BulkEntityArrayAttributeUpdateRequest request =
        BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .setEntityType("")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkEntityArrayAttributeUpdateRequest.Operation.OPERATION_ADD)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("labels").build())
            .addAllValues(
                List.of(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString("test-value")
                                .build())
                        .build()))
            .build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validate(request, attributeMetadataMap));
  }

  @Test
  void validate_emptyEntityIds() {
    BulkEntityArrayAttributeUpdateRequest request =
        BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(Collections.emptyList())
            .setOperation(BulkEntityArrayAttributeUpdateRequest.Operation.OPERATION_ADD)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("labels").build())
            .addAllValues(
                List.of(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString("test-value")
                                .build())
                        .build()))
            .build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validate(request, attributeMetadataMap));
  }

  @Test
  void validate_incorrectOperation() {
    BulkEntityArrayAttributeUpdateRequest request =
        BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkEntityArrayAttributeUpdateRequest.Operation.OPERATION_UNSPECIFIED)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("labels").build())
            .addAllValues(
                List.of(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString("test-value")
                                .build())
                        .build()))
            .build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validate(request, attributeMetadataMap));
  }

  @Test
  void validate_emptyValues() {
    BulkEntityArrayAttributeUpdateRequest request =
        BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkEntityArrayAttributeUpdateRequest.Operation.OPERATION_ADD)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("labels").build())
            .addAllValues(Collections.emptyList())
            .build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validate(request, attributeMetadataMap));
  }

  @Test
  void validate_incorrectAttribute() {
    BulkEntityArrayAttributeUpdateRequest request =
        BulkEntityArrayAttributeUpdateRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkEntityArrayAttributeUpdateRequest.Operation.OPERATION_ADD)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("labels").build())
            .addAllValues(
                List.of(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString("test-value")
                                .build())
                        .build()))
            .build();
    assertThrows(
        IllegalArgumentException.class, () -> validator.validate(request, Collections.emptyMap()));
  }
}
