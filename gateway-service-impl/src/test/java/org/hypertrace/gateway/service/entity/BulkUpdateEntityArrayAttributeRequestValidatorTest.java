package org.hypertrace.gateway.service.entity;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
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
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntityArrayAttributeRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BulkUpdateEntityArrayAttributeRequestValidatorTest {

  BulkUpdateEntityArrayAttributeRequestValidator validator;
  Map<String, AttributeMetadata> attributeMetadataMap;

  @BeforeEach
  void setup() {
    validator = new BulkUpdateEntityArrayAttributeRequestValidator();
    attributeMetadataMap = new HashMap<>();
    AttributeMetadata attributeMetadata = mock(AttributeMetadata.class);
    when(attributeMetadata.getSourcesList())
        .thenReturn(List.of(AttributeSource.AS, AttributeSource.QS, AttributeSource.EDS));
    attributeMetadataMap.put("labels", attributeMetadata);
  }

  @Test
  void validate_success() {
    BulkUpdateEntityArrayAttributeRequest request =
        BulkUpdateEntityArrayAttributeRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkUpdateEntityArrayAttributeRequest.Operation.OPERATION_ADD)
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
    Status status = validator.validate(request, attributeMetadataMap);
    assertTrue(status.isOk());
  }

  @Test
  void validate_emptyEntityType() {
    BulkUpdateEntityArrayAttributeRequest request =
        BulkUpdateEntityArrayAttributeRequest.newBuilder()
            .setEntityType("")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkUpdateEntityArrayAttributeRequest.Operation.OPERATION_ADD)
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
    Status status = validator.validate(request, attributeMetadataMap);
    assertFalse(status.isOk());
  }

  @Test
  void validate_emptyEntityIds() {
    BulkUpdateEntityArrayAttributeRequest request =
        BulkUpdateEntityArrayAttributeRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(Collections.emptyList())
            .setOperation(BulkUpdateEntityArrayAttributeRequest.Operation.OPERATION_ADD)
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
    Status status = validator.validate(request, attributeMetadataMap);
    assertFalse(status.isOk());
  }

  @Test
  void validate_incorrectOperation() {
    BulkUpdateEntityArrayAttributeRequest request =
        BulkUpdateEntityArrayAttributeRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkUpdateEntityArrayAttributeRequest.Operation.OPERATION_UNSPECIFIED)
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
    Status status = validator.validate(request, attributeMetadataMap);
    assertFalse(status.isOk());
  }

  @Test
  void validate_emptyValues_validInput() {
    BulkUpdateEntityArrayAttributeRequest request =
        BulkUpdateEntityArrayAttributeRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkUpdateEntityArrayAttributeRequest.Operation.OPERATION_ADD)
            .setAttribute(ColumnIdentifier.newBuilder().setColumnName("labels").build())
            .addAllValues(Collections.emptyList())
            .build();
    Status status = validator.validate(request, attributeMetadataMap);
    assertTrue(status.isOk());
  }

  @Test
  void validate_incorrectAttribute() {
    BulkUpdateEntityArrayAttributeRequest request =
        BulkUpdateEntityArrayAttributeRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkUpdateEntityArrayAttributeRequest.Operation.OPERATION_ADD)
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
    Status status = validator.validate(request, Collections.emptyMap());
    assertFalse(status.isOk());
  }
}
