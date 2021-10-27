package org.hypertrace.gateway.service.entity;

import static org.hypertrace.gateway.service.v1.entity.MultiValuedAttributeOperation.OperationType.OPERATION_TYPE_ADD;
import static org.hypertrace.gateway.service.v1.entity.MultiValuedAttributeOperation.OperationType.OPERATION_TYPE_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesRequestOperation;
import org.hypertrace.gateway.service.v1.entity.MultiValuedAttributeOperation;
import org.hypertrace.gateway.service.v1.entity.MultiValuedAttributeOperation.OperationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BulkUpdateEntitiesRequestValidatorTest {

  BulkUpdateEntitiesRequestValidator validator;
  Map<String, AttributeMetadata> attributeMetadataMap;

  @BeforeEach
  void setup() {
    validator = new BulkUpdateEntitiesRequestValidator();
    attributeMetadataMap = new HashMap<>();
    AttributeMetadata attributeMetadata = mock(AttributeMetadata.class);
    when(attributeMetadata.getSourcesList())
        .thenReturn(List.of(AttributeSource.AS, AttributeSource.QS, AttributeSource.EDS));
    attributeMetadataMap.put("labels", attributeMetadata);
  }

  @Test
  void validate_success() {
    BulkUpdateEntitiesRequest request =
        createBulkUpdateEntitiesRequest(
            "test-entity-type",
            List.of("entity-id-1", "entity-id-2"),
            OPERATION_TYPE_ADD,
            "labels",
            List.of("test-value"));
    Status status = validator.validate(request, attributeMetadataMap);
    assertTrue(status.isOk());
  }

  @Test
  void validate_emptyEntityType() {
    BulkUpdateEntitiesRequest request =
        createBulkUpdateEntitiesRequest(
            "",
            List.of("entity-id-1", "entity-id-2"),
            OPERATION_TYPE_ADD,
            "labels",
            List.of("test-value"));
    Status status = validator.validate(request, attributeMetadataMap);
    assertFalse(status.isOk());
  }

  @Test
  void validate_emptyEntityIds() {
    BulkUpdateEntitiesRequest request =
        createBulkUpdateEntitiesRequest(
            "test-entity-type",
            Collections.emptyList(),
            OPERATION_TYPE_ADD,
            "labels",
            List.of("test-value"));
    Status status = validator.validate(request, attributeMetadataMap);
    assertFalse(status.isOk());
  }

  @Test
  void validate_operationNotSet() {
    BulkUpdateEntitiesRequest request =
        BulkUpdateEntitiesRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .build();

    Status status = validator.validate(request, Collections.emptyMap());
    assertFalse(status.isOk());
  }

  @Test
  void validate_multiValuedOperationNotSet() {
    BulkUpdateEntitiesRequest request =
        BulkUpdateEntitiesRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(BulkUpdateEntitiesRequestOperation.newBuilder().build())
            .build();

    Status status = validator.validate(request, Collections.emptyMap());
    assertFalse(status.isOk());
  }

  @Test
  void validate_incorrectOperationType() {
    BulkUpdateEntitiesRequest request =
        createBulkUpdateEntitiesRequest(
            "test-entity-type",
            List.of("entity-id-1", "entity-id-2"),
            OPERATION_TYPE_UNSPECIFIED,
            "labels",
            List.of("test-value"));
    Status status = validator.validate(request, attributeMetadataMap);
    assertFalse(status.isOk());
  }

  @Test
  void validate_emptyValues_validInput() {
    BulkUpdateEntitiesRequest request =
        createBulkUpdateEntitiesRequest(
            "test-entity-type",
            List.of("entity-id-1", "entity-id-2"),
            OPERATION_TYPE_ADD,
            "labels",
            Collections.emptyList());
    Status status = validator.validate(request, attributeMetadataMap);
    assertTrue(status.isOk());
  }

  @Test
  void validate_incorrectAttribute() {
    BulkUpdateEntitiesRequest request =
        createBulkUpdateEntitiesRequest(
            "test-entity-type",
            List.of("entity-id-1", "entity-id-2"),
            OPERATION_TYPE_ADD,
            "labels",
            List.of("test-value"));
    Status status = validator.validate(request, Collections.emptyMap());
    assertFalse(status.isOk());
  }

  @Test
  void validate_attributeNotSet() {
    BulkUpdateEntitiesRequest request =
        BulkUpdateEntitiesRequest.newBuilder()
            .setEntityType("test-entity-type")
            .addAllEntityIds(List.of("entity-id-1", "entity-id-2"))
            .setOperation(
                BulkUpdateEntitiesRequestOperation.newBuilder()
                    .setMultiValuedAttributeOperation(
                        MultiValuedAttributeOperation.newBuilder()
                            .setType(OPERATION_TYPE_ADD)
                            .addAllValues(
                                List.of(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setValueType(ValueType.STRING)
                                                .setString("test-value")
                                                .build())
                                        .build()))
                            .build()))
            .build();
    Status status = validator.validate(request, Collections.emptyMap());
    assertFalse(status.isOk());
  }

  @Test
  void validate_incorrectAttributeSource() {
    BulkUpdateEntitiesRequest request =
        createBulkUpdateEntitiesRequest(
            "test-entity-type",
            List.of("entity-id-1", "entity-id-2"),
            OPERATION_TYPE_ADD,
            "labels",
            List.of("test-value"));
    attributeMetadataMap = new HashMap<>();
    AttributeMetadata attributeMetadata = mock(AttributeMetadata.class);
    when(attributeMetadata.getSourcesList())
        .thenReturn(List.of(AttributeSource.AS, AttributeSource.QS));
    attributeMetadataMap.put("labels", attributeMetadata);
    Status status = validator.validate(request, attributeMetadataMap);
    assertFalse(status.isOk());
  }

  private BulkUpdateEntitiesRequest createBulkUpdateEntitiesRequest(
      String entityType,
      List<String> entityIds,
      OperationType operationType,
      String attributeName,
      List<String> values) {
    BulkUpdateEntitiesRequest request =
        BulkUpdateEntitiesRequest.newBuilder()
            .setEntityType(entityType)
            .addAllEntityIds(entityIds)
            .setOperation(
                BulkUpdateEntitiesRequestOperation.newBuilder()
                    .setMultiValuedAttributeOperation(
                        MultiValuedAttributeOperation.newBuilder()
                            .setType(operationType)
                            .setAttribute(
                                ColumnIdentifier.newBuilder().setColumnName(attributeName).build())
                            .addAllValues(
                                values.stream()
                                    .map(
                                        value ->
                                            LiteralConstant.newBuilder()
                                                .setValue(
                                                    Value.newBuilder()
                                                        .setValueType(ValueType.STRING)
                                                        .setString(value)
                                                        .build())
                                                .build())
                                    .collect(Collectors.toList()))
                            .build())
                    .build())
            .build();
    return request;
  }
}
