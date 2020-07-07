package org.hypertrace.gateway.service.entity.update;

import static org.hypertrace.core.attribute.service.v1.AttributeScope.BACKEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityUpdateRequest;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.SetAttribute;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityOperation;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityRequest;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityResponse;
import org.junit.jupiter.api.Test;

public class EdsEntityUpdaterTest {
  @Test
  public void nonExistentEntity() {
    EntityQueryServiceClient mockEqsClient = mock(EntityQueryServiceClient.class);
    EdsEntityUpdater entityUpdater = new EdsEntityUpdater(mockEqsClient);

    // mock empty ResultSetChunk
    UpdateEntityRequest updateEntityRequest = createResolveTestRequest("non-existent-id");
    ResultSetChunk emptyResult = ResultSetChunk.newBuilder().build();
    when(mockEqsClient.update(any(), any())).thenReturn(List.of(emptyResult).iterator());

    UpdateExecutionContext updateExecutionContext =
        new UpdateExecutionContext(null, mockTestMetadata());
    UpdateEntityResponse response =
        entityUpdater.update(updateEntityRequest, updateExecutionContext).build();
    assertFalse(response.hasEntity());
  }

  @Test
  public void updateSuccess() {
    EntityQueryServiceClient mockEqsClient = mock(EntityQueryServiceClient.class);
    EdsEntityUpdater entityUpdater = new EdsEntityUpdater(mockEqsClient);

    // mock ResultSetChunk response from EQS
    UpdateEntityRequest updateEntityRequest = createResolveTestRequest("some-id");
    ResultSetChunk result =
        ResultSetChunk.newBuilder()
            .addRow(
                Row.newBuilder()
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                            .setString("some-id"))
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                            .setString("RESOLVED")))
            .setResultSetMetadata(
                ResultSetMetadata.newBuilder()
                    .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("Test.id"))
                    .addColumnMetadata(ColumnMetadata.newBuilder().setColumnName("Test.status")))
            .build();

    when(mockEqsClient.update(any(), any())).thenReturn(List.of(result).iterator());

    UpdateExecutionContext updateExecutionContext =
        new UpdateExecutionContext(null, mockTestMetadata());
    UpdateEntityResponse response =
        entityUpdater.update(updateEntityRequest, updateExecutionContext).build();
    // verify response
    assertTrue(response.hasEntity());
    assertEquals(BACKEND.name(), response.getEntity().getEntityType());
    assertEquals(2, response.getEntity().getAttributeCount());
    assertEquals(
        ValueType.STRING, response.getEntity().getAttributeMap().get("Test.id").getValueType());
    assertEquals("some-id", response.getEntity().getAttributeMap().get("Test.id").getString());
    assertEquals(
        ValueType.STRING, response.getEntity().getAttributeMap().get("Test.status").getValueType());
    assertEquals("RESOLVED", response.getEntity().getAttributeMap().get("Test.status").getString());

    // verify request that goes to EQS
    EntityUpdateRequest expectedEqsUpdateRequest =
        EntityUpdateRequest.newBuilder()
            .setEntityType(BACKEND.name())
            .addEntityIds("some-id")
            .setOperation(
                UpdateOperation.newBuilder()
                    .setSetAttribute(
                        org.hypertrace.entity.query.service.v1.SetAttribute.newBuilder()
                            .setAttribute(
                                org.hypertrace.entity.query.service.v1.ColumnIdentifier.newBuilder()
                                    .setColumnName("Test.status"))
                            .setValue(
                                org.hypertrace.entity.query.service.v1.LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                                            .setValueType(
                                                org.hypertrace.entity.query.service.v1.ValueType
                                                    .STRING)
                                            .setString("RESOLVED")))))
            .addSelection(
                org.hypertrace.entity.query.service.v1.Expression.newBuilder()
                    .setColumnIdentifier(
                        org.hypertrace.entity.query.service.v1.ColumnIdentifier.newBuilder()
                            .setColumnName("Test.id")))
            .addSelection(
                org.hypertrace.entity.query.service.v1.Expression.newBuilder()
                    .setColumnIdentifier(
                        org.hypertrace.entity.query.service.v1.ColumnIdentifier.newBuilder()
                            .setColumnName("Test.status")))
            .build();
    verify(mockEqsClient, times(1)).update(eq(expectedEqsUpdateRequest), any());
  }

  private UpdateEntityRequest createResolveTestRequest(String entityId) {
    UpdateEntityRequest updateEntityRequest =
        UpdateEntityRequest.newBuilder()
            .setEntityId(entityId)
            .setEntityType(BACKEND.name())
            .setOperation(
                UpdateEntityOperation.newBuilder()
                    .setSetAttribute(
                        SetAttribute.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder().setColumnName("Test.status"))
                            .setValue(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        Value.newBuilder()
                                            .setValueType(ValueType.STRING)
                                            .setString("RESOLVED")))))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("Test.id")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("Test.status")))
            .build();

    return updateEntityRequest;
  }

  private Map<String, AttributeMetadata> mockTestMetadata() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    AttributeMetadata stringMetadata =
        AttributeMetadata.newBuilder().setValueKind(AttributeKind.TYPE_STRING).build();
    attributeMetadataMap.put("Test.id", stringMetadata);
    attributeMetadataMap.put("Test.status", stringMetadata);

    return attributeMetadataMap;
  }
}
