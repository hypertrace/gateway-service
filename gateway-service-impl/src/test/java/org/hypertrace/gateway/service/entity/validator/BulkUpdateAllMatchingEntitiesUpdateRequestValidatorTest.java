package org.hypertrace.gateway.service.entity.validator;

import static io.grpc.Status.INVALID_ARGUMENT;
import static org.hypertrace.gateway.service.common.util.QueryExpressionUtil.buildStringFilter;
import static org.hypertrace.gateway.service.v1.common.ValueType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import io.grpc.Status;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Update;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class BulkUpdateAllMatchingEntitiesUpdateRequestValidatorTest {
  private static final String TEST_TENANT_ID = "test_tenant_id";

  @Mock private AttributeMetadataProvider mockMetadataProvider;
  @Mock private RequestContext mockContext;
  private BulkUpdateAllMatchingEntitiesRequest request;

  private BulkUpdateAllMatchingEntitiesUpdateRequestValidator
      bulkUpdateAllMatchingEntitiesUpdateRequestValidator;

  private AutoCloseable mockitoCloseable;
  private Map<String, AttributeMetadata> attributeMetadataMap;
  private RequestContext context;

  @BeforeEach
  void setUp() {
    mockitoCloseable = openMocks(this);
    request =
        BulkUpdateAllMatchingEntitiesRequest.newBuilder()
            .setEntityType("API")
            .addUpdates(
                Update.newBuilder()
                    .setFilter(
                        buildStringFilter(
                            "API.apiId",
                            org.hypertrace.gateway.service.v1.common.Operator.EQ,
                            "some-api"))
                    .addOperations(
                        UpdateOperation.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder().setColumnName("API.httpMethod"))
                            .setOperator(UpdateOperation.Operator.OPERATOR_SET)
                            .setValue(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                                            .setValueType(STRING)
                                            .setString("GET")))))
            .build();
    attributeMetadataMap =
        Map.ofEntries(
            Map.entry(
                "API.apiId",
                AttributeMetadata.newBuilder().addSources(AttributeSource.EDS).build()),
            Map.entry(
                "API.qsSource",
                AttributeMetadata.newBuilder().addSources(AttributeSource.QS).build()),
            Map.entry(
                "API.httpMethod",
                AttributeMetadata.newBuilder().addSources(AttributeSource.EDS).build()));
    context =
        new RequestContext(
            org.hypertrace.core.grpcutils.context.RequestContext.forTenantId(TEST_TENANT_ID));
    when(mockMetadataProvider.getAttributesMetadata(any(RequestContext.class), eq("API")))
        .thenReturn(attributeMetadataMap);
  }

  @AfterEach
  void tearDown() throws Exception {
    mockitoCloseable.close();
  }

  @Test
  void testValidateValid() {
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider, request, mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(Status.OK, result);
  }

  @Test
  void testValidateWithoutEntityType() {
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider, request.toBuilder().clearEntityType().build(), mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(INVALID_ARGUMENT.getCode(), result.getCode());
    assertEquals("entity_type is mandatory in the request.", result.getDescription());
  }

  @Test
  void testValidateWithoutUpdates() {
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider, request.toBuilder().clearUpdates().build(), mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(INVALID_ARGUMENT.getCode(), result.getCode());
    assertEquals("updates are mandatory in the request.", result.getDescription());
  }

  @Test
  void testValidateWithoutFilter() {
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider,
            request.toBuilder().addUpdates(Update.newBuilder()).build(),
            mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(INVALID_ARGUMENT.getCode(), result.getCode());
    assertEquals("filters are mandatory in the request", result.getDescription());
  }

  @Test
  void testValidateWithoutOperator() {
    request =
        BulkUpdateAllMatchingEntitiesRequest.newBuilder()
            .setEntityType("API")
            .addUpdates(
                Update.newBuilder()
                    .setFilter(
                        buildStringFilter(
                            "API.apiId",
                            org.hypertrace.gateway.service.v1.common.Operator.EQ,
                            "some-api"))
                    .addOperations(
                        UpdateOperation.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder().setColumnName("API.httpMethod"))
                            .setValue(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                                            .setValueType(STRING)
                                            .setString("GET")))))
            .build();
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider, request, mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(INVALID_ARGUMENT.getCode(), result.getCode());
    assertEquals("operators are mandatory in the request", result.getDescription());
  }

  @Test
  void testValidateWithoutAttributeId() {
    request =
        BulkUpdateAllMatchingEntitiesRequest.newBuilder()
            .setEntityType("API")
            .addUpdates(
                Update.newBuilder()
                    .setFilter(
                        buildStringFilter(
                            "API.apiId",
                            org.hypertrace.gateway.service.v1.common.Operator.EQ,
                            "some-api"))
                    .addOperations(
                        UpdateOperation.newBuilder()
                            .setOperator(Operator.OPERATOR_SET)
                            .setValue(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                                            .setValueType(STRING)
                                            .setString("GET")))))
            .build();
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider, request, mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(INVALID_ARGUMENT.getCode(), result.getCode());
    assertEquals("attribute ids are mandatory in the request", result.getDescription());
  }

  @Test
  void testValidateWithInvalidAttributeId() {
    request =
        BulkUpdateAllMatchingEntitiesRequest.newBuilder()
            .setEntityType("API")
            .addUpdates(
                Update.newBuilder()
                    .setFilter(
                        buildStringFilter(
                            "API.apiId",
                            org.hypertrace.gateway.service.v1.common.Operator.EQ,
                            "some-api"))
                    .addOperations(
                        UpdateOperation.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder().setColumnName("API.http_method"))
                            .setOperator(Operator.OPERATOR_SET)
                            .setValue(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                                            .setValueType(STRING)
                                            .setString("GET")))))
            .build();
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider, request, mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(INVALID_ARGUMENT.getCode(), result.getCode());
    assertEquals("Attribute API.http_method does not exist", result.getDescription());
  }

  @Test
  void testValidateWithInvalidSource() {
    request =
        BulkUpdateAllMatchingEntitiesRequest.newBuilder()
            .setEntityType("API")
            .addUpdates(
                Update.newBuilder()
                    .setFilter(
                        buildStringFilter(
                            "API.apiId",
                            org.hypertrace.gateway.service.v1.common.Operator.EQ,
                            "some-api"))
                    .addOperations(
                        UpdateOperation.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder().setColumnName("API.qsSource"))
                            .setOperator(Operator.OPERATOR_SET)
                            .setValue(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.gateway.service.v1.common.Value.newBuilder()
                                            .setValueType(STRING)
                                            .setString("GET")))))
            .build();
    bulkUpdateAllMatchingEntitiesUpdateRequestValidator =
        new BulkUpdateAllMatchingEntitiesUpdateRequestValidator(
            mockMetadataProvider, request, mockContext);
    final Status result = bulkUpdateAllMatchingEntitiesUpdateRequestValidator.validate();
    assertEquals(INVALID_ARGUMENT.getCode(), result.getCode());
    assertEquals("Only EDS attributes are supported for update right now", result.getDescription());
  }
}
