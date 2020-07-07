package org.hypertrace.gateway.service.trace;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TracesRequestValidatorTest {

  private TracesRequestValidator validator;

  @BeforeEach
  public void setup() {
    validator = new TracesRequestValidator();
  }

  @Test
  public void test_validate_nonExistentAttributeIsUsed_thenExpectIllegalArgumentException() {
    TracesRequest request =
        TracesRequest.newBuilder()
            .setScope(AttributeScope.API_TRACE.name())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("foo_name")))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validate(request, attributeMetadataMap);
        });
  }

  @Test
  public void test_validate_funcExprHasEmptyAlias_thenExpectIllegalArgumentException() {
    Expression fexpr =
        QueryExpressionUtil.getAggregateFunctionExpression(
                "Api.Trace.duration", FunctionType.SUM, "")
            .build();

    TracesRequest request =
        TracesRequest.newBuilder()
            .setScope(AttributeScope.API_TRACE.name())
            .addAllSelection(Collections.singletonList(fexpr))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "Api.Trace.duration",
        AttributeMetadata.newBuilder()
            .setFqn("Api.Trace.duration")
            .setType(AttributeType.ATTRIBUTE)
            .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validate(request, attributeMetadataMap);
        });
  }

  @Test
  public void
      test_validate_funcExprHasAttributeTypeForAttribute_thenNoIllegalArgumentExceptionThrown() {
    Expression fexpr =
        QueryExpressionUtil.getAggregateFunctionExpression(
                "duration", FunctionType.AVG, "AVG_duration")
            .build();

    TracesRequest request =
        TracesRequest.newBuilder()
            .setScope(AttributeScope.API_TRACE.name())
            .addAllSelection(Collections.singletonList(fexpr))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "duration",
        AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    validator.validate(request, attributeMetadataMap);
  }

  @Test
  public void test_validate_existingAttributeIsUsed_thenNoExceptionShouldBeThrown() {
    TracesRequest request =
        TracesRequest.newBuilder()
            .setScope(AttributeScope.API_TRACE.name())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("foo_name")))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "foo_name",
        AttributeMetadata.newBuilder().setType(AttributeType.ATTRIBUTE).setFqn("foo_name").build());
    validator.validate(request, attributeMetadataMap);
  }

  @Test
  public void test_validate_emptySelectionList_thenIllegalArgumentExceptionThrown() {
    TracesRequest request =
        TracesRequest.newBuilder().setScope(AttributeScope.API_TRACE.name()).build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validate(request, attributeMetadataMap);
        });
  }

  @Test
  public void test_validate_noScope_thenIllegalArgumentExceptionThrown() {
    TracesRequest request = TracesRequest.newBuilder().build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validateScope(request);
        });
  }

  @Test
  public void test_validate_undefinedScope_thenIllegalArgumentExceptionThrown() {
    TracesRequest request = TracesRequest.newBuilder().setScope("SCOPE_UNDEFINED").build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validateScope(request);
        });
  }

  @Test
  public void test_validate_unKnownScope_thenIllegalArgumentExceptionThrown() {
    TracesRequest request = TracesRequest.newBuilder().setScope("UNKNOWN").build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validateScope(request);
        });
  }

  @Test
  public void test_validate_unHandledScope_thenIllegalArgumentExceptionThrown() {
    TracesRequest request = TracesRequest.newBuilder().setScope("SERVICE").build();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validateScope(request);
        });
  }
}
