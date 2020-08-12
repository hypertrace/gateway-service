package org.hypertrace.gateway.service.entity;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EntitiesRequestValidatorTest {

  private EntitiesRequestValidator validator;

  @BeforeEach
  public void setup() {
    validator = new EntitiesRequestValidator();
  }

  @Test
  public void whenNonExistentAttributeIsUsed_thenExpectIllegalArgumentException() {
    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
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
  public void whenTimeAggregationHasNoPeriod_thenExpectIllegalArgumentException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setAggregation(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "Service.name", FunctionType.SUM, "name"))
            .build();

    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .addAllTimeAggregation(Collections.singletonList(timeAggregation))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "Service.name",
        AttributeMetadata.newBuilder()
            .setFqn("Service.name")
            .setType(AttributeType.ATTRIBUTE)
            .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validate(request, attributeMetadataMap);
        });
  }

  @Test
  public void whenTimeAggregationHasFuncExprOnAttribute_thenDontThrowIllegalArgumentException() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(30).setUnit("SECONDS"))
            .setAggregation(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "duration", FunctionType.SUM, "SUM_duration"))
            .build();

    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .addAllTimeAggregation(Collections.singletonList(timeAggregation))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "duration",
        AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    validator.validate(request, attributeMetadataMap);
  }

  // We allow at most 10K intervals for time series. See TimeAggregationValidator.MAX_RESULTS for
  // the enforcement.
  @Test
  public void whenTimeAggregationExpectedResultsIsWithinBounds_thenDontThrowIllegalArgumentException() {
    TimeAggregation timeAggregation =
            TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setValue(30).setUnit("SECONDS"))
                    .setAggregation(
                            QueryExpressionUtil.getAggregateFunctionExpression(
                                    "duration", FunctionType.SUM, "SUM_duration"))
                    .build();

    EntitiesRequest request =
            EntitiesRequest.newBuilder()
                    .setStartTimeMillis(0L)
                    .setEndTimeMillis(900L)
                    .setEntityType(DomainEntityType.SERVICE.name())
                    .addAllTimeAggregation(Collections.singletonList(timeAggregation))
                    .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
            "duration",
            AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    validator.validate(request, attributeMetadataMap);
  }

  @Test
  public void whenTimeAggregationExpectedResultsNotWithinBounds_ThrowIllegalStateException() {
    TimeAggregation timeAggregation =
            TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setValue(30).setUnit("SECONDS"))
                    .setAggregation(
                            QueryExpressionUtil.getAggregateFunctionExpression(
                                    "duration", FunctionType.SUM, "SUM_duration"))
                    .build();

    EntitiesRequest request =
            EntitiesRequest.newBuilder()
                    .setStartTimeMillis(0L)
                    .setEndTimeMillis(300030L * 1000L)
                    .setEntityType(DomainEntityType.SERVICE.name())
                    .addAllTimeAggregation(Collections.singletonList(timeAggregation))
                    .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
            "duration",
            AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    Assertions.assertThrows(
            IllegalStateException.class,
            () -> {
              validator.validate(request, attributeMetadataMap);
            });
  }

  @Test
  public void whenFuncExprHasEmptyAlias_thenExpectIllegalArgumentException() {
    Expression fexpr =
        QueryExpressionUtil.getAggregateFunctionExpression("Service.name", FunctionType.SUM, "")
            .build();

    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .addAllSelection(Collections.singletonList(fexpr))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "Service.name",
        AttributeMetadata.newBuilder()
            .setFqn("Service.name")
            .setType(AttributeType.ATTRIBUTE)
            .build());

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          validator.validate(request, attributeMetadataMap);
        });
  }

  @Test
  public void whenFuncExprHasAttributeTypeForAttribute_thenNoIllegalArgumentExceptionThrown() {
    Expression fexpr =
        QueryExpressionUtil.getAggregateFunctionExpression(
                "duration", FunctionType.AVG, "AVG_duration")
            .build();

    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .addAllSelection(Collections.singletonList(fexpr))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "duration",
        AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    validator.validate(request, attributeMetadataMap);
  }

  @Test
  public void whenExistingAttributeIsUsed_thenNoExceptionShouldBeThrown() {
    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
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
  public void whenValidMetricTypeAttributeIsUsedInTimeAgg_thenNoExceptionShouldBeThrown() {
    TimeAggregation timeAggregation =
        TimeAggregation.newBuilder()
            .setPeriod(Period.newBuilder().setValue(30).setUnit(ChronoUnit.SECONDS.name()))
            .setAggregation(
                QueryExpressionUtil.getAggregateFunctionExpression(
                    "Service.name", FunctionType.SUM, "name"))
            .build();

    EntitiesRequest request =
        EntitiesRequest.newBuilder()
            .setEntityType(DomainEntityType.SERVICE.name())
            .addAllTimeAggregation(Collections.singletonList(timeAggregation))
            .build();

    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "Service.name",
        AttributeMetadata.newBuilder()
            .setFqn("Service.name")
            .setType(AttributeType.METRIC)
            .build());
    validator.validate(request, attributeMetadataMap);
  }
}
