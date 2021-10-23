package org.hypertrace.gateway.service.explore;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeType;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Period;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExploreRequestValidatorTest {
  @Test
  public void simpleSelectionsOnColumns_shouldBeValid() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "apiName",
        AttributeMetadata.newBuilder().setFqn("apiName").setType(AttributeType.ATTRIBUTE).build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();
    exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
  }

  @Test
  public void aggregatedSelections_shouldBeValid() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "duration",
        AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    attributeMetadataMap.put(
        "callersCount",
        AttributeMetadata.newBuilder()
            .setFqn("callersCount")
            .setType(AttributeType.METRIC)
            .build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-18T16:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-18T17:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder().setColumnName("duration")))))
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.MAX)
                            .setAlias("MAX_callersCount")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("callersCount")))))
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVGRATE)
                            .setAlias("RATE_Duration")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder().setColumnName("duration")))
                            .addArguments(
                                Expression.newBuilder()
                                    .setLiteral(
                                        LiteralConstant.newBuilder()
                                            .setValue(
                                                Value.newBuilder()
                                                    .setValueType(ValueType.LONG)
                                                    .setLong(30))))))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();
    exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
  }

  @Test
  public void aggregationsWithGroupBy_shouldBeValid() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "callersCount",
        AttributeMetadata.newBuilder()
            .setFqn("callersCount")
            .setType(AttributeType.METRIC)
            .build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_callersCount")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("callersCount")))))
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .setLimit(3)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();
    exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
  }

  @Test
  public void timeAggregationsAkaIntervals_shouldBeValid() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "duration",
        AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    attributeMetadataMap.put(
        "callersCount",
        AttributeMetadata.newBuilder()
            .setFqn("callersCount")
            .setType(AttributeType.METRIC)
            .build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-18T17:22:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-18T17:26:51.902Z").toEpochMilli())
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("duration"))))))
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.MAX)
                                    .setAlias("MAX_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("duration"))))))
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.MIN)
                                    .setAlias("MIN_callersCount")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("callersCount"))))))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();
    exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
  }

  @Test
  public void timeAggregationsWithGroupByShouldBeValid() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "duration",
        AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-18T17:22:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-18T17:26:51.902Z").toEpochMilli())
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("duration"))))))
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .setLimit(4)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();
    exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
  }

  @Test
  public void requestWithZeroLimit_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("duration"))))))
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void groupByIsSpecifiedAndSelectionsAreNotAggregations_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "apiName",
        AttributeMetadata.newBuilder().setFqn("apiName").setType(AttributeType.ATTRIBUTE).build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-18T17:22:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-18T17:26:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .setLimit(4)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void requestWithNoSelectionsOrTimeAggregations_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void requestWithNoContext_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void requestWithBothTimeAggregationsAndSelections_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "apiName",
        AttributeMetadata.newBuilder().setFqn("apiName").setType(AttributeType.ATTRIBUTE).build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("duration"))))))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void requestWithBothColumnAndFunctionSelections_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "apiName",
        AttributeMetadata.newBuilder().setFqn("apiName").setType(AttributeType.ATTRIBUTE).build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_callersCount")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("callersCount")))))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void requestsWithMultipleGroupBys_shouldBeValid() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "apiName",
        AttributeMetadata.newBuilder().setFqn("apiName").setType(AttributeType.ATTRIBUTE).build());
    attributeMetadataMap.put(
        "callersCount",
        AttributeMetadata.newBuilder()
            .setFqn("callersCount")
            .setType(AttributeType.METRIC)
            .build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_callersCount")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("callersCount")))))
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .addGroupBy(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
            .setLimit(3)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();
    exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
  }

  @Test
  public void requestsWithFunctionGroupBy_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "serviceName",
        AttributeMetadata.newBuilder()
            .setFqn("serviceName")
            .setType(AttributeType.ATTRIBUTE)
            .build());
    attributeMetadataMap.put(
        "callersCount",
        AttributeMetadata.newBuilder()
            .setFqn("callersCount")
            .setType(AttributeType.METRIC)
            .build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_callersCount")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("callersCount")))))
            .addGroupBy(
                Expression.newBuilder()
                    .setFunction(
                        FunctionExpression.newBuilder()
                            .setFunction(FunctionType.AVG)
                            .setAlias("AVG_CallersCount")
                            .addArguments(
                                Expression.newBuilder()
                                    .setColumnIdentifier(
                                        ColumnIdentifier.newBuilder()
                                            .setColumnName("callersCount")))))
            .setLimit(3)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void timeAggregationsWithDifferentPeriods_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    attributeMetadataMap.put(
        "duration",
        AttributeMetadata.newBuilder().setFqn("duration").setType(AttributeType.ATTRIBUTE).build());
    attributeMetadataMap.put(
        "callersCount",
        AttributeMetadata.newBuilder()
            .setFqn("callersCount")
            .setType(AttributeType.METRIC)
            .build());

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-18T17:22:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-18T17:26:51.902Z").toEpochMilli())
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.AVG)
                                    .setAlias("AVG_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("duration"))))))
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(30))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.MAX)
                                    .setAlias("MAX_Duration")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("duration"))))))
            .addTimeAggregation(
                TimeAggregation.newBuilder()
                    .setPeriod(Period.newBuilder().setUnit("SECONDS").setValue(60))
                    .setAggregation(
                        Expression.newBuilder()
                            .setFunction(
                                FunctionExpression.newBuilder()
                                    .setFunction(FunctionType.MIN)
                                    .setAlias("MIN_callersCount")
                                    .addArguments(
                                        Expression.newBuilder()
                                            .setColumnIdentifier(
                                                ColumnIdentifier.newBuilder()
                                                    .setColumnName("callersCount"))))))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }

  @Test
  public void selectingOnUnknownAttributes_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();

    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API_TRACE")
            .setStartTimeMillis(Instant.parse("2019-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2019-11-14T18:40:51.902Z").toEpochMilli())
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder().setColumnName("serviceName")))
            .addSelection(
                Expression.newBuilder()
                    .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("apiName")))
            .setLimit(10)
            .build();

    ExploreRequestValidator exploreRequestValidator = new ExploreRequestValidator();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          exploreRequestValidator.validate(exploreRequest, attributeMetadataMap);
        });
  }
}
