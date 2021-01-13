package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class BaselineEntitiesRequestValidatorTest {
  @Test
  public void emptyEntityIds_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("API")
            .addAllEntityIds(Collections.emptyList())
            .setStartTimeMillis(Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli())
            .build();
    BaselineEntitiesRequestValidator baselineEntitiesRequestValidator =
        new BaselineEntitiesRequestValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          baselineEntitiesRequestValidator.validate(baselineEntitiesRequest, attributeMetadataMap);
        });
  }

  @Test
  public void emptyAggregationsAndTimeSeries_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .setEntityType("API")
            .addAllEntityIds(Collections.singleton("entity1"))
            .setStartTimeMillis(Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli())
            .addAllBaselineAggregateRequest(Collections.emptyList())
            .addAllBaselineMetricSeriesRequest(Collections.emptyList())
            .build();
    BaselineEntitiesRequestValidator baselineEntitiesRequestValidator =
        new BaselineEntitiesRequestValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          baselineEntitiesRequestValidator.validate(baselineEntitiesRequest, attributeMetadataMap);
        });
  }

  @Test
  public void entityTypeNotPresent_shouldThrowIllegalArgException() {
    Map<String, AttributeMetadata> attributeMetadataMap = new HashMap<>();
    BaselineEntitiesRequest baselineEntitiesRequest =
        BaselineEntitiesRequest.newBuilder()
            .addAllEntityIds(Collections.singleton("entity1"))
            .setStartTimeMillis(Instant.parse("2020-11-14T17:40:51.902Z").toEpochMilli())
            .setEndTimeMillis(Instant.parse("2020-11-14T18:40:51.902Z").toEpochMilli())
            .build();
    BaselineEntitiesRequestValidator baselineEntitiesRequestValidator =
        new BaselineEntitiesRequestValidator();
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          baselineEntitiesRequestValidator.validate(baselineEntitiesRequest, attributeMetadataMap);
        });
  }
}
