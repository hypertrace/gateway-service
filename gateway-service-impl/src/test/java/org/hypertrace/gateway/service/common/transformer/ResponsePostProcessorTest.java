package org.hypertrace.gateway.service.common.transformer;

import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.v1.common.AggregatedMetricValue;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.Interval;
import org.hypertrace.gateway.service.v1.common.MetricSeries;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResponsePostProcessorTest {
  private ResponsePostProcessor responsePostProcessor;

  @BeforeEach
  public void setup() {
    this.responsePostProcessor = new ResponsePostProcessor();
  }

  @Test
  public void shouldAddMissingSelections() {
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getSourceToSelectionExpressionMap())
        .thenReturn(
            Map.of(
                "EDS",
                List.of(createAttributeExpression("API.id"), createAttributeExpression("API.name")),
                "QS",
                List.of(createAttributeExpression("API.serviceId"))));
    when(executionContext.getSourceToMetricExpressionMap()).thenReturn(Collections.emptyMap());
    when(executionContext.getSourceToTimeAggregationMap()).thenReturn(Collections.emptyMap());

    List<Entity.Builder> entityBuilders =
        List.of(
            Entity.newBuilder().putAttribute("API.id", Value.newBuilder().setString("123").build()),
            Entity.newBuilder()
                .putAttribute("API.id", Value.newBuilder().setString("234").build())
                .putAttribute("API.name", Value.newBuilder().setString("api2").build()));

    List<Entity.Builder> postProcessedEntityBuilders =
        this.responsePostProcessor.transform(executionContext, entityBuilders);
    assertEquals(2, postProcessedEntityBuilders.size());

    Entity.Builder firstEntityBuilder = postProcessedEntityBuilders.get(0);
    assertEquals(3, firstEntityBuilder.getAttributeCount());
    assertEquals(
        Value.newBuilder().setString("123").build(),
        firstEntityBuilder.getAttributeMap().get("API.id"));
    assertEquals(Value.getDefaultInstance(), firstEntityBuilder.getAttributeMap().get("API.name"));
    assertEquals(
        Value.getDefaultInstance(), firstEntityBuilder.getAttributeMap().get("API.serviceId"));

    Entity.Builder secondEntityBuilder = postProcessedEntityBuilders.get(1);
    assertEquals(3, secondEntityBuilder.getAttributeCount());
    assertEquals(
        Value.newBuilder().setString("234").build(),
        secondEntityBuilder.getAttributeMap().get("API.id"));
    assertEquals(
        Value.newBuilder().setString("api2").build(),
        secondEntityBuilder.getAttributeMap().get("API.name"));
    assertEquals(
        Value.getDefaultInstance(), secondEntityBuilder.getAttributeMap().get("API.serviceId"));
  }

  @Test
  public void shouldAddMissingAggregations() {
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getSourceToSelectionExpressionMap()).thenReturn(Collections.emptyMap());
    when(executionContext.getSourceToMetricExpressionMap())
        .thenReturn(Map.of("QS", List.of(createAggregateExpression("API.duration"))));
    when(executionContext.getSourceToTimeAggregationMap()).thenReturn(Collections.emptyMap());

    List<Entity.Builder> entityBuilders =
        List.of(
            Entity.newBuilder()
                .putMetric(
                    "API.duration",
                    AggregatedMetricValue.newBuilder()
                        .setValue(Value.newBuilder().setLong(1).build())
                        .build()),
            Entity.newBuilder());

    List<Entity.Builder> postProcessedEntityBuilders =
        this.responsePostProcessor.transform(executionContext, entityBuilders);
    assertEquals(2, postProcessedEntityBuilders.size());

    Entity.Builder firstEntityBuilder = postProcessedEntityBuilders.get(0);
    assertEquals(1, firstEntityBuilder.getMetricCount());
    assertEquals(
        AggregatedMetricValue.newBuilder().setValue(Value.newBuilder().setLong(1).build()).build(),
        firstEntityBuilder.getMetricMap().get("API.duration"));

    Entity.Builder secondEntityBuilder = postProcessedEntityBuilders.get(1);
    assertEquals(1, secondEntityBuilder.getMetricCount());
    assertEquals(
        AggregatedMetricValue.getDefaultInstance(),
        secondEntityBuilder.getMetricMap().get("API.duration"));
  }

  @Test
  public void shouldAddMissingTimeAggregations() {
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getSourceToSelectionExpressionMap()).thenReturn(Collections.emptyMap());
    when(executionContext.getSourceToMetricExpressionMap()).thenReturn(Collections.emptyMap());
    when(executionContext.getSourceToTimeAggregationMap())
        .thenReturn(Map.of("QS", List.of(createTimeAggregation("API.duration"))));

    List<Entity.Builder> entityBuilders =
        List.of(
            Entity.newBuilder()
                .putMetricSeries(
                    "API.duration",
                    MetricSeries.newBuilder()
                        .addValue(
                            Interval.newBuilder()
                                .setValue(Value.newBuilder().setLong(2).build())
                                .build())
                        .build()),
            Entity.newBuilder());

    List<Entity.Builder> postProcessedEntityBuilders =
        this.responsePostProcessor.transform(executionContext, entityBuilders);
    assertEquals(2, postProcessedEntityBuilders.size());

    Entity.Builder firstEntityBuilder = postProcessedEntityBuilders.get(0);
    assertEquals(1, firstEntityBuilder.getMetricSeriesCount());
    assertEquals(
        MetricSeries.newBuilder()
            .addValue(Interval.newBuilder().setValue(Value.newBuilder().setLong(2).build()).build())
            .build(),
        firstEntityBuilder.getMetricSeriesMap().get("API.duration"));

    Entity.Builder secondEntityBuilder = postProcessedEntityBuilders.get(1);
    assertEquals(1, secondEntityBuilder.getMetricSeriesCount());
    assertEquals(
        MetricSeries.getDefaultInstance(),
        secondEntityBuilder.getMetricSeriesMap().get("API.duration"));
  }

  private Expression createAttributeExpression(String attributeKey) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setAlias(attributeKey).build())
        .build();
  }

  private Expression createAggregateExpression(String attributeKey) {
    return Expression.newBuilder()
        .setFunction(FunctionExpression.newBuilder().setAlias(attributeKey).build())
        .build();
  }

  private TimeAggregation createTimeAggregation(String attributeKey) {
    return TimeAggregation.newBuilder()
        .setAggregation(
            Expression.newBuilder()
                .setFunction(FunctionExpression.newBuilder().setAlias(attributeKey).build()))
        .build();
  }
}
