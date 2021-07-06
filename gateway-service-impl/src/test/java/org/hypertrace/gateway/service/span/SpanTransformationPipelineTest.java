package org.hypertrace.gateway.service.span;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.span.SpanEvent;
import org.hypertrace.gateway.service.v1.span.SpanEvent.Builder;
import org.junit.jupiter.api.Test;

class SpanTransformationPipelineTest {

  private static final String ATTRIBUTE_KEY_ID = "id";

  private static final List<SpanEvent> ORIGINAL_SPANS =
      List.of(
          createSpan(Map.of(ATTRIBUTE_KEY_ID, "firstSpan")),
          createSpan(Map.of(ATTRIBUTE_KEY_ID, "secondSpan")));

  @Test
  void returnsUnchangedSpansIfNoTransformationsRegistered() {
    assertEquals(
        ORIGINAL_SPANS, SpanTransformationPipeline.getNewPipeline().execute(ORIGINAL_SPANS));
  }

  @Test
  void appliesMultipleTransformationsInOrderOfRegistry() {

    var originalSpans =
        List.of(
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, "firstSpan")),
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, "secondSpan")));

    // first transformation stage
    var firstTransformation = mock(SpanTransformationStage.class);
    var firstTransformationResult =
        List.of(
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, "firstTransformationFirstSpan")),
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, "firstTransformationSecondSpan")));

    when(firstTransformation.transform(anyList())).thenReturn(firstTransformationResult);

    // second transformation stage
    var secondTransformation = mock(SpanTransformationStage.class);
    var secondTransformationResult =
        List.of(
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, "secondTransformationFirstSpan")),
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, "secondTransformationSecondSpan")));

    when(secondTransformation.transform(eq(firstTransformationResult)))
        .thenReturn(secondTransformationResult);

    List<SpanEvent> pipelineTransformationExpectedResult =
        secondTransformationResult.stream().map(Builder::build).collect(toList());

    assertEquals(
        pipelineTransformationExpectedResult,
        SpanTransformationPipeline.getNewPipeline()
            .addProcessingStage(firstTransformation)
            .addProcessingStage(secondTransformation)
            .execute(ORIGINAL_SPANS));
  }

  private static SpanEvent.Builder createSpanBuilder(Map<String, String> attributes) {
    Builder builder = SpanEvent.newBuilder();
    attributes.forEach(
        (attributeName, attributeValue) -> {
          builder.putAttributes(
              attributeName,
              Value.newBuilder().setValueType(ValueType.STRING).setString(attributeValue).build());
        });
    return builder;
  }

  private static SpanEvent createSpan(Map<String, String> attributes) {
    return createSpanBuilder(attributes).build();
  }
}
