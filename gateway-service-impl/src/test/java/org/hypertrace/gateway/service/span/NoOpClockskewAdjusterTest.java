package org.hypertrace.gateway.service.span;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.span.SpanEvent;
import org.hypertrace.gateway.service.v1.span.SpanEvent.Builder;
import org.junit.jupiter.api.Test;

class NoOpClockskewAdjusterTest {

  private static final ClockskewAdjuster CLOCKSKEW_ADJUSTER = new NoOpClockskewAdjuster();

  private static final String ATTRIBUTE_KEY_ID = "id";

  @Test
  void returnsUnchangedSpanList() {
    var originalSpans =
        List.of(
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, UUID.randomUUID().toString())),
            createSpanBuilder(Map.of(ATTRIBUTE_KEY_ID, UUID.randomUUID().toString())));
    assertEquals(originalSpans, CLOCKSKEW_ADJUSTER.transform(originalSpans));
  }

  @Test
  void throwsNPEWhenNullSpanListArePassed() {
    assertThrows(NullPointerException.class, () -> CLOCKSKEW_ADJUSTER.transform(null));
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
}
