package org.hypertrace.gateway.service.span;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

class JaegarBasedClockskewAdjuster extends ClockskewAdjuster {

  @Override
  public List<SpanEvent.Builder> transform(@NonNull List<? extends SpanEvent.Builder> spans) {
    Map<String, String> parentChildMap = new HashMap<>();
    Map<String, Span> idToSpanMap =
        spans.stream()
            .map(
                spanBuilder -> {
                  Map<String, Value> attributesMap = spanBuilder.getAttributesMap();
                  String spanId = attributesMap.get("EVENT.id").getString(),
                      parentSpanId = attributesMap.get("EVENT.parentSpanId").getString();
                  Instant
                      startTime =
                          Instant.ofEpochMilli(attributesMap.get("EVENT.startTime").getLong()),
                      endTime = Instant.ofEpochMilli(attributesMap.get("EVENT.endTime").getLong());
                  Duration duration = Duration.between(startTime, endTime);
                  return new Span()
                      .id(spanId)
                      .parentSpanId(parentSpanId)
                      .startTime(startTime)
                      .endTime(endTime)
                      .duration(duration)
                      .spanBuilder(spanBuilder);
                })
            .peek(
                span -> {
                  if (null != span.parentSpanId()) {
                    parentChildMap.putIfAbsent(span.parentSpanId(), span.id());
                  }
                })
            .collect(toMap(Span::id, span -> span));
    // Start from root spans, and adjust each parent-child pair
    return idToSpanMap.entrySet().stream()
        .filter(span -> null == span.getValue().parentSpanId())
        .peek(entry -> adjustSpan(entry.getValue(), idToSpanMap, parentChildMap))
        .map(
            entry -> {
              Span updatedSpan = entry.getValue();
              entry
                  .getValue()
                  .spanBuilder()
                  .putAttributes(
                      "EVENT.startTime",
                      Value.newBuilder()
                          .setValueType(ValueType.LONG)
                          .setLong(updatedSpan.startTime().toEpochMilli())
                          .build());
              return updatedSpan.spanBuilder();
            })
        .collect(toList());
  }

  private void adjustSpan(
      Span span, Map<String, Span> idToSpanMap, Map<String, String> parentToChildMap) {
    if (null != span) {
      Span childSpan = idToSpanMap.get(parentToChildMap.get(span.id()));
      Duration adjustment = getAdjustmentForChildSpan(childSpan, span);
      adjustTimestamp(childSpan, adjustment);
      adjustSpan(childSpan, idToSpanMap, parentToChildMap);
    }
  }

  private void adjustTimestamp(Span childSpan, Duration adjustment) {
    childSpan.startTime().plus(adjustment);
  }

  private Duration getAdjustmentForChildSpan(Span childSpan, Span parentSpan) {
    // if child span is greater than parent span
    if (childSpan.duration().compareTo(parentSpan.duration()) > 0) {
      // in this case, we can only ensure that it does not start before its parent
      if (childSpan.startTime().isBefore(parentSpan.startTime())) {
        return Duration.between(parentSpan.startTime(), childSpan.startTime());
      }
      return Duration.ofMillis(0);
    }
    // if child already fits in its parent, do not adjust
    if (!childSpan.startTime().isBefore(parentSpan.startTime())
        && !childSpan.endTime().isAfter(parentSpan.endTime())) {
      return Duration.ofMillis(0);
    }
    var latency = (parentSpan.duration().minus(childSpan.duration()).toMillis()) >> 1;
    return Duration.between(childSpan.startTime(), parentSpan.startTime().plusMillis(latency));
  }
}
