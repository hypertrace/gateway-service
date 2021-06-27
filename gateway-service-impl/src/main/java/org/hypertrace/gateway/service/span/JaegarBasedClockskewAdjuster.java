package org.hypertrace.gateway.service.span;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.experimental.Accessors;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.span.SpanEvent;
import org.hypertrace.gateway.service.v1.span.SpanEvent.Builder;

class JaegarBasedClockskewAdjuster implements ClockskewAdjuster {

  @Override
  public List<SpanEvent> adjustSpansForClockSkew(ImmutableList<SpanEvent> spans) {
    Map<String, Span> idToSpanMap =
        spans.parallelStream()
            .map(
                spanEvent -> {
                  Map<String, Value> attrMap = spanEvent.getAttributesMap();
                  String spanId = attrMap.get("EVENT.id").getString();
                  String parentSpanId = attrMap.get("EVENT.parentSpanId").getString();
                  long startTime = attrMap.get("EVENT.startTime").getLong();
                  long endTime = attrMap.get("EVENT.endTime").getLong();
                  return new Span()
                      .id(spanId)
                      .startTime(Instant.ofEpochMilli(startTime))
                      .endTime(Instant.ofEpochMilli(endTime))
                      .parentSpanId(parentSpanId)
                      .duration(
                          Duration.between(
                              Instant.ofEpochMilli(endTime), Instant.ofEpochMilli(startTime)))
                      .spanEvent(spanEvent);
                })
            .collect(toMap(Span::id, span -> span));
    // update child spanId of each span
    idToSpanMap.forEach((spanId, span) -> idToSpanMap.get(span.parentSpanId()).childSpanId(spanId));
    // Start from root spans, and adjust each parent-child pair
    idToSpanMap.entrySet().stream()
        .filter(entry -> null == entry.getValue().parentSpanId())
        .forEach(entry -> adjustSpan(entry.getValue(), idToSpanMap));
    return idToSpanMap.values().stream().map(Span::getAdjustedSpanEvent).collect(toList());
  }

  private void adjustSpan(Span span, Map<String, Span> idToSpanMap) {
    if (null != span && null != span.childSpanId()) {
      Span childSpan = idToSpanMap.get(span.childSpanId());
      Duration adjustment = getAdjustmentForChildSpan(childSpan, span);
      adjustTimestamp(childSpan, adjustment);
      adjustSpan(childSpan, idToSpanMap);
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

  @Data
  @Accessors(chain = true, fluent = true)
  private static class Span {
    private String id;
    private String parentSpanId;
    private String childSpanId;
    private Instant startTime;
    private Instant endTime;
    private Duration duration;
    private SpanEvent spanEvent;

    public SpanEvent getAdjustedSpanEvent() {
      Preconditions.checkArgument(null != spanEvent);
      Preconditions.checkArgument(null != startTime);
      Preconditions.checkArgument(null != endTime);
      Builder builder = SpanEvent.newBuilder().putAllAttributes(spanEvent.getAttributesMap());
      builder.putAttributes(
          "EVENT.startTime",
          Value.newBuilder()
              .setLong(startTime.toEpochMilli())
              .setValueType(ValueType.LONG)
              .build());
      builder.putAttributes(
          "EVENT.endTime",
          Value.newBuilder()
              .setLong(endTime.toEpochMilli())
              .setValueType(ValueType.LONG)
              .build());
      return builder.build();
    }
  }
}
