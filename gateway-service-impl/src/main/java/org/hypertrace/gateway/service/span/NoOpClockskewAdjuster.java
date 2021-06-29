package org.hypertrace.gateway.service.span;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

public class NoOpClockskewAdjuster implements ClockskewAdjuster{
  public List<SpanEvent.Builder> process(List<? extends SpanEvent.Builder> spans) {
    return new ArrayList<>(spans);
  }
}
