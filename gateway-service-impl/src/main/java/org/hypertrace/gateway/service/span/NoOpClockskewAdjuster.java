package org.hypertrace.gateway.service.span;

import java.util.ArrayList;
import java.util.List;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

public class NoOpClockskewAdjuster implements ClockskewAdjuster{
  public List<SpanEvent> process(List<? extends SpanEvent> spans) {
    return new ArrayList<>(spans);
  }
}
