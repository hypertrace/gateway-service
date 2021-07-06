package org.hypertrace.gateway.service.span;

import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

public class NoOpClockskewAdjuster extends ClockskewAdjuster {
  public List<SpanEvent.Builder> transform(@NonNull List<? extends SpanEvent.Builder> spans) {
    return new ArrayList<>(spans);
  }
}
