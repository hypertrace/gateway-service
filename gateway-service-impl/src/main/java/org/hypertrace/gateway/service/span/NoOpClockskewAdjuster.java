package org.hypertrace.gateway.service.span;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

public class NoOpClockskewAdjuster implements ClockskewAdjuster{

  @Override
  public List<SpanEvent> adjustSpansForClockSkew(ImmutableList<SpanEvent> spans) {
    return spans;
  }
}
