package org.hypertrace.gateway.service.span;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

public interface ClockskewAdjuster {
  List<SpanEvent> adjustSpansForClockSkew(ImmutableList<SpanEvent> spans);
}
