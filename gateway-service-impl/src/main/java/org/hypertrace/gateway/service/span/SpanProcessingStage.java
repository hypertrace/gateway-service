package org.hypertrace.gateway.service.span;

import java.util.List;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

interface SpanProcessingStage {
  List<SpanEvent> process(List<? extends SpanEvent> spans);
}
