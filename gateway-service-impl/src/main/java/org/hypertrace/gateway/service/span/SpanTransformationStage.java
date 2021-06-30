package org.hypertrace.gateway.service.span;

import java.util.List;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

interface SpanTransformationStage {
  List<SpanEvent.Builder> transform(List<? extends SpanEvent.Builder> spans);
}
