package org.hypertrace.gateway.service.span;

import java.util.List;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

/**
 * Represents a transformation stage in the {@link SpanTransformationPipeline}
 */
interface SpanTransformationStage {
  List<SpanEvent.Builder> transform(List<? extends SpanEvent.Builder> spans);
}
