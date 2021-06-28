package org.hypertrace.gateway.service.span;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.hypertrace.gateway.service.v1.span.SpanEvent;

class SpanTransformationPipeline {

  private final Function<List<? extends SpanEvent>, List<SpanEvent>> pipeline;

  private SpanTransformationPipeline() {
    this.pipeline = ArrayList::new;
  }

  public static SpanTransformationPipeline getNewPipeline() {
    return new SpanTransformationPipeline();
  }

  private SpanTransformationPipeline(
      Function<List<? extends SpanEvent>, List<SpanEvent>> pipeline) {
    this.pipeline = pipeline;
  }

  public SpanTransformationPipeline addProcessingStage(SpanProcessingStage processingStage) {
    Function<List<? extends SpanEvent>, List<SpanEvent>> updatedPipeline =
        pipeline.andThen(processingStage::process);
    return new SpanTransformationPipeline(updatedPipeline);
  }

  public List<SpanEvent> execute(List<SpanEvent> spans) {
    return pipeline.apply(spans);
  }
}
