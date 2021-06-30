package org.hypertrace.gateway.service.span;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.hypertrace.gateway.service.v1.span.SpanEvent;
import org.hypertrace.gateway.service.v1.span.SpanEvent.Builder;

/**
 * A pipeline of handlers that transform a list of spans through it. This class is not thread-safe.
 */
class SpanTransformationPipeline {

  private final Function<List<? extends SpanEvent.Builder>, List<SpanEvent.Builder>> pipeline;

  private SpanTransformationPipeline() {
    this.pipeline = ArrayList::new;
  }

  public static SpanTransformationPipeline getNewPipeline() {
    return new SpanTransformationPipeline();
  }

  private SpanTransformationPipeline(
      Function<List<? extends SpanEvent.Builder>, List<SpanEvent.Builder>> pipeline) {
    this.pipeline = pipeline;
  }

  public SpanTransformationPipeline addProcessingStage(SpanTransformationStage processingStage) {
    Function<List<? extends SpanEvent.Builder>, List<SpanEvent.Builder>> updatedPipeline =
        pipeline.andThen(processingStage::transform);
    return new SpanTransformationPipeline(updatedPipeline);
  }

  /**
   * Processes the passed list of spans through the pipeline
   *
   * @param spans list of spans to process
   * @return processed spans
   */
  public List<SpanEvent> execute(List<SpanEvent> spans) {
    List<Builder> mutableSpans =
        spans.stream()
            .map(span -> SpanEvent.newBuilder().putAllAttributes(span.getAttributesMap()))
            .collect(toList());
    return pipeline.apply(mutableSpans).stream().map(SpanEvent.Builder::build).collect(toList());
  }
}
