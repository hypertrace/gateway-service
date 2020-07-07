package org.hypertrace.gateway.service.trace;

import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.validators.request.RequestValidator;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This contains logic to validate TracesRequest object. Currently the following rules are
 * checked:
 * 1) Validate that the attributes in the selection criteria exist as part of attribute
 *    metadata
 * 2) Validate that the attributes used in aggregations exist and the aggregations themselves are valid
 *
 */
public class TracesRequestValidator extends RequestValidator<TracesRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(TracesRequestValidator.class);

  public void validate(
      TracesRequest tracesRequest, Map<String, AttributeMetadata> attributeMetadataMap) {
    validateScope(tracesRequest);
    // 1. check if all attributes in selection exist as part of attribute metadata
    tracesRequest
        .getSelectionList()
        .forEach(expression -> validateAttributeExists(attributeMetadataMap, expression));

    // 2. check if it has at least 1 selection
    checkArgument(
        tracesRequest.getSelectionCount() > 0, "There's no selection specified in this request");

    // 3. check if function aggregation expression attributes are valid
    validateFunctionExpressions(tracesRequest.getSelectionList(), attributeMetadataMap);
  }

  void validateScope(TracesRequest tracesRequest) {
    // Check that the scope is set to either API_TRACE, BACKEND_TRACE or TRACE
    TraceScope traceScope = TraceScope.valueOf(tracesRequest.getScope());
    checkArgument(
        traceScope == TraceScope.BACKEND_TRACE
            || traceScope == TraceScope.API_TRACE
            || traceScope == TraceScope.TRACE,
        "TraceRequest scope should be one of BACKEND_TRACE, API_TRACE or TRACE.");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
