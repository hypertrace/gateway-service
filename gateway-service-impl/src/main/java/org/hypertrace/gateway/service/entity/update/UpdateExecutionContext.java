package org.hypertrace.gateway.service.entity.update;

import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;

/** Context object that contains necessary data to process update request. */
public class UpdateExecutionContext {
  private final Map<String, String> requestHeaders;
  private final Map<String, AttributeMetadata> attributeMetadata;

  public UpdateExecutionContext(
      Map<String, String> requestHeaders, Map<String, AttributeMetadata> attributeMetadata) {
    this.requestHeaders = requestHeaders;
    this.attributeMetadata = attributeMetadata;
  }

  public Map<String, String> getRequestHeaders() {
    return requestHeaders;
  }

  public Map<String, AttributeMetadata> getAttributeMetadata() {
    return attributeMetadata;
  }
}
