package org.hypertrace.gateway.service.common.transformer;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.client.EntityLabelsClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.trace.Trace;
import org.hypertrace.gateway.service.v1.trace.TracesResponse;

public class TracesResponseLabelsPostProcessor extends ResponseLabelsPostProcessor<TracesResponse.Builder> {
  TracesResponseLabelsPostProcessor(AttributeMetadataProvider attributeMetadataProvider,
                                      EntityLabelsMappings entityLabelsMappings,
                                      EntityLabelsClient entityLabelsClient) {
    super(attributeMetadataProvider, entityLabelsMappings, entityLabelsClient);
  }

  @Override
  protected List<String> getEntityIds(AttributeMetadata attributeMetadata,
                                      TracesResponse.Builder responseBuilder) {
    String entityIdColumnName = attributeMetadata.getId();
    Stream<Value> valueStream = responseBuilder.getTracesList().stream()
        .map(trace -> trace.getAttributesMap().get(entityIdColumnName));
    return getEntityIds(valueStream);
  }

  @Override
  protected void addLabelsToResponse(Map<String, List<String>> entityLabelsByEntityId,
                                     String entityIdAttributeId,
                                     String entityLabelsAttributeId,
                                     TracesResponse.Builder responseBuilder) {
    for (Trace.Builder builder : responseBuilder.getTracesBuilderList()) {
      String entityId = builder.getAttributesMap().get(entityIdAttributeId).getString();
      List<String> entityLabels = entityLabelsByEntityId.get(entityId);
      builder.putAttributes(entityLabelsAttributeId, createStringArrayValue(entityLabels));
    }
  }
}
