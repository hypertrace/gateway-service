package org.hypertrace.gateway.service.common.transformer;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.client.EntityLabelsClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity;

public class EntitiesResponseLabelsPostProcessor extends ResponseLabelsPostProcessor<EntitiesResponse.Builder> {
  EntitiesResponseLabelsPostProcessor(AttributeMetadataProvider attributeMetadataProvider,
                              EntityLabelsMappings entityLabelsMappings,
                              EntityLabelsClient entityLabelsClient) {
    super(attributeMetadataProvider, entityLabelsMappings, entityLabelsClient);
  }

  @Override
  protected List<String> getEntityIds(AttributeMetadata attributeMetadata,
                                      EntitiesResponse.Builder responseBuilder) {
    String entityIdColumnName = attributeMetadata.getId();
    Stream<Value> valuesStream = responseBuilder.getEntityList().stream()
        .map(entity -> entity.getAttributeMap().get(entityIdColumnName));
    return getEntityIds(valuesStream);
  }

  @Override
  protected void addLabelsToResponse(Map<String, List<String>> entityLabelsByEntityId,
                                     String entityIdAttributeId,
                                     String entityLabelsAttributeId,
                                     EntitiesResponse.Builder responseBuilder) {
    for (Entity.Builder builder : responseBuilder.getEntityBuilderList()) {
      String entityId = builder.getAttributeMap().get(entityIdAttributeId).getString();
      List<String> entityLabels = entityLabelsByEntityId.get(entityId);
      builder.putAttribute(entityLabelsAttributeId, createStringArrayValue(entityLabels));
    }
  }
}
