package org.hypertrace.gateway.service.common.transformer;

import com.google.protobuf.Message;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.entity.query.service.client.EntityLabelsClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;

public abstract class ResponseLabelsPostProcessor<T extends Message.Builder> {
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final EntityLabelsMappings entityLabelsMappings;
  private final EntityLabelsClient entityLabelsClient;

  ResponseLabelsPostProcessor(AttributeMetadataProvider attributeMetadataProvider,
                                     EntityLabelsMappings entityLabelsMappings,
                                     EntityLabelsClient entityLabelsClient) {
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.entityLabelsMappings = entityLabelsMappings;
    this.entityLabelsClient = entityLabelsClient;
  }

  void addEntityLabelsToResponse(RequestContext requestContext,
                                 T responseBuilder) {
    List<Expression> entityLabelExpressions = requestContext.getEntityLabelExpressions();
    if (entityLabelExpressions.isEmpty()) {
      return;
    }

    for (Expression labelExpression : entityLabelExpressions) {
      addEntityLabelsForExpressionToResponse(labelExpression, requestContext, responseBuilder);
    }
  }

  private void addEntityLabelsForExpressionToResponse(Expression labelExpression,
                                                      RequestContext requestContext,
                                                      T responseBuilder) {
    Optional<ScopeEntityLabelsMapping> scopeEntityLabelsMappingOptional =
        entityLabelsMappings.getEntityLabelsMapping(labelExpression.getColumnIdentifier().getColumnName());
    if (scopeEntityLabelsMappingOptional.isEmpty()) {
      return;
    }

    ScopeEntityLabelsMapping scopeEntityLabelsMapping = scopeEntityLabelsMappingOptional.get();
    AttributeMetadata entityIdAttributeMetadata = attributeMetadataProvider.getAttributeMetadata(
        requestContext,
        scopeEntityLabelsMapping.getScope(),
        scopeEntityLabelsMapping.getEntityIdKey()
    ).orElseThrow();

    AttributeMetadata mappedEntityIdAttributeMetadata = attributeMetadataProvider.getAttributeMetadata(
        requestContext,
        scopeEntityLabelsMapping.getMappedScope(),
        scopeEntityLabelsMapping.getMappedEntityIdKey()
    ).orElseThrow();

    AttributeMetadata mappedEntityLabelsAttributeMetadata = attributeMetadataProvider.getAttributeMetadata(
        requestContext,
        scopeEntityLabelsMapping.getMappedScope(),
        scopeEntityLabelsMapping.getMappedLabelsKey()
    ).orElseThrow();

    List<String> entityIds = getEntityIds(entityIdAttributeMetadata, responseBuilder);
    Map<String, List<String>> entityLabelsMap = getEntityLabelsForEntities(
        mappedEntityIdAttributeMetadata.getScope().name(),
        mappedEntityIdAttributeMetadata,
        mappedEntityLabelsAttributeMetadata,
        entityIds,
        requestContext
    );

    addLabelsToResponse(
        entityLabelsMap,
        entityIdAttributeMetadata.getId(),
        scopeEntityLabelsMapping.getLabelsColumnName(),
        responseBuilder);
  }

  private Map<String, List<String>> getEntityLabelsForEntities(String entityType,
                                                               AttributeMetadata mappedEntityIdMetadata,
                                                               AttributeMetadata mappedEntityLabelsMetadata,
                                                               List<String> entityIds,
                                                               RequestContext requestContext) {

    return entityLabelsClient.getEntityLabelsForEntities(
        mappedEntityIdMetadata.getId(),
        mappedEntityLabelsMetadata.getId(),
        entityIds,
        entityType,
        requestContext.getHeaders(),
        requestContext.getTenantId());
  }

  protected List<String> getEntityIds(Stream<Value> values) {
    return values.map(Value::getString).collect(Collectors.toList());
  }

  protected Value createStringArrayValue(List<String> stringList) {
    return Value.newBuilder()
        .setValueType(ValueType.STRING_ARRAY)
        .addAllStringArray(stringList)
        .build();
  }

  protected abstract List<String> getEntityIds(AttributeMetadata attributeMetadata, T responseBuilder);
  protected abstract void addLabelsToResponse(Map<String, List<String>> entityLabelsByEntityId,
                                   String entityIdAttributeId,
                                   String entityLabelsAttributeId,
                                   T responseBuilder);
}
