package org.hypertrace.gateway.service.entity;

import static org.hypertrace.gateway.service.v1.entity.MultiValuedAttributeOperation.OperationType.OPERATION_TYPE_UNSPECIFIED;

import io.grpc.Status;
import java.util.Map;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.MultiValuedAttributeOperation;

/** Validator for bulk update for entity array attribute request. */
public class BulkUpdateEntitiesRequestValidator {

  public Status validate(
      BulkUpdateEntitiesRequest request, Map<String, AttributeMetadata> attributeMetadataMap) {
    if (StringUtils.isBlank(request.getEntityType())) {
      return Status.INVALID_ARGUMENT.withDescription("entity_type is mandatory in the request.");
    }

    if (ObjectUtils.isEmpty(request.getEntityIdsList())) {
      return Status.INVALID_ARGUMENT.withDescription("entity_ids are mandatory in the request.");
    }

    if (!request.hasOperation()) {
      return Status.INVALID_ARGUMENT.withDescription("operation is mandatory in the request.");
    }

    if (!request.getOperation().hasMultiValuedAttributeOperation()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "multi valued operation is mandatory in the request.");
    }

    MultiValuedAttributeOperation multiValuedAttributeOperation =
        request.getOperation().getMultiValuedAttributeOperation();
    if (!multiValuedAttributeOperation.hasAttribute()) {
      return Status.INVALID_ARGUMENT.withDescription(
          "attribute needs to be specified in the request");
    }

    // check attribute exists
    String attributeId = multiValuedAttributeOperation.getAttribute().getColumnName();
    if (!attributeMetadataMap.containsKey(attributeId)) {
      return Status.INVALID_ARGUMENT.withDescription(attributeId + " attribute doesn't exist.");
    }

    AttributeMetadata metadata = attributeMetadataMap.get(attributeId);
    if (!metadata.getSourcesList().stream()
        .anyMatch(attributeSource -> attributeSource.equals(AttributeSource.EDS))) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Only EDS attributes are supported for update right now");
    }

    if (multiValuedAttributeOperation.getType() == OPERATION_TYPE_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "operation type needs to be correctly specified in the request");
    }

    return Status.OK;
  }
}
