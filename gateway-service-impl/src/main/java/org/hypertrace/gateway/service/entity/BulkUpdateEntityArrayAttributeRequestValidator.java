package org.hypertrace.gateway.service.entity;

import static org.hypertrace.gateway.service.v1.entity.BulkUpdateEntityArrayAttributeRequest.Operation.OPERATION_UNSPECIFIED;

import io.grpc.Status;
import java.util.Map;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntityArrayAttributeRequest;

/** Validator for bulk update for entity array attribute request. */
public class BulkUpdateEntityArrayAttributeRequestValidator {

  public Status validate(
      BulkUpdateEntityArrayAttributeRequest request,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    if (StringUtils.isBlank(request.getEntityType())) {
      return Status.INVALID_ARGUMENT.withDescription("entity_type is mandatory in the request.");
    }

    if (ObjectUtils.isEmpty(request.getEntityIdsList())) {
      return Status.INVALID_ARGUMENT.withDescription("entity_ids are mandatory in the request.");
    }

    if (!request.hasAttribute()) {

      return Status.INVALID_ARGUMENT.withDescription(
          "attribute needs to be specified in the request");
    }

    // check attribute exists
    String attributeId = request.getAttribute().getColumnName();
    if (!attributeMetadataMap.containsKey(attributeId)) {
      return Status.INVALID_ARGUMENT.withDescription(attributeId + " attribute doesn't exist.");
    }

    AttributeMetadata metadata = attributeMetadataMap.get(attributeId);
    if (!metadata.getSourcesList().stream()
        .anyMatch(attributeSource -> attributeSource.equals(AttributeSource.EDS))) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Only EDS attributes are supported for update right now");
    }

    if (request.getOperation() == OPERATION_UNSPECIFIED) {
      return Status.INVALID_ARGUMENT.withDescription(
          "operation  needs to be correctly specified in the request");
    }

    return Status.OK;
  }
}
