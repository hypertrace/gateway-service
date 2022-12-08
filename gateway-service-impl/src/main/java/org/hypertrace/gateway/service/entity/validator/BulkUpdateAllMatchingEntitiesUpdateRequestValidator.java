package org.hypertrace.gateway.service.entity.validator;

import static io.grpc.Status.OK;
import static java.util.function.Predicate.not;
import static org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator.OPERATION_UNSPECIFIED;

import io.grpc.Status;
import java.util.Map;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.Update;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation;

@AllArgsConstructor
public class BulkUpdateAllMatchingEntitiesUpdateRequestValidator {
  private final AttributeMetadataProvider metadataProvider;
  private final BulkUpdateAllMatchingEntitiesRequest request;
  private final RequestContext context;
  private final Predicate<Status> NOT_OK = not(OK::equals);

  public Status validate() {
    if (StringUtils.isBlank(request.getEntityType())) {
      return Status.INVALID_ARGUMENT.withDescription("entity_type is mandatory in the request.");
    }

    if (request.getUpdatesList().isEmpty()) {
      return Status.INVALID_ARGUMENT.withDescription("updates are mandatory in the request.");
    }

    return request.getUpdatesList().stream()
        .map(this::validate)
        .filter(NOT_OK)
        .findFirst()
        .orElse(OK);
  }

  private Status validate(final Update update) {
    if (Filter.getDefaultInstance().equals(update.getFilter())) {
      return Status.INVALID_ARGUMENT.withDescription("filters are mandatory in the request");
    }

    return update.getOperationsList().stream()
        .map(this::validate)
        .filter(NOT_OK)
        .findFirst()
        .orElse(OK);
  }

  private Status validate(final UpdateOperation operation) {
    if (OPERATION_UNSPECIFIED.equals(operation.getOperator())) {
      return Status.INVALID_ARGUMENT.withDescription("operators are mandatory in the request");
    }

    return validate(operation.getAttribute());
  }

  private Status validate(final ColumnIdentifier columnIdentifier) {
    if (ColumnIdentifier.getDefaultInstance().equals(columnIdentifier)) {
      return Status.INVALID_ARGUMENT.withDescription("attribute ids are mandatory in the request");
    }

    final Map<String, AttributeMetadata> metadataMap =
        metadataProvider.getAttributesMetadata(context, request.getEntityType());
    final String attributeId = columnIdentifier.getColumnName();
    final AttributeMetadata metadata = metadataMap.get(attributeId);

    if (metadata == null) {
      return Status.INVALID_ARGUMENT.withDescription(
          String.format("Attribute %s does not exist", attributeId));
    }

    if (!metadata.getSourcesList().contains(AttributeSource.EDS)) {
      return Status.INVALID_ARGUMENT.withDescription(
          "Only EDS attributes are supported for update right now");
    }

    return OK;
  }
}
