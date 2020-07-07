package org.hypertrace.gateway.service.entity;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.validators.request.RequestValidator;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validator for update entity request. */
public class UpdateEntityRequestValidator extends RequestValidator<UpdateEntityRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateEntityRequestValidator.class);

  @Override
  public void validate(
      UpdateEntityRequest updateEntityRequest,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(updateEntityRequest.getEntityType()),
        "entity_type is mandatory in the request.");

    Preconditions.checkArgument(
        StringUtils.isNotBlank(updateEntityRequest.getEntityId()),
        "entity_id is mandatory in the request.");

    Preconditions.checkArgument(
        updateEntityRequest.getSelectionCount() > 0,
        "Selection list can't be empty in the request.");

    Preconditions.checkArgument(
        updateEntityRequest.hasOperation(), "operation needs to be specified in the request");

    if (updateEntityRequest.getOperation().hasSetAttribute()) {
      // check attribute exists
      String attributeId =
          updateEntityRequest.getOperation().getSetAttribute().getAttribute().getColumnName();
      Preconditions.checkArgument(
          attributeMetadataMap.containsKey(attributeId), attributeId + " attribute doesn't exist.");

      // only 1 attribute source is supported
      AttributeMetadata metadata = attributeMetadataMap.get(attributeId);
      Preconditions.checkArgument(
          metadata.getSourcesCount() == 1 && metadata.getSources(0).equals(AttributeSource.EDS),
          "Only EDS attributes are supported for update right now");
    }
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
