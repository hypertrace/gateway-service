package org.hypertrace.gateway.service.entity;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.validators.request.RequestValidator;
import org.hypertrace.gateway.service.v1.entity.BulkEntityArrayAttributeUpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Validator for update entity request. */
public class BulkEntityArrayAttributeUpdateRequestValidator
    extends RequestValidator<BulkEntityArrayAttributeUpdateRequest> {
  private static final Logger LOG =
      LoggerFactory.getLogger(BulkEntityArrayAttributeUpdateRequestValidator.class);

  @Override
  public void validate(
      BulkEntityArrayAttributeUpdateRequest request,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(request.getEntityType()),
        "entity_type is mandatory in the request.");

    Preconditions.checkArgument(
        ObjectUtils.isNotEmpty(request.getEntityIdsList()),
        "entity_ids are mandatory in the request.");

    Preconditions.checkArgument(
        request.hasAttribute(), "attribute needs to be specified in the request");

    // check attribute exists
    String attributeId = request.getAttribute().getColumnName();
    Preconditions.checkArgument(
        attributeMetadataMap.containsKey(attributeId), attributeId + " attribute doesn't exist.");

    // only 1 attribute source is supported
    AttributeMetadata metadata = attributeMetadataMap.get(attributeId);
    Preconditions.checkArgument(
        metadata.getSourcesCount() == 1 && metadata.getSources(0).equals(AttributeSource.EDS),
        "Only EDS attributes are supported for update right now");

    Preconditions.checkArgument(
        request.getOperation().ordinal() > 0,
        "operation  needs to be correctly specified in the request");

    Preconditions.checkArgument(
        ObjectUtils.isNotEmpty(request.getValuesList()), "values are mandatory in the request.");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
