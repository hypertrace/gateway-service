package org.hypertrace.gateway.service.entity;

import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.validators.request.RequestValidator;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This contains logic to validate EntitiesRequest object. Currently the following rules are
 * checked:
 * 1) Validate that the attributes in the selection criteria exist as part of attribute
 *    metadata
 * 2) Validate that the attributes used in aggregations exist and the aggregations themselves are valid
 *    Note that we don't validate that aggregation attributes are of type METRIC instead of ATTRIBUTE since there are
 *    some like 'duration' that are defined as ATTRIBUTE.
 */
public class EntitiesRequestValidator extends RequestValidator<EntitiesRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(EntitiesRequestValidator.class);

  public void validate(
      EntitiesRequest entitiesRequest, Map<String, AttributeMetadata> attributeMetadataMap) {

    // 1. check if all attributes in selection exist as part of attribute metadata
    entitiesRequest
        .getSelectionList()
        .forEach(expression -> validateAttributeExists(attributeMetadataMap, expression));

    // 2. check if time aggregation metrics are valid
    validateTimeAggregations(
        entitiesRequest.getTimeAggregationList(),
        entitiesRequest.getStartTimeMillis(),
        entitiesRequest.getEndTimeMillis(),
        attributeMetadataMap);

    // 3. check if function aggregation expression attributes are valid
    validateFunctionExpressions(entitiesRequest.getSelectionList(), attributeMetadataMap);
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
