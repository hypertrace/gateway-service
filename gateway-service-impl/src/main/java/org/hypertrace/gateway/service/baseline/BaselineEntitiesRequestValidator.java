package org.hypertrace.gateway.service.baseline;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.validators.request.RequestValidator;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BaselineEntitiesRequestValidator extends RequestValidator<BaselineEntitiesRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(BaselineEntitiesRequestValidator.class);

  @Override
  public void validate(
      BaselineEntitiesRequest request, Map<String, AttributeMetadata> attributeMetadataMap) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(request.getEntityType()), "EntityType is mandatory in the request.");

    Preconditions.checkArgument(request.getEntityIdsCount() > 0, "EntityIds cannot be empty");

    Preconditions.checkArgument(
        (request.getBaselineAggregateRequestCount() > 0
            || request.getBaselineMetricSeriesRequestCount() > 0),
        "Both Selection list and TimeSeries list can't be empty in the request.");

    Preconditions.checkArgument(
        request.getStartTimeMillis() > 0
            && request.getEndTimeMillis() > 0
            && request.getStartTimeMillis() < request.getEndTimeMillis(),
        "Invalid time range. Both start and end times have to be valid timestamps.");
  }

  @Override
  protected Logger getLogger() {
    return LOG;
  }
}
