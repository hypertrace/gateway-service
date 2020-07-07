package org.hypertrace.gateway.service.explore;

import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

/**
 * Implementations of this handle the various Requests
 * 1. RequestHandler handles simple selection and aggregation requests(including those with Group By).
 * 2. TimeAggregationsRequestHandler handles time series requests.
 * 3. TimeAggregationsWithGroupByRequestHandler handles time series requests that have Group By.
 *
 * <p>ExploreService.explore() switches to one of these implementations based on the request type.
 */
interface IRequestHandler {
  ExploreResponse.Builder handleRequest(
      ExploreRequestContext requestContext, ExploreRequest request);
}
