package org.hypertrace.gateway.service.explore;

import java.util.List;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

/**
 * Extension of IRequestHandler that will also implement methods to manually sort results from the
 * query service. This is needed when Group By is in the request as Pinot does not support Group By
 * with Order By.
 */
interface RequestHandlerWithSorting extends IRequestHandler {
  List<OrderByExpression> getRequestOrderByExpressions(ExploreRequest request);

  void sortAndPaginatePostProcess(
      ExploreResponse.Builder builder,
      List<OrderByExpression> orderByExpressions,
      int limit,
      int offset);
}
