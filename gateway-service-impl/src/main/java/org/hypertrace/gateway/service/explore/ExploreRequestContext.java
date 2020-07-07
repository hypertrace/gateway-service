package org.hypertrace.gateway.service.explore;

import java.util.List;
import java.util.Map;
import org.hypertrace.gateway.service.common.QueryRequestContext;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;

public class ExploreRequestContext extends QueryRequestContext {
  private final ExploreRequest exploreRequest;

  private boolean hasGroupBy = false;
  private List<OrderByExpression> orderByExpressions;

  ExploreRequestContext(
      String tenantId, ExploreRequest exploreRequest, Map<String, String> requestHeaders) {
    super(
        tenantId,
        exploreRequest.getStartTimeMillis(),
        exploreRequest.getEndTimeMillis(),
        requestHeaders);

    this.exploreRequest = exploreRequest;
    this.orderByExpressions = exploreRequest.getOrderByList();
  }

  ExploreRequest getExploreRequest() {
    return this.exploreRequest;
  }

  String getContext() {
    return this.exploreRequest.getContext();
  }

  /**
   * hasGroupBy could be different from the value in ExploreRequest since
   * TimeAggregationsRequestHandler always sets hasGroupBy=true due to grouping by the start time
   * when getting the time series. hasGroupBy is used to determine if one needs to do manual sorting
   * after getting the results back from Query Service which we need to do when Group By is
   * specified.
   */
  void setHasGroupBy(boolean hasGroupBy) {
    this.hasGroupBy = hasGroupBy;
  }

  boolean hasGroupBy() {
    return this.hasGroupBy;
  }

  public List<OrderByExpression> getOrderByExpressions() {
    return this.orderByExpressions;
  }

  /**
   * Since we need to match the alias of the order by to that in the selection when doing manual
   * sorting, we provide this method to be able to set a list of orders bys based on the one in
   * ExploreRequest but with the aliases in the Function Expressions replaced with those in the
   * selections. See RequestHandler.getOrderByExpressions().
   */
  public void setOrderByExpressions(List<OrderByExpression> orderByExpressions) {
    this.orderByExpressions = orderByExpressions;
  }

  public int getLimit() {
    return this.exploreRequest.getLimit();
  }

  public int getOffset() {
    return this.exploreRequest.getOffset();
  }

  boolean getIncludeRestGroup() {
    return this.exploreRequest.getIncludeRestGroup();
  }
}
