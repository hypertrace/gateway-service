package org.hypertrace.gateway.service.explore;

import static org.hypertrace.core.query.service.client.QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT;

import java.util.List;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.gateway.service.common.QueryRequestContext;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;

public class ExploreRequestContext extends QueryRequestContext {
  private final ExploreRequest exploreRequest;

  private boolean hasGroupBy = false;
  private List<OrderByExpression> orderByExpressions;

  public ExploreRequestContext(RequestContext grpcRequestContext, ExploreRequest exploreRequest) {
    super(
        grpcRequestContext, exploreRequest.getStartTimeMillis(), exploreRequest.getEndTimeMillis());

    this.exploreRequest = exploreRequest;
    this.orderByExpressions = exploreRequest.getOrderByList();
  }

  public ExploreRequest getExploreRequest() {
    return this.exploreRequest;
  }

  public String getContext() {
    return this.exploreRequest.getContext();
  }

  /**
   * hasGroupBy could be different from the value in ExploreRequest since
   * TimeAggregationsRequestHandler always sets hasGroupBy=true due to grouping by the start time
   * when getting the time series. hasGroupBy is used to determine if one needs to do manual sorting
   * after getting the results back from Query Service which we need to do when Group By is
   * specified.
   */
  public void setHasGroupBy(boolean hasGroupBy) {
    this.hasGroupBy = hasGroupBy;
  }

  public boolean hasGroupBy() {
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

  /**
   * Returns the max number of rows to be fetched exclusive of any rest group rows which may be
   * added on top of the group by limit
   */
  public int getRowLimitBeforeRest() {
    // On a grouped aggregation without time series, row limit should not be larger than group limit
    if (this.hasGroupBy() && !this.hasTimeAggregations()) {
      return Math.min(this.getBackwardsCompatibleRowLimit(), this.getGroupByLimit());
    }

    return this.getBackwardsCompatibleRowLimit();
  }

  public int getRowLimitAfterRest() {
    return this.getBackwardsCompatibleRowLimit();
  }

  public int getOffset() {
    return this.exploreRequest.getOffset();
  }

  public boolean getIncludeRestGroup() {
    return this.exploreRequest.getIncludeRestGroup();
  }

  private int getGroupByLimit() {
    // If a request has no group limit, default to row limit
    if (this.providedGroupLimitUnset()) {
      return this.exploreRequest.getLimit();
    }
    return this.exploreRequest.getGroupLimit();
  }

  /**
   * Returns the row limit, falling back to calculated/default limits if old format queries are
   * detected
   */
  private int getBackwardsCompatibleRowLimit() {
    // Row limit previously was the number of groups for a grouped + time series request. If group
    // limit isn't set, assume the old format and expand the row limit to default limit
    if (this.providedGroupLimitUnset() && this.hasGroupBy() && this.hasTimeAggregations()) {
      return Math.max(this.exploreRequest.getLimit(), DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);
    }
    // Previously, a rest group would not count against the limit
    if (this.providedGroupLimitUnset() && this.hasGroupBy() && this.getIncludeRestGroup()) {
      return this.exploreRequest.getLimit() + 1;
    }

    return this.exploreRequest.getLimit();
  }

  private boolean providedGroupLimitUnset() {
    return this.exploreRequest.getGroupLimit() == 0;
  }

  private boolean hasTimeAggregations() {
    return !this.exploreRequest.getTimeAggregationList().isEmpty();
  }
}
