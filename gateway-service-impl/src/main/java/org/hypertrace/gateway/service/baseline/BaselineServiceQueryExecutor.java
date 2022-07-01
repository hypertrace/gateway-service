package org.hypertrace.gateway.service.baseline;

import java.util.Iterator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;

public class BaselineServiceQueryExecutor {
  private final QueryServiceClient queryServiceClient;

  public BaselineServiceQueryExecutor(QueryServiceClient queryServiceClient) {
    this.queryServiceClient = queryServiceClient;
  }

  public Iterator<ResultSetChunk> executeQuery(
      RequestContext requestContext, QueryRequest aggQueryRequest) {
    return queryServiceClient.executeQuery(requestContext, aggQueryRequest);
  }
}
