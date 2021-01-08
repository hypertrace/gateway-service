package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.client.QueryServiceClient;

import java.util.Iterator;
import java.util.Map;

public class BaselineServiceQueryExecutor {

  private final int qsRequestTimeout;
  private final QueryServiceClient queryServiceClient;

  public BaselineServiceQueryExecutor(int qsRequestTimeout, QueryServiceClient queryServiceClient) {
    this.qsRequestTimeout = qsRequestTimeout;
    this.queryServiceClient = queryServiceClient;
  }

  public Iterator<ResultSetChunk> executeQuery(
      Map<String, String> requestHeaders, QueryRequest aggQueryRequest) {
    return queryServiceClient.executeQuery(aggQueryRequest, requestHeaders, qsRequestTimeout);
  }
}
