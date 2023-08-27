package org.hypertrace.gateway.service.common.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.Iterator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryServiceGrpc.QueryServiceBlockingStub;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.gateway.service.common.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryServiceClient {
  private static final Logger LOG = LoggerFactory.getLogger(QueryServiceClient.class);
  private final QueryServiceBlockingStub stub;
  private final Duration timeoutDuration;

  public QueryServiceClient(QueryServiceBlockingStub stub, Duration timeoutDuration) {
    this.stub = stub;
    this.timeoutDuration = timeoutDuration;
  }

  public Iterator<ResultSetChunk> executeQuery(
      RequestContext requestContext, QueryRequest request) {

    LOG.debug("Sending query to query service with request: {}", request);
    return requestContext
        .getGrpcContext()
        .call(
            () ->
                stub.withDeadlineAfter(timeoutDuration.toMillis(), MILLISECONDS).execute(request));
  }
}
