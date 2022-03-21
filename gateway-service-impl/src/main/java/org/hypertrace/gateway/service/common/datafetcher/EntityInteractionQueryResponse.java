package org.hypertrace.gateway.service.common.datafetcher;

import java.util.Iterator;
import org.hypertrace.core.query.service.api.ResultSetChunk;

public class EntityInteractionQueryResponse {
  private EntityInteractionQueryRequest request;
  private Iterator<ResultSetChunk> resultSetChunkIterator;

  public EntityInteractionQueryResponse(
      EntityInteractionQueryRequest request, Iterator<ResultSetChunk> resultSetChunkIterator) {
    this.request = request;
    this.resultSetChunkIterator = resultSetChunkIterator;
  }

  public EntityInteractionQueryRequest getRequest() {
    return request;
  }

  public Iterator<ResultSetChunk> getResultSetChunkIterator() {
    return resultSetChunkIterator;
  }
}
