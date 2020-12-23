package org.hypertrace.gateway.service.span;

import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createCountByColumnSelection;
import static org.hypertrace.gateway.service.common.util.AttributeMetadataUtil.getSpaceAttributeId;
import static org.hypertrace.gateway.service.common.util.AttributeMetadataUtil.getTimestampAttributeId;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.span.SpanEvent;
import org.hypertrace.gateway.service.v1.span.SpansRequest;
import org.hypertrace.gateway.service.v1.span.SpansResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to aggregate all the spans of a given Api trace. This specific service is going to serve
 * SpanRequest, and SpanResponse from spans.proto
 */
public class SpanService {

  private static final Logger LOG = LoggerFactory.getLogger(SpanService.class);
  private final QueryServiceClient queryServiceClient;
  private final int requestTimeout;
  private final AttributeMetadataProvider attributeMetadataProvider;

  private Timer queryExecutionTimer;

  public SpanService(
      QueryServiceClient queryServiceClient,
      int requestTimeout,
      AttributeMetadataProvider attributeMetadataProvider) {
    this.queryServiceClient = queryServiceClient;
    this.requestTimeout = requestTimeout;
    this.attributeMetadataProvider = attributeMetadataProvider;
    initMetrics();
  }

  private void initMetrics() {
    queryExecutionTimer = new Timer();
    PlatformMetricsRegistry.register("span.query.execution", queryExecutionTimer);
  }

  public SpansResponse getSpansByFilter(RequestContext context, SpansRequest request) {
    final Context timerContext = queryExecutionTimer.time();
    try {
      Map<String, AttributeMetadata> attributeMap =
          attributeMetadataProvider.getAttributesMetadata(context, AttributeScope.EVENT.name());
      SpansResponse.Builder spanResponseBuilder = SpansResponse.newBuilder();

      Collection<SpanEvent> filteredSpanEvents = filterSpans(context, request, attributeMap);

      spanResponseBuilder.addAllSpans(filteredSpanEvents);
      spanResponseBuilder.setTotal(getTotalFilteredSpans(context, request));

      SpansResponse response = spanResponseBuilder.build();
      LOG.debug("Span Service Response: {}", response);

      return response;
    } finally {
      timerContext.stop();
    }
  }

  @VisibleForTesting
  Collection<SpanEvent> filterSpans(
      RequestContext context,
      SpansRequest request,
      Map<String, AttributeMetadata> attributeMetadataMap) {
    return filterSpanEvents(context, request, attributeMetadataMap);
  }

  @VisibleForTesting
  List<SpanEvent> filterSpanEvents(
      RequestContext context,
      SpansRequest request,
      Map<String, AttributeMetadata> attributeMetadataMap) {

    QueryRequest.Builder queryBuilder = createQueryWithFilter(request, context);

    if (!request.getSelectionList().isEmpty()) {
      request
          .getSelectionList()
          .forEach(
              exp ->
                  queryBuilder.addSelection(
                      QueryAndGatewayDtoConverter.convertToQueryExpression(exp)));
    }

    addSortLimitAndOffset(request, queryBuilder);

    List<SpanEvent> spanEventsResult = new ArrayList<>();
    QueryRequest queryRequest = queryBuilder.build();

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(queryRequest, context.getHeaders(), requestTimeout);

    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      LOG.debug("Received chunk: {}", chunk);

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        SpanEvent.Builder spanEventBuilder = SpanEvent.newBuilder();
        for (int i = 0; i < chunk.getResultSetMetadata().getColumnMetadataCount(); i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
          String attrName = metadata.getColumnName();
          spanEventBuilder.putAttributes(
              metadata.getColumnName(),
              QueryAndGatewayDtoConverter.convertToGatewayValue(
                  attrName, row.getColumn(i), attributeMetadataMap));
        }

        spanEventsResult.add(spanEventBuilder.build());
      }
    }
    return spanEventsResult;
  }

  // Adds the sort, limit and offset information to the QueryService if it is requested
  private void addSortLimitAndOffset(SpansRequest request, QueryRequest.Builder queryBuilder) {
    if (request.getOrderByCount() > 0) {
      List<OrderByExpression> orderByExpressions = request.getOrderByList();
      queryBuilder.addAllOrderBy(
          QueryAndGatewayDtoConverter.convertToQueryOrderByExpressions(orderByExpressions));
    }

    int limit = request.getLimit();
    if (limit > 0) {
      queryBuilder.setLimit(limit);
    }

    int offset = request.getOffset();
    if (offset > 0) {
      queryBuilder.setOffset(offset);
    }
  }

  private QueryRequest.Builder createQueryWithFilter(
      SpansRequest request, RequestContext requestContext) {
    Filter filter =
        QueryAndGatewayDtoConverter.addTimeAndSpaceFiltersAndConvertToQueryFilter(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            getTimestampAttributeId(
                this.attributeMetadataProvider, requestContext, AttributeScope.EVENT.name()),
            getSpaceAttributeId(
                this.attributeMetadataProvider, requestContext, AttributeScope.EVENT.name()),
            request.getFilter());
    return QueryRequest.newBuilder().setFilter(filter);
  }

  private int getTotalFilteredSpans(RequestContext context, SpansRequest request) {
    int total = 0;
    String timestampAttributeId =
        getTimestampAttributeId(
            this.attributeMetadataProvider, context, AttributeScope.EVENT.name());

    QueryRequest queryRequest =
        createQueryWithFilter(request, context)
            .addSelection(createCountByColumnSelection(timestampAttributeId))
            .build();

    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(queryRequest, context.getHeaders(), requestTimeout);
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received chunk: " + chunk.toString());
      }

      // There should be only 1 result
      if (chunk.getRowCount() != 1 && chunk.getResultSetMetadata().getColumnMetadataCount() != 1) {
        LOG.error(
            "Count the Api Traces total returned in multiple row / column. "
                + "Total Row: {}, Total Column: {}",
            chunk.getRowCount(),
            chunk.getResultSetMetadata().getColumnMetadataCount());
        break;
      }

      // There's only 1 result with 1 column. If there's no result, Pinot doesn't
      // return any row unfortunately
      if (chunk.getRowCount() > 0) {
        Row row = chunk.getRow(0);
        String totalStr = row.getColumn(0).getString();
        try {
          total = Integer.parseInt(totalStr);
        } catch (NumberFormatException nfe) {
          LOG.error(
              "Unable to convert Total to a number. Received value: {} from Query Service",
              totalStr);
        }
      }
    }
    return total;
  }
}
