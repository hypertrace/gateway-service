package org.hypertrace.gateway.service.logevent;

import static org.hypertrace.gateway.service.common.util.AttributeMetadataUtil.getTimestampAttributeId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.log.events.LogEvent;
import org.hypertrace.gateway.service.v1.log.events.LogEventRequest;
import org.hypertrace.gateway.service.v1.log.events.LogEventResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogEventService {

  private static final Logger LOG = LoggerFactory.getLogger(LogEventService.class);
  private final QueryServiceClient queryServiceClient;
  private final int requestTimeout;
  private final AttributeMetadataProvider attributeMetadataProvider;

  private Timer queryExecutionTimer;

  public LogEventService(
      QueryServiceClient queryServiceClient,
      int requestTimeout,
      AttributeMetadataProvider attributeMetadataProvider) {
    this.queryServiceClient = queryServiceClient;
    this.requestTimeout = requestTimeout;
    this.attributeMetadataProvider = attributeMetadataProvider;
    initMetrics();
  }

  private void initMetrics() {
    queryExecutionTimer =
        PlatformMetricsRegistry.registerTimer(
            "hypertrace.log.event.query.execution", ImmutableMap.of());
  }

  public LogEventResponse getLogEventsByFilter(RequestContext context, LogEventRequest request) {
    Instant start = Instant.now();
    try {
      Map<String, AttributeMetadata> attributeMap =
          attributeMetadataProvider.getAttributesMetadata(context, "LOG_EVENT");
      LogEventResponse.Builder logEventResponseBuilder = LogEventResponse.newBuilder();

      Collection<LogEvent> filteredLogEvents = filterLogEvents(context, request, attributeMap);

      logEventResponseBuilder.addAllLogEvents(filteredLogEvents);

      LogEventResponse response = logEventResponseBuilder.build();
      LOG.debug("Log Event Service Response: {}", response);
      return response;
    } finally {
      queryExecutionTimer.record(
          Duration.between(start, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  List<LogEvent> filterLogEvents(
      RequestContext context,
      LogEventRequest request,
      Map<String, AttributeMetadata> attributeMetadataMap) {

    QueryRequest.Builder queryBuilder =
        QueryRequest.newBuilder()
            .setFilter(
                QueryAndGatewayDtoConverter.addTimeAndSpaceFiltersAndConvertToQueryFilter(
                    request.getStartTimeMillis(),
                    request.getEndTimeMillis(),
                    "",
                    getTimestampAttributeId(this.attributeMetadataProvider, context, "LOG_EVENT"),
                    "",
                    request.getFilter()));

    if (!request.getSelectionList().isEmpty()) {
      request
          .getSelectionList()
          .forEach(
              exp ->
                  queryBuilder.addSelection(
                      QueryAndGatewayDtoConverter.convertToQueryExpression(exp)));
    }

    addSortLimitAndOffset(request, queryBuilder);

    List<LogEvent> logEventResult = new ArrayList<>();
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
        LogEvent.Builder logEventBuilder = LogEvent.newBuilder();
        for (int i = 0; i < chunk.getResultSetMetadata().getColumnMetadataCount(); i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
          String attrName = metadata.getColumnName();
          logEventBuilder.putAttributes(
              metadata.getColumnName(),
              QueryAndGatewayDtoConverter.convertToGatewayValue(
                  attrName, row.getColumn(i), attributeMetadataMap));
        }

        logEventResult.add(logEventBuilder.build());
      }
    }
    return logEventResult;
  }

  // Adds the sort, limit and offset information to the QueryService if it is requested
  private void addSortLimitAndOffset(LogEventRequest request, QueryRequest.Builder queryBuilder) {
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
}
