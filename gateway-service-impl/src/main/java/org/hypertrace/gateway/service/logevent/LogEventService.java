package org.hypertrace.gateway.service.logevent;

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
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
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
        PlatformMetricsRegistry.registerTimer("hypertrace.log.event.query.execution", ImmutableMap.of());
  }

  public LogEventResponse getSpansByFilter(RequestContext context, LogEventRequest request) {
    Instant start = Instant.now();
    try {
      Map<String, AttributeMetadata> attributeMap =
          attributeMetadataProvider.getAttributesMetadata(context, AttributeScope.EVENT.name());
      LogEventResponse.Builder logEventResponseBuilder = LogEventResponse.newBuilder();

      Collection<LogEvent> filteredLogEvents = filterLogEvents(context, request, attributeMap);

      logEventResponseBuilder.addAllLogEvents(filteredLogEvents);
      logEventResponseBuilder.setTotal(filteredLogEvents.size());

      LogEventResponse response = logEventResponseBuilder.build();
      LOG.debug("Span Service Response: {}", response);

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

    QueryRequest.Builder queryBuilder = createQueryWithFilter(request, context);

    if (!request.getSelectionList().isEmpty()) {
      request
          .getSelectionList()
          .forEach(
              exp ->
                  queryBuilder.addSelection(
                      QueryAndGatewayDtoConverter.convertToQueryExpression(exp)));
    }


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

  private QueryRequest.Builder createQueryWithFilter(
      LogEventRequest request, RequestContext requestContext) {
    // create filter
    return QueryRequest.newBuilder();
  }
}