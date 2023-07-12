package org.hypertrace.gateway.service.trace;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createCountByColumnSelection;
import static org.hypertrace.gateway.service.common.util.AttributeMetadataUtil.getSpaceAttributeId;
import static org.hypertrace.gateway.service.common.util.AttributeMetadataUtil.getTimestampAttributeId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.QueryRequest.Builder;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.transformer.RequestPreProcessor;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.common.util.ExpressionReader;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.v1.common.OrderByExpression;
import org.hypertrace.gateway.service.v1.trace.Trace;
import org.hypertrace.gateway.service.v1.trace.TracesRequest;
import org.hypertrace.gateway.service.v1.trace.TracesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to aggregate and create Api Trace.
 *
 * <p>Api Trace = 1 Api Execution that contains 1 entry span + multiple correlated exit spans The
 * entry span is called root span.
 *
 * <p>The trace attribute = root span attributes, and the trace is filterable by the root span's
 * attributes.
 *
 * <p>Trace will not have independent attributes.
 */
public class TracesService {

  private static final Logger LOG = LoggerFactory.getLogger(TracesService.class);

  private final QueryServiceClient queryServiceClient;
  private final AttributeMetadataProvider attributeMetadataProvider;
  private final ExecutorService queryExecutor;
  private final TracesRequestValidator requestValidator;
  private final RequestPreProcessor requestPreProcessor;

  private Timer queryExecutionTimer;

  public TracesService(
      QueryServiceClient queryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFilterConfigs,
      ExecutorService queryExecutor) {
    this.queryServiceClient = queryServiceClient;
    this.attributeMetadataProvider = attributeMetadataProvider;
    this.queryExecutor = queryExecutor;
    this.requestValidator = new TracesRequestValidator();
    this.requestPreProcessor =
        new RequestPreProcessor(attributeMetadataProvider, scopeFilterConfigs);
    initMetrics();
  }

  private void initMetrics() {
    queryExecutionTimer =
        PlatformMetricsRegistry.registerTimer(
            "hypertrace.traces.query.execution", ImmutableMap.of());
  }

  public TracesResponse getTracesByFilter(RequestContext context, TracesRequest request) {
    Instant start = Instant.now();
    try {
      requestValidator.validateScope(request);

      TracesRequest preProcessedRequest = requestPreProcessor.process(request, context);

      TraceScope scope = TraceScope.valueOf(preProcessedRequest.getScope());
      Map<String, AttributeMetadata> attributeMap =
          attributeMetadataProvider.getAttributesMetadata(context, preProcessedRequest.getScope());

      requestValidator.validate(preProcessedRequest, attributeMap);

      TracesResponse.Builder tracesResponseBuilder = TracesResponse.newBuilder();
      // filter traces
      CompletableFuture<List<Trace>> filteredTraceFuture =
          CompletableFuture.supplyAsync(
              () -> filterTraces(context, preProcessedRequest, attributeMap, scope), queryExecutor);

      // Get the total API Traces in a separate query because this will scale better
      // for large data-set
      tracesResponseBuilder.setTotal(getTotalFilteredTraces(context, preProcessedRequest, scope));
      tracesResponseBuilder.addAllTraces(filteredTraceFuture.join());
      TracesResponse response = tracesResponseBuilder.build();
      LOG.debug("Traces Service Response: {}", response);

      return response;
    } finally {
      queryExecutionTimer.record(Duration.between(start, Instant.now()).toMillis(), MILLISECONDS);
    }
  }

  @VisibleForTesting
  List<Trace> filterTraces(
      RequestContext context,
      TracesRequest request,
      Map<String, AttributeMetadata> attributeMetadataMap,
      TraceScope scope) {

    Map<String, AttributeMetadata> resultKeyToAttributeMetadataMap =
        AttributeMetadataUtil.remapAttributeMetadataByResultKey(
            request.getSelectionList(), attributeMetadataMap);

    QueryRequest.Builder builder = createQueryWithFilter(request, scope, context);

    if (!request.getSelectionList().isEmpty()) {
      request
          .getSelectionList()
          .forEach(
              exp ->
                  builder.addSelection(QueryAndGatewayDtoConverter.convertToQueryExpression(exp)));
    }

    // Adds the parent span id selection to the query builder for the span event
    addSortLimitAndOffset(request, builder);

    List<Trace> tracesResult = new ArrayList<>();
    QueryRequest queryRequest = builder.build();
    Iterator<ResultSetChunk> resultSetChunkIterator =
        queryServiceClient.executeQuery(context, queryRequest);

    // form the result
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received chunk: " + chunk.toString());
      }

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        Trace.Builder traceBuilder = Trace.newBuilder();
        for (int i = 0; i < chunk.getResultSetMetadata().getColumnMetadataCount(); i++) {
          ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
          String attrName = metadata.getColumnName();
          traceBuilder.putAttributes(
              metadata.getColumnName(),
              QueryAndGatewayDtoConverter.convertToGatewayValue(
                  attrName, row.getColumn(i), resultKeyToAttributeMetadataMap));
        }

        tracesResult.add(traceBuilder.build());
      }
    }
    return tracesResult;
  }

  int getTotalFilteredTraces(RequestContext context, TracesRequest request, TraceScope scope) {
    int total = -1;
    if (request.getFetchTotal()) {
      total = 0;
      Builder queryBuilder = createQueryWithFilter(request, scope, context);
      // validated that the selection is not empty
      if (request.getSelectionCount() < 1) {
        throw new IllegalArgumentException("Query request does not have any selection");
      }

      String firstSelectionAttributeId =
          ExpressionReader.getAttributeIdFromAttributeSelection(request.getSelection(0))
              .orElseThrow();
      QueryRequest queryRequest =
          queryBuilder
              .addSelection(createCountByColumnSelection(firstSelectionAttributeId))
              .setLimit(1)
              .build();

      Iterator<ResultSetChunk> resultSetChunkIterator =
          queryServiceClient.executeQuery(context, queryRequest);
      while (resultSetChunkIterator.hasNext()) {
        ResultSetChunk chunk = resultSetChunkIterator.next();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received chunk: " + chunk.toString());
        }

        // There should be only 1 result
        if (chunk.getRowCount() != 1
            && chunk.getResultSetMetadata().getColumnMetadataCount() != 1) {
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
    }
    return total;
  }

  private Builder createQueryWithFilter(
      TracesRequest request, TraceScope scope, RequestContext requestContext) {

    Filter filter =
        QueryAndGatewayDtoConverter.addTimeAndSpaceFiltersAndConvertToQueryFilter(
            request.getStartTimeMillis(),
            request.getEndTimeMillis(),
            request.getSpaceId(),
            getTimestampAttributeId(this.attributeMetadataProvider, requestContext, scope.name()),
            getSpaceAttributeId(this.attributeMetadataProvider, requestContext, scope.name()),
            request.getFilter());
    return QueryRequest.newBuilder().setFilter(filter);
  }

  // Adds the sort, limit and offset information to the QueryService if it is requested
  private void addSortLimitAndOffset(TracesRequest request, Builder queryBuilder) {
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
