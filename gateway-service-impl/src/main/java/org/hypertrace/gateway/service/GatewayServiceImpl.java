package org.hypertrace.gateway.service;

import static org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ServiceException;
import com.typesafe.config.Config;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.client.AttributeServiceClient;
import org.hypertrace.core.attribute.service.client.config.AttributeServiceClientConfig;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.query.service.api.QueryServiceGrpc;
import org.hypertrace.core.query.service.client.QueryServiceConfig;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc;
import org.hypertrace.entity.query.service.v1.EntityQueryServiceGrpc.EntityQueryServiceBlockingStub;
import org.hypertrace.entity.service.client.config.EntityServiceClientConfig;
import org.hypertrace.gateway.service.baseline.BaselineService;
import org.hypertrace.gateway.service.baseline.BaselineServiceImpl;
import org.hypertrace.gateway.service.baseline.BaselineServiceQueryExecutor;
import org.hypertrace.gateway.service.baseline.BaselineServiceQueryParser;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.EntityService;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.entity.config.LogConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorConfig;
import org.hypertrace.gateway.service.executor.QueryExecutorServiceFactory;
import org.hypertrace.gateway.service.explore.ExploreService;
import org.hypertrace.gateway.service.logevent.LogEventsService;
import org.hypertrace.gateway.service.span.SpanService;
import org.hypertrace.gateway.service.trace.TracesService;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesRequest;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityRequest;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityResponse;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;
import org.hypertrace.gateway.service.v1.log.events.LogEventsRequest;
import org.hypertrace.gateway.service.v1.log.events.LogEventsResponse;
import org.hypertrace.gateway.service.v1.span.SpansResponse;
import org.hypertrace.gateway.service.v1.trace.TracesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway service for all entity data. This should be a light weight gateway which looks at the
 * entity type in the incoming requests, translates (if required) the request into the request
 * expected by the downstream service and forwards the response back to the original client (again
 * translating if required). This should not have any business logic, only translation logic.
 */
public class GatewayServiceImpl extends GatewayServiceGrpc.GatewayServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(GatewayServiceImpl.class);

  private static final String QUERY_SERVICE_CONFIG_KEY = "query.service.config";
  private static final String REQUEST_TIMEOUT_CONFIG_KEY = "request.timeout";

  private Counter requestStatusErrorCounter;
  private Counter requestStatusSuccessCounter;
  private static final String SERVICE_REQUESTS_STATUS_COUNTER =
      "hypertrace.gateway.service.requests.status";

  private final TracesService traceService;
  private final SpanService spanService;
  private final EntityService entityService;
  private final ExploreService exploreService;
  private final BaselineService baselineService;
  private final LogEventsService logEventsService;

  public GatewayServiceImpl(Config appConfig, GrpcChannelRegistry grpcChannelRegistry) {
    AttributeServiceClientConfig asConfig = AttributeServiceClientConfig.from(appConfig);
    AttributeServiceClient asClient =
        new AttributeServiceClient(
            grpcChannelRegistry.forPlaintextAddress(asConfig.getHost(), asConfig.getPort()));
    AttributeMetadataProvider attributeMetadataProvider = new AttributeMetadataProvider(asClient);
    EntityIdColumnsConfigs entityIdColumnsConfigs = EntityIdColumnsConfigs.fromConfig(appConfig);

    Config untypedQsConfig = appConfig.getConfig(QUERY_SERVICE_CONFIG_KEY);
    QueryServiceConfig qsConfig = new QueryServiceConfig(untypedQsConfig);
    QueryServiceClient queryServiceClient =
        new QueryServiceClient(
            QueryServiceGrpc.newBlockingStub(
                    grpcChannelRegistry.forPlaintextAddress(
                        qsConfig.getQueryServiceHost(), qsConfig.getQueryServicePort()))
                .withCallCredentials(getClientCallCredsProvider().get()),
            Duration.ofMillis(untypedQsConfig.getInt(REQUEST_TIMEOUT_CONFIG_KEY)));
    ExecutorService queryExecutor =
        QueryExecutorServiceFactory.buildExecutorService(QueryExecutorConfig.from(appConfig));

    EntityServiceClientConfig esConfig = EntityServiceClientConfig.from(appConfig);
    final EntityQueryServiceBlockingStub eqsStub =
        EntityQueryServiceGrpc.newBlockingStub(
                grpcChannelRegistry.forPlaintextAddress(esConfig.getHost(), esConfig.getPort()))
            .withCallCredentials(getClientCallCredsProvider().get());
    EntityQueryServiceClient eqsClient =
        new EntityQueryServiceClient(
            grpcChannelRegistry.forPlaintextAddress(esConfig.getHost(), esConfig.getPort()));

    ScopeFilterConfigs scopeFilterConfigs = new ScopeFilterConfigs(appConfig);
    LogConfig logConfig = new LogConfig(appConfig);
    this.traceService =
        new TracesService(
            queryServiceClient, attributeMetadataProvider, scopeFilterConfigs, queryExecutor);
    this.spanService =
        new SpanService(queryServiceClient, attributeMetadataProvider, queryExecutor);
    this.entityService =
        new EntityService(
            queryServiceClient,
            eqsClient,
            eqsStub,
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            scopeFilterConfigs,
            logConfig,
            queryExecutor);
    this.exploreService =
        new ExploreService(
            queryServiceClient,
            eqsClient,
            attributeMetadataProvider,
            scopeFilterConfigs,
            entityIdColumnsConfigs);
    BaselineServiceQueryParser baselineServiceQueryParser =
        new BaselineServiceQueryParser(attributeMetadataProvider);
    BaselineServiceQueryExecutor baselineServiceQueryExecutor =
        new BaselineServiceQueryExecutor(queryServiceClient);
    this.baselineService =
        new BaselineServiceImpl(
            attributeMetadataProvider,
            baselineServiceQueryParser,
            baselineServiceQueryExecutor,
            entityIdColumnsConfigs);
    this.logEventsService = new LogEventsService(queryServiceClient, attributeMetadataProvider);
    initMetrics();
  }

  private void initMetrics() {
    requestStatusErrorCounter =
        PlatformMetricsRegistry.registerCounter(
            SERVICE_REQUESTS_STATUS_COUNTER, ImmutableMap.of("error", "true"));
    requestStatusSuccessCounter =
        PlatformMetricsRegistry.registerCounter(
            SERVICE_REQUESTS_STATUS_COUNTER, ImmutableMap.of("error", "false"));
  }

  @Override
  public void getTraces(
      org.hypertrace.gateway.service.v1.trace.TracesRequest request,
      io.grpc.stub.StreamObserver<org.hypertrace.gateway.service.v1.trace.TracesResponse>
          responseObserver) {

    Optional<String> tenantId =
        org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      requestStatusErrorCounter.increment();
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      RequestContext requestContext =
          new RequestContext(org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get());

      TracesResponse response = traceService.getTracesByFilter(requestContext, request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling traces request: {}", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void getSpans(
      org.hypertrace.gateway.service.v1.span.SpansRequest request,
      io.grpc.stub.StreamObserver<org.hypertrace.gateway.service.v1.span.SpansResponse>
          responseObserver) {
    Optional<String> tenantId =
        org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      requestStatusErrorCounter.increment();
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      RequestContext context =
          new RequestContext(org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get());
      SpansResponse response = spanService.getSpansByFilter(context, request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling spans request: {}", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void getEntities(
      org.hypertrace.gateway.service.v1.entity.EntitiesRequest request,
      StreamObserver<org.hypertrace.gateway.service.v1.entity.EntitiesResponse> responseObserver) {

    LOG.debug("Received request: {}", request);

    Optional<String> tenantId =
        org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      requestStatusErrorCounter.increment();
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(request.getEntityType()),
          "EntityType is mandatory in the request.");

      Preconditions.checkArgument(
          request.getSelectionCount() > 0, "Selection list can't be empty in the request.");

      Preconditions.checkArgument(
          request.getStartTimeMillis() > 0
              && request.getEndTimeMillis() > 0
              && request.getStartTimeMillis() < request.getEndTimeMillis(),
          "Invalid time range. Both start and end times have to be valid timestamps.");

      EntitiesResponse response =
          entityService.getEntities(
              new RequestContext(
                  org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get()),
              request);

      LOG.debug("Received response: {}", response);

      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling entities request: {}.", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void updateEntity(
      UpdateEntityRequest request, StreamObserver<UpdateEntityResponse> responseObserver) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received request: {}", request);
    }

    Optional<String> tenantId =
        org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      requestStatusErrorCounter.increment();
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      UpdateEntityResponse response =
          entityService.updateEntity(
              new RequestContext(
                  org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get()),
              request);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Received response: {}", response);
      }
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling UpdateEntityRequest: {}.", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void bulkUpdateEntities(
      BulkUpdateEntitiesRequest request,
      StreamObserver<BulkUpdateEntitiesResponse> responseObserver) {
    LOG.debug("Received request: {}", request);

    try {
      org.hypertrace.core.grpcutils.context.RequestContext.CURRENT
          .get()
          .getTenantId()
          .orElseThrow(() -> new ServiceException("Tenant id is missing in the request."));

      BulkUpdateEntitiesResponse response =
          entityService.bulkUpdateEntities(
              new RequestContext(
                  org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get()),
              request);

      LOG.debug("Received response: {}", response);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling bulkUpdateEntities: {}.", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void bulkUpdateAllMatchingEntities(
      final BulkUpdateAllMatchingEntitiesRequest request,
      final StreamObserver<BulkUpdateAllMatchingEntitiesResponse> responseObserver) {
    LOG.debug("Received request: {}", request);

    try {
      org.hypertrace.core.grpcutils.context.RequestContext.CURRENT
          .get()
          .getTenantId()
          .orElseThrow(() -> new ServiceException("Tenant id is missing in the request."));

      final BulkUpdateAllMatchingEntitiesResponse response =
          entityService.bulkUpdateAllMatchingEntities(
              new RequestContext(
                  org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get()),
              request);

      LOG.debug("Received response: {}", response);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling bulkUpdateEntities: {}.", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void getBaselineForEntities(
      BaselineEntitiesRequest request, StreamObserver<BaselineEntitiesResponse> responseObserver) {
    Optional<String> tenantId =
        org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      requestStatusErrorCounter.increment();
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      BaselineEntitiesResponse response =
          baselineService.getBaselineForEntities(
              new RequestContext(
                  org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get()),
              request);

      LOG.debug("Received response: {}", response);

      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling entities request: {}.", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void explore(ExploreRequest request, StreamObserver<ExploreResponse> responseObserver) {
    Optional<String> tenantId =
        org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      requestStatusErrorCounter.increment();
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {
      ExploreResponse response =
          exploreService.explore(
              new RequestContext(
                  org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get()),
              request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling explore request: {}", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }

  @Override
  public void getLogEvents(
      LogEventsRequest request, StreamObserver<LogEventsResponse> responseObserver) {
    Optional<String> tenantId =
        org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get().getTenantId();
    if (tenantId.isEmpty()) {
      requestStatusErrorCounter.increment();
      responseObserver.onError(new ServiceException("Tenant id is missing in the request."));
      return;
    }

    try {

      RequestContext context =
          new RequestContext(org.hypertrace.core.grpcutils.context.RequestContext.CURRENT.get());
      LogEventsResponse response = logEventsService.getLogEventsByFilter(context, request);
      responseObserver.onNext(response);
      responseObserver.onCompleted();
      requestStatusSuccessCounter.increment();
    } catch (Exception e) {
      LOG.error("Error while handling logEvents request: {}", request, e);
      requestStatusErrorCounter.increment();
      responseObserver.onError(e);
    }
  }
}
