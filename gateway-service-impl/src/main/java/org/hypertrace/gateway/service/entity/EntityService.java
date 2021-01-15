package org.hypertrace.gateway.service.entity;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.OrderByPercentileSizeSetter;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.datafetcher.EntityDataServiceEntityFetcher;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.common.datafetcher.EntityInteractionsFetcher;
import org.hypertrace.gateway.service.common.datafetcher.EntityResponse;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.common.transformer.RequestPreProcessor;
import org.hypertrace.gateway.service.common.transformer.ResponsePostProcessor;
import org.hypertrace.gateway.service.common.util.AttributeMetadataUtil;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.entity.config.LogConfig;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.entity.query.ExecutionTreeBuilder;
import org.hypertrace.gateway.service.entity.query.QueryNode;
import org.hypertrace.gateway.service.entity.query.visitor.ExecutionVisitor;
import org.hypertrace.gateway.service.entity.update.EdsEntityUpdater;
import org.hypertrace.gateway.service.entity.update.UpdateExecutionContext;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.hypertrace.gateway.service.v1.entity.InteractionsRequest;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityRequest;
import org.hypertrace.gateway.service.v1.entity.UpdateEntityResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that provides a generic implementation to query any entities like API/SERVICE/BACKEND
 *
 * <p>If there is any specific implementation for a specific Entity Type, this can be extended
 */
public class EntityService {
  private static final Logger LOG = LoggerFactory.getLogger(EntityService.class);

  private static final UpdateEntityRequestValidator updateEntityRequestValidator =
      new UpdateEntityRequestValidator();
  private final AttributeMetadataProvider metadataProvider;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;
  private final EntityInteractionsFetcher interactionsFetcher;
  private final RequestPreProcessor requestPreProcessor;
  private final ResponsePostProcessor responsePostProcessor;
  private final EdsEntityUpdater edsEntityUpdater;
  private final LogConfig logConfig;
  // Metrics
  private Timer queryBuildTimer;
  private Timer queryExecutionTimer;

  public EntityService(
      QueryServiceClient qsClient,
      int qsRequestTimeout, EntityQueryServiceClient edsQueryServiceClient,
      AttributeMetadataProvider metadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      ScopeFilterConfigs scopeFilterConfigs,
      LogConfig logConfig) {
    this.metadataProvider = metadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    this.interactionsFetcher = new EntityInteractionsFetcher(qsClient, qsRequestTimeout, metadataProvider);
    this.requestPreProcessor = new RequestPreProcessor(metadataProvider, scopeFilterConfigs);
    this.responsePostProcessor = new ResponsePostProcessor();
    this.edsEntityUpdater = new EdsEntityUpdater(edsQueryServiceClient);
    this.logConfig = logConfig;

    registerEntityFetchers(qsClient, qsRequestTimeout, edsQueryServiceClient);
    initMetrics();
  }

  private void registerEntityFetchers(
      QueryServiceClient queryServiceClient, int qsRequestTimeout,
      EntityQueryServiceClient edsQueryServiceClient) {
    EntityQueryHandlerRegistry registry = EntityQueryHandlerRegistry.get();
    registry.registerEntityFetcher(
        AttributeSource.QS.name(),
        new QueryServiceEntityFetcher(queryServiceClient, qsRequestTimeout, metadataProvider, entityIdColumnsConfigs));
    registry.registerEntityFetcher(
        AttributeSource.EDS.name(),
        new EntityDataServiceEntityFetcher(edsQueryServiceClient, metadataProvider, entityIdColumnsConfigs));
  }

  private void initMetrics() {
    this.queryBuildTimer = new Timer();
    this.queryExecutionTimer = new Timer();
    PlatformMetricsRegistry.register("entities.query.build", queryBuildTimer);
    PlatformMetricsRegistry.register("entities.query.execution", queryExecutionTimer);
  }

  /**
   * Method to get entities along with the requested attributes and metrics.
   * Does it in multiple steps:
   * 1) Construct the filter tree from the filter condition in the query
   * 2) Optimize the filter tree by merging nodes corresponding to the same data source
   * 3) Constructs the complete execution tree
   * 4) Passes the execution tree through the ExecutionVisitor to get the result
   * 5) Adds entity interaction data if requested for
   */
  public EntitiesResponse getEntities(
      String tenantId, EntitiesRequest originalRequest, Map<String, String> requestHeaders) {
    long startTime = System.currentTimeMillis();
    String timestampAttributeId = AttributeMetadataUtil.getTimestampAttributeId(
        metadataProvider, new RequestContext(tenantId, requestHeaders), originalRequest.getEntityType());

    // Set the size for percentiles in order by if it is not set. This is to give UI the time to fix
    // the bug which does not set the size when they have order by in the request.
    originalRequest = OrderByPercentileSizeSetter.setPercentileSize(originalRequest);
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            tenantId,
            originalRequest.getStartTimeMillis(),
            originalRequest.getEndTimeMillis(),
            originalRequest.getEntityType(),
            timestampAttributeId,
            requestHeaders);
    EntitiesRequest preProcessedRequest = requestPreProcessor.transformFilter(originalRequest, entitiesRequestContext);

    ExecutionContext executionContext =
        ExecutionContext.from(metadataProvider, entityIdColumnsConfigs, preProcessedRequest, entitiesRequestContext);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    queryBuildTimer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);

    /*
    * EntityQueryHandlerRegistry.get() returns Singleton object, so, it's guaranteed that
    * it won't create new object for each request.
    */
    EntityResponse response =
        executionTree.acceptVisitor(new ExecutionVisitor(executionContext, EntityQueryHandlerRegistry.get()));

    EntityFetcherResponse entityFetcherResponse = response.getEntityFetcherResponse();
    Set<EntityKey> allEntityKeys = response.getEntityKeys();

    List<Entity.Builder> results =
        this.responsePostProcessor.transform(
            executionContext, new ArrayList<>(entityFetcherResponse.getEntityKeyBuilderMap().values()));

    // Add interactions.
    if (!results.isEmpty()) {
      addEntityInteractions(
          tenantId, preProcessedRequest, entityFetcherResponse.getEntityKeyBuilderMap(), requestHeaders);
    }

    EntitiesResponse.Builder responseBuilder =
        EntitiesResponse.newBuilder().setTotal(allEntityKeys.size());

    results.forEach(e -> responseBuilder.addEntity(e.build()));

    long queryExecutionTime = System.currentTimeMillis() - startTime;
    if (queryExecutionTime > logConfig.getQueryThresholdInMillis()) {
      LOG.info(
          "Total query execution took: {}(ms) for request: {}",
          queryExecutionTime,
          originalRequest);
    }

    queryExecutionTimer.update(queryExecutionTime, TimeUnit.MILLISECONDS);
    return responseBuilder.build();
  }

  public UpdateEntityResponse updateEntity(
      String tenantId, UpdateEntityRequest request, Map<String, String> requestHeaders) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(request.getEntityType()),
        "entity_type is mandatory in the request.");

    RequestContext requestContext = new RequestContext(tenantId, requestHeaders);

    Map<String, AttributeMetadata> attributeMetadataMap =
        metadataProvider.getAttributesMetadata(requestContext, request.getEntityType());

    updateEntityRequestValidator.validate(request, attributeMetadataMap);

    UpdateExecutionContext updateExecutionContext =
        new UpdateExecutionContext(requestHeaders, attributeMetadataMap);

    // Validations have ensured that only EDS update operation is supported.
    // If in the future we need more sophisticated update across data sources, we'll need
    // to add the capability similar to what we have for querying.
    UpdateEntityResponse.Builder responseBuilder =
        edsEntityUpdater.update(request, updateExecutionContext);
    return responseBuilder.build();
  }

  private void addEntityInteractions(
      String tenantId,
      EntitiesRequest request,
      Map<EntityKey, Builder> result,
      Map<String, String> requestHeaders) {
    if (InteractionsRequest.getDefaultInstance().equals(request.getIncomingInteractions())
        && InteractionsRequest.getDefaultInstance().equals(request.getOutgoingInteractions())) {
      return;
    }

    RequestContext requestContext = new RequestContext(tenantId, requestHeaders);

    interactionsFetcher.populateEntityInteractions(requestContext, request, result);
  }
}
