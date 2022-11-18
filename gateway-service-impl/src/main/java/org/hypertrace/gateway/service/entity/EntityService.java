package org.hypertrace.gateway.service.entity;

import static org.hypertrace.gateway.service.v1.common.Operator.IN;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
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
import org.hypertrace.gateway.service.common.util.QueryExpressionUtil;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.entity.config.LogConfig;
import org.hypertrace.gateway.service.entity.query.EntityExecutionContext;
import org.hypertrace.gateway.service.entity.query.ExecutionTreeBuilder;
import org.hypertrace.gateway.service.entity.query.QueryNode;
import org.hypertrace.gateway.service.entity.query.visitor.ExecutionVisitor;
import org.hypertrace.gateway.service.entity.update.EdsEntityUpdater;
import org.hypertrace.gateway.service.entity.update.UpdateExecutionContext;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateEntitiesResponse;
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
  private static final BulkUpdateEntitiesRequestValidator BULK_UPDATE_ENTITIES_REQUEST_VALIDATOR =
      new BulkUpdateEntitiesRequestValidator();
  private final AttributeMetadataProvider metadataProvider;
  private final EntityIdColumnsConfigs entityIdColumnsConfigs;
  private final ExecutorService queryExecutor;
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
      EntityQueryServiceClient edsQueryServiceClient,
      AttributeMetadataProvider metadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      ScopeFilterConfigs scopeFilterConfigs,
      LogConfig logConfig,
      ExecutorService queryExecutor) {
    this.metadataProvider = metadataProvider;
    this.entityIdColumnsConfigs = entityIdColumnsConfigs;
    this.queryExecutor = queryExecutor;
    this.interactionsFetcher =
        new EntityInteractionsFetcher(qsClient, metadataProvider, queryExecutor);
    this.requestPreProcessor = new RequestPreProcessor(metadataProvider, scopeFilterConfigs);
    this.responsePostProcessor = new ResponsePostProcessor();
    this.edsEntityUpdater = new EdsEntityUpdater(edsQueryServiceClient);
    this.logConfig = logConfig;

    registerEntityFetchers(qsClient, edsQueryServiceClient);
    initMetrics();
  }

  private void registerEntityFetchers(
      QueryServiceClient queryServiceClient, EntityQueryServiceClient edsQueryServiceClient) {
    EntityQueryHandlerRegistry registry = EntityQueryHandlerRegistry.get();
    registry.registerEntityFetcher(
        AttributeSource.QS.name(),
        new QueryServiceEntityFetcher(
            queryServiceClient, metadataProvider, entityIdColumnsConfigs));
    registry.registerEntityFetcher(
        AttributeSource.EDS.name(),
        new EntityDataServiceEntityFetcher(
            edsQueryServiceClient, metadataProvider, entityIdColumnsConfigs));
  }

  private void initMetrics() {
    this.queryBuildTimer =
        PlatformMetricsRegistry.registerTimer("hypertrace.entities.query.build", ImmutableMap.of());
    this.queryExecutionTimer =
        PlatformMetricsRegistry.registerTimer(
            "hypertrace.entities.query.execution", ImmutableMap.of());
  }

  /**
   * Method to get entities along with the requested attributes and metrics. Does it in multiple
   * steps:
   *
   * <ul>
   *   <li>1) Construct the filter tree from the filter condition in the query
   *   <li>2) Optimize the filter tree by merging nodes corresponding to the same data source
   *   <li>3) Constructs the complete execution tree
   *   <li>4) Passes the execution tree through the ExecutionVisitor to get the result
   *   <li>5) Adds entity interaction data if requested for
   * </ul>
   */
  public EntitiesResponse getEntities(
      RequestContext requestContext, EntitiesRequest originalRequest) {
    Instant start = Instant.now();
    String timestampAttributeId =
        AttributeMetadataUtil.getTimestampAttributeId(
            metadataProvider, requestContext, originalRequest.getEntityType());

    // Set the size for percentiles in order by if it is not set. This is to give UI the time to fix
    // the bug which does not set the size when they have order by in the request.
    originalRequest = OrderByPercentileSizeSetter.setPercentileSize(originalRequest);
    EntitiesRequestContext entitiesRequestContext =
        new EntitiesRequestContext(
            requestContext.getGrpcContext(),
            originalRequest.getStartTimeMillis(),
            originalRequest.getEndTimeMillis(),
            originalRequest.getEntityType(),
            timestampAttributeId);
    EntitiesRequest preProcessedRequest =
        requestPreProcessor.process(originalRequest, entitiesRequestContext);

    preProcessedRequest = addEntityIdsFromInteractionFilter(requestContext, preProcessedRequest);

    LOG.info("Preprocessed query is {}", preProcessedRequest);
    EntityExecutionContext executionContext =
        new EntityExecutionContext(
            metadataProvider, entityIdColumnsConfigs, entitiesRequestContext, preProcessedRequest);
    ExecutionTreeBuilder executionTreeBuilder = new ExecutionTreeBuilder(executionContext);
    QueryNode executionTree = executionTreeBuilder.build();
    queryBuildTimer.record(
        Duration.between(start, Instant.now()).toMillis(), TimeUnit.MILLISECONDS);

    /*
     * EntityQueryHandlerRegistry.get() returns Singleton object, so, it's guaranteed that
     * it won't create new object for each request.
     */
    EntityResponse response =
        executionTree.acceptVisitor(
            new ExecutionVisitor(
                executionContext, EntityQueryHandlerRegistry.get(), this.queryExecutor));

    EntityFetcherResponse entityFetcherResponse = response.getEntityFetcherResponse();

    List<Entity.Builder> results =
        this.responsePostProcessor.transform(
            executionContext,
            new ArrayList<>(entityFetcherResponse.getEntityKeyBuilderMap().values()));

    // Add interactions.
    if (!results.isEmpty()) {
      addEntityInteractions(
          requestContext, preProcessedRequest, entityFetcherResponse.getEntityKeyBuilderMap());
    }

    EntitiesResponse.Builder responseBuilder =
        EntitiesResponse.newBuilder().setTotal(Long.valueOf(response.getTotal()).intValue());

    results.forEach(e -> responseBuilder.addEntity(e.build()));

    long queryExecutionTime = Duration.between(start, Instant.now()).toMillis();
    if (queryExecutionTime > logConfig.getQueryThresholdInMillis()) {
      LOG.info(
          "Total query execution took: {}(ms) for request: {}",
          queryExecutionTime,
          originalRequest);
    }

    queryExecutionTimer.record(queryExecutionTime, TimeUnit.MILLISECONDS);
    return responseBuilder.build();
  }

  public UpdateEntityResponse updateEntity(
      RequestContext requestContext, UpdateEntityRequest request) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(request.getEntityType()),
        "entity_type is mandatory in the request.");

    Map<String, AttributeMetadata> attributeMetadataMap =
        metadataProvider.getAttributesMetadata(requestContext, request.getEntityType());

    updateEntityRequestValidator.validate(request, attributeMetadataMap);

    UpdateExecutionContext updateExecutionContext =
        new UpdateExecutionContext(requestContext.getHeaders(), attributeMetadataMap);

    // Validations have ensured that only EDS update operation is supported.
    // If in the future we need more sophisticated update across data sources, we'll need
    // to add the capability similar to what we have for querying.
    UpdateEntityResponse.Builder responseBuilder =
        edsEntityUpdater.update(request, updateExecutionContext);
    return responseBuilder.build();
  }

  public BulkUpdateEntitiesResponse bulkUpdateEntities(
      RequestContext requestContext, BulkUpdateEntitiesRequest request) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        metadataProvider.getAttributesMetadata(requestContext, request.getEntityType());

    Status status = BULK_UPDATE_ENTITIES_REQUEST_VALIDATOR.validate(request, attributeMetadataMap);
    if (!status.isOk()) {
      LOG.error("Bulk update entities request is not valid: {}", status.getDescription());
      throw status.asRuntimeException();
    }

    UpdateExecutionContext updateExecutionContext =
        new UpdateExecutionContext(requestContext.getHeaders(), attributeMetadataMap);

    return edsEntityUpdater.bulkUpdateEntities(request, updateExecutionContext);
  }

  private EntitiesRequest addEntityIdsFromInteractionFilter(
      RequestContext requestContext, EntitiesRequest preProcessedRequest) {
    List<EntityKey> entityKeys =
        this.interactionsFetcher.fetchInteractionsIdsIfNecessary(
            requestContext, preProcessedRequest);
    if (!entityKeys.isEmpty()) {
      Filter entityIdsInFilter =
          createEntityKeysInFilter(requestContext, preProcessedRequest.getEntityType(), entityKeys);

      EntitiesRequest.Builder preProcessRequestBuilder = preProcessedRequest.toBuilder();
      // This check is required. During the pre-process step,
      // filter is set to default filter if no filters are provided.
      if (!preProcessRequestBuilder.hasFilter()
          || Filter.getDefaultInstance().equals(preProcessRequestBuilder.getFilter())) {
        preProcessRequestBuilder.setFilter(entityIdsInFilter);
      } else {
        preProcessRequestBuilder.setFilter(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(preProcessRequestBuilder.getFilter())
                .addChildFilter(entityIdsInFilter)
                .build());
      }
      preProcessedRequest = preProcessRequestBuilder.build();
    }
    return preProcessedRequest;
  }

  private Filter createEntityKeysInFilter(
      RequestContext requestContext, String entityType, List<EntityKey> entityKeys) {
    List<String> entityIds =
        entityKeys.stream()
            .filter(key -> key.getAttributes().size() == 1) // filter out all keys with single id
            .map(EntityKey::toString)
            .collect(Collectors.toUnmodifiableList());

    List<String> idAttributeIds =
        AttributeMetadataUtil.getIdAttributeIds(
            metadataProvider, entityIdColumnsConfigs, requestContext, entityType);

    if (idAttributeIds.size() != 1) {
      LOG.error("Entity Type {} should have single ID Attribute", entityType);
      throw Status.UNIMPLEMENTED
          .withDescription("Entity Type " + entityType + " should have single ID Attribute")
          .asRuntimeException();
    }

    return Filter.newBuilder()
        .setLhs(QueryExpressionUtil.buildAttributeExpression(idAttributeIds.get(0)))
        .setOperator(IN)
        .setRhs(QueryExpressionUtil.getLiteralExpression(entityIds).build())
        .build();
  }

  private void addEntityInteractions(
      RequestContext requestContext, EntitiesRequest request, Map<EntityKey, Builder> result) {
    if (InteractionsRequest.getDefaultInstance().equals(request.getIncomingInteractions())
        && InteractionsRequest.getDefaultInstance().equals(request.getOutgoingInteractions())) {
      return;
    }

    interactionsFetcher.populateEntityInteractions(requestContext, request, result);
  }
}
