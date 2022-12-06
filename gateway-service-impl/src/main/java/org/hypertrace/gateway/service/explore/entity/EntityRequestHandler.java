package org.hypertrace.gateway.service.explore.entity;

import com.google.common.annotations.VisibleForTesting;
import java.util.Iterator;
import java.util.Set;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.explore.ExploreRequestContext;
import org.hypertrace.gateway.service.explore.RequestHandler;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

/**
 * {@link EntityRequestHandler} is currently used only when the selections, group bys and filters
 * are on EDS. Can be extended later to support multiple sources. Only needed, when there is a group
 * by on the request, else can directly use {@link
 * org.hypertrace.gateway.service.v1.entity.EntitiesRequest}
 *
 * <p>Currently,
 *
 * <ul>
 *   <li>Query to {@link
 *       org.hypertrace.gateway.service.common.datafetcher.QueryServiceEntityFetcher} with the time
 *       filter to get set of entity ids. Can be extended to support QS filters
 *   <li>Query to {@link EntityServiceEntityFetcher} with selections, group bys, and filters with an
 *       IN clause on entity ids
 * </ul>
 */
public class EntityRequestHandler extends RequestHandler {
  private final AttributeMetadataProvider attributeMetadataProvider;

  private final QueryServiceEntityFetcher queryServiceEntityFetcher;
  private final EntityServiceEntityFetcher entityServiceEntityFetcher;

  public EntityRequestHandler(
      AttributeMetadataProvider attributeMetadataProvider,
      EntityIdColumnsConfigs entityIdColumnsConfigs,
      QueryServiceClient queryServiceClient,
      EntityQueryServiceClient entityQueryServiceClient) {
    super(
        queryServiceClient,
        entityQueryServiceClient,
        attributeMetadataProvider,
        entityIdColumnsConfigs);

    this.attributeMetadataProvider = attributeMetadataProvider;
    this.queryServiceEntityFetcher =
        new QueryServiceEntityFetcher(
            queryServiceClient, attributeMetadataProvider, entityIdColumnsConfigs);
    this.entityServiceEntityFetcher =
        new EntityServiceEntityFetcher(
            attributeMetadataProvider, entityIdColumnsConfigs, entityQueryServiceClient);
  }

  @VisibleForTesting
  public EntityRequestHandler(
      AttributeMetadataProvider attributeMetadataProvider,
      QueryServiceClient queryServiceClient,
      EntityQueryServiceClient entityQueryServiceClient,
      QueryServiceEntityFetcher queryServiceEntityFetcher,
      EntityServiceEntityFetcher entityServiceEntityFetcher,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    super(
        queryServiceClient,
        entityQueryServiceClient,
        attributeMetadataProvider,
        entityIdColumnsConfigs);

    this.attributeMetadataProvider = attributeMetadataProvider;
    this.queryServiceEntityFetcher = queryServiceEntityFetcher;
    this.entityServiceEntityFetcher = entityServiceEntityFetcher;
  }

  @Override
  public ExploreResponse.Builder handleRequest(
      ExploreRequestContext requestContext, ExploreRequest exploreRequest) {
    // Track if we have Group By so we can determine if we need to do Order By, Limit and Offset
    // ourselves.
    if (!exploreRequest.getGroupByList().isEmpty()) {
      requestContext.setHasGroupBy(true);
    }

    Set<String> entityIds = getEntityIds(requestContext, exploreRequest);
    ExploreResponse.Builder builder = ExploreResponse.newBuilder();

    if (entityIds.isEmpty()) {
      return builder;
    }

    Iterator<ResultSetChunk> resultSetChunkIterator =
        entityServiceEntityFetcher.getResults(requestContext, exploreRequest, entityIds);

    readChunkResults(requestContext, builder, resultSetChunkIterator);

    // If there's a Group By in the request, we need to do the sorting and pagination ourselves.
    if (requestContext.hasGroupBy()) {
      sortAndPaginatePostProcess(
          builder,
          requestContext.getOrderByExpressions(),
          requestContext.getRowLimitBeforeRest(),
          requestContext.getOffset());
    }

    if (requestContext.hasGroupBy() && requestContext.getIncludeRestGroup()) {
      getTheRestGroupRequestHandler()
          .getRowsForTheRestGroup(requestContext, exploreRequest, builder);
    }

    return builder;
  }
}
