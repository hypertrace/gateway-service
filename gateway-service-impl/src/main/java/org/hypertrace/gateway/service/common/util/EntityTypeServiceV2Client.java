package org.hypertrace.gateway.service.common.util;

import io.grpc.Channel;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceGrpc;
import org.hypertrace.entity.type.service.v2.QueryEntityTypesRequest;

public class EntityTypeServiceV2Client {
  private final EntityTypeServiceGrpc.EntityTypeServiceBlockingStub blockingStub;

  public EntityTypeServiceV2Client(Channel channel) {
    this.blockingStub =
        EntityTypeServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public List<EntityType> getAllEntityTypes(RequestContext requestContext) {
    return requestContext
        .call(
            () -> this.blockingStub.queryEntityTypes(QueryEntityTypesRequest.newBuilder().build()))
        .getEntityTypeList();
  }
}
