package org.hypertrace.gateway.service.common.util;

import com.google.common.collect.Lists;
import io.grpc.Channel;
import java.util.List;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.hypertrace.entity.type.service.v1.EntityTypeFilter;
import org.hypertrace.entity.type.service.v1.EntityTypeServiceGrpc;

public class EntityTypeServiceClient {
  private final EntityTypeServiceGrpc.EntityTypeServiceBlockingStub blockingStub;

  public EntityTypeServiceClient(Channel channel) {
    this.blockingStub =
        EntityTypeServiceGrpc.newBlockingStub(channel)
            .withCallCredentials(
                RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get());
  }

  public List<EntityType> getAllEntityTypes(RequestContext requestContext) {
    return Lists.newArrayList(
        requestContext.call(
            () -> this.blockingStub.queryEntityTypes(EntityTypeFilter.newBuilder().build())));
  }
}
