package org.hypertrace.gateway.service.explore.entity;

import com.google.protobuf.GeneratedMessageV3;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.gateway.service.EntityTypesProvider;
import org.hypertrace.gateway.service.common.AbstractServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.GatewayServiceConfig;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.EntityService;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.EntitiesResponse;

public class EntityServiceTest extends AbstractServiceTest<EntitiesRequest, EntitiesResponse> {
  private static final String SUITE_NAME = "entity";

  public static Stream<String> data() {
    return getTestFileNames(SUITE_NAME);
  }

  @Override
  protected String getTestSuiteName() {
    return SUITE_NAME;
  }

  @Override
  protected GeneratedMessageV3.Builder getGatewayServiceRequestBuilder() {
    return EntitiesRequest.newBuilder();
  }

  @Override
  protected GeneratedMessageV3.Builder getGatewayServiceResponseBuilder() {
    return EntitiesResponse.newBuilder();
  }

  @Override
  protected EntitiesResponse executeApi(
      GatewayServiceConfig gatewayServiceConfig,
      EntitiesRequest request,
      QueryServiceClient queryServiceClient,
      EntityQueryServiceClient entityQueryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      EntityTypesProvider entityTypesProvider) {
    EntityService entityService =
        new EntityService(
            gatewayServiceConfig,
            queryServiceClient,
            entityQueryServiceClient,
            null,
            attributeMetadataProvider,
            Executors.newFixedThreadPool(1));
    return entityService.getEntities(
        new RequestContext(
            org.hypertrace.core.grpcutils.context.RequestContext.forTenantId(TENANT_ID)),
        request);
  }
}
