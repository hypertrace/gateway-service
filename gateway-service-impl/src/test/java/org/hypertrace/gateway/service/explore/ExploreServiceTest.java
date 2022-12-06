package org.hypertrace.gateway.service.explore;

import com.google.protobuf.GeneratedMessageV3;
import java.util.stream.Stream;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.gateway.service.common.AbstractServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
import org.hypertrace.gateway.service.common.util.QueryServiceClient;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.hypertrace.gateway.service.v1.explore.ExploreResponse;

public class ExploreServiceTest extends AbstractServiceTest<ExploreRequest, ExploreResponse> {
  private static final String SUITE_NAME = "explore";

  public static Stream<String> data() {
    return getTestFileNames(SUITE_NAME);
  }

  @Override
  protected String getTestSuiteName() {
    return SUITE_NAME;
  }

  @Override
  protected GeneratedMessageV3.Builder getGatewayServiceRequestBuilder() {
    return ExploreRequest.newBuilder();
  }

  @Override
  protected GeneratedMessageV3.Builder getGatewayServiceResponseBuilder() {
    return ExploreResponse.newBuilder();
  }

  @Override
  protected ExploreResponse executeApi(
      ExploreRequest request,
      QueryServiceClient queryServiceClient,
      EntityQueryServiceClient entityQueryServiceClient,
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFilterConfigs,
      EntityIdColumnsConfigs entityIdColumnsConfigs) {
    ExploreService exploreService =
        new ExploreService(
            queryServiceClient,
            entityQueryServiceClient,
            attributeMetadataProvider,
            scopeFilterConfigs,
            entityIdColumnsConfigs);
    return exploreService.explore(
        new RequestContext(
            org.hypertrace.core.grpcutils.context.RequestContext.forTenantId(TENANT_ID)),
        request);
  }
}
