package org.hypertrace.gateway.service.explore;

import com.google.protobuf.GeneratedMessageV3;
import java.util.HashMap;
import java.util.stream.Stream;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.AbstractServiceTest;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.config.ScopeFilterConfigs;
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
      AttributeMetadataProvider attributeMetadataProvider,
      ScopeFilterConfigs scopeFilterConfigs) {
    ExploreService exploreService =
        new ExploreService(queryServiceClient, 500, attributeMetadataProvider, scopeFilterConfigs);
    return exploreService.explore(TENANT_ID, request, new HashMap<>());
  }
}
