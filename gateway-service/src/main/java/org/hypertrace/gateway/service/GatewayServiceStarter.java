package org.hypertrace.gateway.service;

import java.util.List;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServerDefinition;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class GatewayServiceStarter extends StandAloneGrpcPlatformServiceContainer {

  public GatewayServiceStarter(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected List<GrpcPlatformServerDefinition> getServerDefinitions() {
    return List.of(
        GrpcPlatformServerDefinition.builder()
            .name(this.getServiceName())
            .port(this.getServicePort())
            .serviceFactory(new GatewayServiceFactory())
            .build());
  }
}
