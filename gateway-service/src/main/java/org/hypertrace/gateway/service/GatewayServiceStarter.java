package org.hypertrace.gateway.service;

import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class GatewayServiceStarter extends StandAloneGrpcPlatformServiceContainer {

  public GatewayServiceStarter(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  public GrpcPlatformServiceFactory getServiceFactory() {
    return new GatewayServiceFactory();
  }
}
