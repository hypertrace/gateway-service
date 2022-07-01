package org.hypertrace.gateway.service;

import java.util.List;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;

public class GatewayServiceFactory implements GrpcPlatformServiceFactory {
  private static final String SERVICE_NAME = "gateway-service";

  @Override
  public List<GrpcPlatformService> buildServices(GrpcServiceContainerEnvironment environment) {
    return List.of(
        new GrpcPlatformService(
            new GatewayServiceImpl(
                environment.getConfig(SERVICE_NAME), environment.getChannelRegistry())));
  }
}
