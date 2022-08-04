package org.hypertrace.gateway.service;

import java.util.List;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.gateway.service.entity.config.InteractionConfigs;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;

public class GatewayServiceFactory implements GrpcPlatformServiceFactory {
  private static final String SERVICE_NAME = "gateway-service";

  @Override
  public List<GrpcPlatformService> buildServices(GrpcServiceContainerEnvironment environment) {
    InteractionConfigs.init(environment.getConfig(SERVICE_NAME));
    TimestampConfigs.init(environment.getConfig(SERVICE_NAME));
    return List.of(
        new GrpcPlatformService(
            new GatewayServiceImpl(
                environment.getConfig(SERVICE_NAME), environment.getChannelRegistry())));
  }
}
