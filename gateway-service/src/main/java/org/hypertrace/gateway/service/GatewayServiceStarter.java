package org.hypertrace.gateway.service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.hypertrace.core.grpcutils.server.InterceptorUtil;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.gateway.service.entity.config.InteractionConfigs;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GatewayServiceStarter extends PlatformService {

  private static final Logger LOG = LoggerFactory.getLogger(GatewayServiceStarter.class);
  private static final String SERVICE_NAME_CONFIG = "service.name";
  private static final String SERVICE_PORT_CONFIG = "service.port";
  private static final int DEFAULT_PORT = 50071;

  private String serviceName;
  private Server server;

  public GatewayServiceStarter(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    serviceName = getAppConfig().getString(SERVICE_NAME_CONFIG);
    int port =
        getAppConfig().hasPath(SERVICE_PORT_CONFIG)
            ? getAppConfig().getInt(SERVICE_PORT_CONFIG)
            : DEFAULT_PORT;

    InteractionConfigs.init(getAppConfig());
    TimestampConfigs.init(getAppConfig());

    GatewayServiceImpl ht = new GatewayServiceImpl(getAppConfig());

    server = ServerBuilder.forPort(port).addService(InterceptorUtil.wrapInterceptors(ht)).build();
  }

  @Override
  protected void doStart() {
    LOG.info("Starting GatewayService..");
    try {
      server.start();
      server.awaitTermination();
    } catch (IOException e) {
      LOG.error("Fail to start the server.");
      throw new RuntimeException(e);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
  }

  @Override
  protected void doStop() {
    LOG.info("Shutting down service: {}", serviceName);
    while (!server.isShutdown()) {
      server.shutdown();
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
    }
  }

  @Override
  public boolean healthCheck() {
    return true;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }
}
