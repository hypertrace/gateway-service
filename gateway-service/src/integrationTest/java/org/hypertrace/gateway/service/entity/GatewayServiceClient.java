package org.hypertrace.gateway.service.entity;

import io.grpc.Channel;
import org.hypertrace.gateway.service.GatewayServiceGrpc;

public class GatewayServiceClient {

  private final GatewayServiceGrpc.GatewayServiceBlockingStub blockingStub;

  public GatewayServiceClient(Channel channel) {
    blockingStub = GatewayServiceGrpc.newBlockingStub(channel);
  }

  public GatewayServiceGrpc.GatewayServiceBlockingStub getBlockingStub() {
    return blockingStub;
  }
}
