package org.hypertrace.gateway.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.hypertrace.gateway.service.entity.config.InteractionConfigs;
import org.hypertrace.gateway.service.entity.config.TimestampConfigs;
import org.junit.jupiter.api.BeforeEach;

/**
 * Abstract class with common setup needed for tests. Initializes InteractionConfigs
 */
public class AbstractGatewayServiceTest {
  protected static final String TENANT_ID = "tenant1";

  Config config;

  @BeforeEach
  public void setup() {
    String configFilePath =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("configs/gateway-service/application.conf")
            .getPath();
    if (configFilePath == null) {
      throw new RuntimeException("Cannot find gateway-service config file in the classpath");
    }

    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    config = ConfigFactory.load(fileConfig);
    InteractionConfigs.init(config);
    TimestampConfigs.init(config);
  }

  public Config getConfig() {
    return config;
  }
}
