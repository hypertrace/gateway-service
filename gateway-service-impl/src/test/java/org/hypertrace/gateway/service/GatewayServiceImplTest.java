package org.hypertrace.gateway.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link GatewayServiceImpl}
 */
public class GatewayServiceImplTest {
  private static Config appConfig;

  @BeforeAll
  static void initConfig() {
    String configFilePath =
        Thread.currentThread()
            .getContextClassLoader()
            .getResource("configs/gateway-service/application.conf")
            .getPath();
    if (configFilePath == null) {
      throw new RuntimeException("Cannot find gateway-service config file in the classpath");
    }

    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    appConfig = ConfigFactory.load(fileConfig);
  }

  @Test
  public void testServiceImplInitialization() {
    new GatewayServiceImpl(appConfig);
  }
}
