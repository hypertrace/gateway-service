package org.hypertrace.gateway.service.entity.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

public class LogConfigTest {

  @Test
  public void test_getQueryThresholdInMillis_fromValidFile() {
    File configFile = new File(Thread.currentThread()
        .getContextClassLoader()
        .getResource("configs/gateway-service/application.conf")
        .getPath());
    Config appConfig = ConfigFactory.parseFile(configFile);
    LogConfig logConfig = new LogConfig(appConfig);
    assertEquals(1500L, logConfig.getQueryThresholdInMillis());
  }

  @Test
  public void test_getQueryThresholdInMillis_missingFromConfig_shouldGetDefault() {
    Config appConfig = ConfigFactory.parseMap(new HashMap<>());
    LogConfig logConfig = new LogConfig(appConfig);
    assertEquals(2500L, logConfig.getQueryThresholdInMillis());
  }
}
