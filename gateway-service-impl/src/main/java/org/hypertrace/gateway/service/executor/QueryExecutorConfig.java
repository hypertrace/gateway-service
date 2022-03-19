package org.hypertrace.gateway.service.executor;

import com.typesafe.config.Config;

public class QueryExecutorConfig {
  private static final String CONFIG_PATH = "query.executor.config";
  private static final String THREAD_COUNT_PATH = "thread.count";

  private final int threadCount;

  public static QueryExecutorConfig from(Config serviceConfig) {
    return new QueryExecutorConfig(serviceConfig.getConfig(CONFIG_PATH).getInt(THREAD_COUNT_PATH));
  }

  QueryExecutorConfig(int threadCount) {
    this.threadCount = threadCount;
  }

  public int getThreadCount() {
    return threadCount;
  }
}
