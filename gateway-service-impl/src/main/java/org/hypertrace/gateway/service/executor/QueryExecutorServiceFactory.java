package org.hypertrace.gateway.service.executor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QueryExecutorServiceFactory {

  private QueryExecutorServiceFactory() {}

  public static ExecutorService buildExecutorService(QueryExecutorConfig config) {
    return Executors.newFixedThreadPool(
        config.getThreadCount(),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("query-executor-%d").build());
  }
}
