package org.hypertrace.gateway.service.trace;

public enum TraceScope {
  SCOPE_UNDEFINED,
  API_TRACE,
  BACKEND_TRACE,
  // transacation trace
  TRACE,
}
