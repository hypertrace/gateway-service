package org.hypertrace.gateway.service.entity.config;

import java.util.List;

/** Configuration that holds the incoming/outgoing attribute ids for an Entity Type */
public class InteractionConfig {
  private final String type;
  private final List<String> callerSideAttributeIds;
  private final List<String> calleeSideAttributeIds;

  public InteractionConfig(
      String type, List<String> callerSideAttributeIds, List<String> calleeSideAttributeIds) {
    this.type = type;
    this.callerSideAttributeIds = callerSideAttributeIds;
    this.calleeSideAttributeIds = calleeSideAttributeIds;
  }

  public String getType() {
    return type;
  }

  public List<String> getCallerSideAttributeIds() {
    return callerSideAttributeIds;
  }

  public List<String> getCalleeSideAttributeIds() {
    return calleeSideAttributeIds;
  }
}
