package org.hypertrace.gateway.service.entity.query;

import java.util.Collections;
import java.util.Set;
import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

public class JoinNode implements QueryNode {
  private final Set<String> attrSelectionSources;
  private final Set<String> aggMetricSelectionSources;

  private final QueryNode childNode;

  private JoinNode(
      QueryNode childNode,
      Set<String> attrSelectionSources,
      Set<String> aggMetricSelectionSources) {
    this.attrSelectionSources = attrSelectionSources;
    this.aggMetricSelectionSources = aggMetricSelectionSources;
    this.childNode = childNode;
  }

  public Set<String> getAttrSelectionSources() {
    return attrSelectionSources;
  }

  public Set<String> getAggMetricSelectionSources() {
    return aggMetricSelectionSources;
  }

  public QueryNode getChildNode() {
    return childNode;
  }

  @Override
  public <R> R acceptVisitor(Visitor<R> v) {
    return v.visit(this);
  }

  @Override
  public String toString() {
    return "SelectionNode{"
        + "attrSelectionSources="
        + attrSelectionSources
        + ", aggMetricSelectionSources="
        + aggMetricSelectionSources
        + ", childNode="
        + childNode
        + '}';
  }

  public static class Builder {
    private final QueryNode childNode;
    private Set<String> attrSelectionSources = Collections.emptySet();
    private Set<String> aggMetricSelectionSources = Collections.emptySet();

    public Builder(QueryNode childNode) {
      this.childNode = childNode;
    }

    public JoinNode.Builder setAttrSelectionSources(Set<String> attrSelectionSources) {
      this.attrSelectionSources = attrSelectionSources;
      return this;
    }

    public JoinNode.Builder setAggMetricSelectionSources(Set<String> aggMetricSelectionSources) {
      this.aggMetricSelectionSources = aggMetricSelectionSources;
      return this;
    }

    public JoinNode build() {
      return new JoinNode(childNode, attrSelectionSources, aggMetricSelectionSources);
    }
  }
}
