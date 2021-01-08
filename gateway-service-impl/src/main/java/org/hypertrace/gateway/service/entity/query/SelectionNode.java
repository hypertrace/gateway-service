package org.hypertrace.gateway.service.entity.query;

import java.util.Collections;
import java.util.Set;
import org.hypertrace.gateway.service.entity.query.visitor.Visitor;

/**
 * A generic selection node that fetches attributes, aggregated metrics and timeseries data from
 * their respective sources.
 *
 * <p>The output of the childNode is used to construct the filter criteria for the selection
 */
public class SelectionNode implements QueryNode {

  private final Set<String> attrSelectionSources;
  private final Set<String> aggMetricSelectionSources;
  private final Set<String> timeSeriesSelectionSources;

  private final QueryNode childNode;

  private SelectionNode(
      QueryNode childNode,
      Set<String> attrSelectionSources,
      Set<String> aggMetricSelectionSources,
      Set<String> timeSeriesSelectionSources) {
    this.attrSelectionSources = attrSelectionSources;
    this.aggMetricSelectionSources = aggMetricSelectionSources;
    this.timeSeriesSelectionSources = timeSeriesSelectionSources;
    this.childNode = childNode;
  }

  public Set<String> getAttrSelectionSources() {
    return attrSelectionSources;
  }

  public Set<String> getAggMetricSelectionSources() {
    return aggMetricSelectionSources;
  }

  public Set<String> getTimeSeriesSelectionSources() {
    return timeSeriesSelectionSources;
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
        + ", timeSeriesSelectionSources="
        + timeSeriesSelectionSources
        + ", childNode="
        + childNode
        + '}';
  }

  public static class Builder {
    private final QueryNode childNode;
    private Set<String> attrSelectionSources = Collections.emptySet();
    private Set<String> aggMetricSelectionSources = Collections.emptySet();
    private Set<String> timeSeriesSelectionSources = Collections.emptySet();

    public Builder(QueryNode childNode) {
      this.childNode = childNode;
    }

    public Builder setAttrSelectionSources(Set<String> attrSelectionSources) {
      this.attrSelectionSources = attrSelectionSources;
      return this;
    }

    public Builder setAggMetricSelectionSources(Set<String> aggMetricSelectionSources) {
      this.aggMetricSelectionSources = aggMetricSelectionSources;
      return this;
    }

    public Builder setTimeSeriesSelectionSources(Set<String> timeSeriesSelectionSources) {
      this.timeSeriesSelectionSources = timeSeriesSelectionSources;
      return this;
    }

    public SelectionNode build() {
      return new SelectionNode(
          childNode, attrSelectionSources, aggMetricSelectionSources, timeSeriesSelectionSources);
    }
  }
}
