package org.hypertrace.gateway.service.common.config;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample scope filters config
 * scopeFiltersConfig = [
 *   {
 *     scope = API_TRACE
 *     filters = [
 *       {
 *         key = apiBoundaryType
 *         op = EQ
 *         value = ENTRY
 *       },
 *       {
 *         key = apiId
 *         op = NEQ
 *         value = "null" // Quotes are important to distinguish it from null config value.
 *       }
 *     ]
 *   }
 * ]
 */
public class ScopeFilterConfigs {

  private static final Logger LOG = LoggerFactory.getLogger(ScopeFilterConfigs.class);

  private static final String SCOPE_FILTERS_CONFIG = "scopeFiltersConfig";
  private static final String SCOPE_CONFIG = "scope";
  private static final String FILTERS_CONFIG = "filters";
  private static final String FILTER_KEY_CONFIG = "key";
  private static final String FILTER_OPERATOR_CONFIG = "op";
  private static final String FILTER_VALUE_CONFIG = "value";

  private final Map<String, ScopeFilterConfig> scopeFilterConfigMap;

  public ScopeFilterConfigs(Config config) {
    this.scopeFilterConfigMap = new HashMap<>();

    if (!config.hasPath(SCOPE_FILTERS_CONFIG)) {
      return;
    }

    List<? extends Config> configList = config.getConfigList(SCOPE_FILTERS_CONFIG);
    configList.forEach(this::addScopeFilterConfig);
  }

  private void addScopeFilterConfig(Config config) {
    String attributeScope = config.getString(SCOPE_CONFIG);
    List<? extends Config> filterConfigsList = config.getConfigList(FILTERS_CONFIG);
    try {
      List<ScopeFilter> scopeFilters =
          filterConfigsList.stream()
              .map(
                  (filterConfig) ->
                      new ScopeFilter<>(
                          filterConfig.getString(SCOPE_CONFIG),
                          filterConfig.getString(FILTER_KEY_CONFIG),
                          Operator.valueOf(filterConfig.getString(FILTER_OPERATOR_CONFIG)),
                          filterConfig.getString(FILTER_VALUE_CONFIG)))
              .collect(Collectors.toUnmodifiableList());
      ScopeFilterConfig scopeFilterConfig = new ScopeFilterConfig(attributeScope, scopeFilters);
      this.scopeFilterConfigMap.put(attributeScope, scopeFilterConfig);
    } catch (Exception ex) {
      LOG.error("Exception while reading scope filter configs for scope: {}", attributeScope, ex);
    }
  }

  public Filter createScopeFilter(
      String scope,
      Filter originalFilter,
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext) {
    if (!scopeFilterConfigMap.containsKey(scope)) {
      return originalFilter;
    }

    ScopeFilterConfig scopeFilterConfig = scopeFilterConfigMap.get(scope);
    Filter.Builder scopeChildFilter =
        createScopeFilter(scopeFilterConfig, attributeMetadataProvider, requestContext);
    if (originalFilter.equals(Filter.getDefaultInstance())) {
      return scopeChildFilter.build();
    } else {
      return Filter.newBuilder()
          .setOperator(Operator.AND)
          .addChildFilter(originalFilter)
          .addChildFilter(scopeChildFilter)
          .build();
    }
  }

  private Filter.Builder createScopeFilter(
      ScopeFilterConfig scopeFilterConfig,
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext) {
    Filter.Builder filterBuilder = Filter.newBuilder();

    filterBuilder.setOperator(Operator.AND);
    scopeFilterConfig
        .getScopeFilters()
        .forEach(
            (scopeFilter ->
                filterBuilder.addChildFilter(
                    createScopeChildFilter(
                        scopeFilter,
                        attributeMetadataProvider,
                        requestContext))));

    return filterBuilder;
  }

  private Filter.Builder createScopeChildFilter(
      ScopeFilter scopeFilter,
      AttributeMetadataProvider attributeMetadataProvider,
      RequestContext requestContext) {
    AttributeMetadata attributeMetadata =
        attributeMetadataProvider.getAttributeMetadata(
            requestContext, scopeFilter.getScope(), scopeFilter.getKey())
            .orElseThrow();
    return Filter.newBuilder()
        .setLhs(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(attributeMetadata.getId())))
        .setOperator(scopeFilter.getOperator())
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setValueType(ValueType.STRING)
                                .setString(
                                    (String)
                                        scopeFilter
                                            .getFilterValue())
                        )));
  }
}
