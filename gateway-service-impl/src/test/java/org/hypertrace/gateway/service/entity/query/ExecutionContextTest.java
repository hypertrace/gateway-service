package org.hypertrace.gateway.service.entity.query;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.EntitiesRequestContext;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.generateEQFilter;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExecutionContextTest {
  private static final String TENANT_ID = "tenant1";

  private static final String API_API_ID_ATTR = "API.apiId";
  private static final String API_NAME_ATTR = "API.name";
  private static final String API_TYPE_ATTR = "API.apiType";
  private static final String API_PATTERN_ATTR = "API.urlPattern";
  private static final String API_START_TIME_ATTR = "API.startTime";
  private static final String API_END_TIME_ATTR = "API.endTime";
  private static final String API_NUM_CALLS_ATTR = "API.numCalls";
  private static final String API_STATE_ATTR = "API.state";
  private static final String API_DISCOVERY_STATE = "API.apiDiscoveryState";
  private static final String API_ID_ATTR = "API.id";

  @Mock private AttributeMetadataProvider attributeMetadataProvider;
  @Mock private EntityIdColumnsConfigs entityIdColumnsConfigs;
  private EntitiesRequestContext entitiesRequestContext;

  @BeforeEach
  public void setup() {
    attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);
    when(attributeMetadataProvider.getAttributesMetadata(
            any(RequestContext.class), eq(AttributeScope.API.name())))
        .thenReturn(attributeSources);

    attributeSources.forEach(
        (attributeId, attribute) ->
            when(attributeMetadataProvider.getAttributeMetadata(
                    any(RequestContext.class),
                    eq(attribute.getScopeString()),
                    eq(attribute.getKey())))
                .thenReturn(Optional.of(attribute)));

    entitiesRequestContext =
        new EntitiesRequestContext(TENANT_ID, 0, 100, "API", "API.startTime", new HashMap<>());
  }

  @Test
  public void testFilterExpressionMaps() {
    Filter filter =
        Filter.newBuilder()
            .addChildFilter(generateEQFilter(API_NAME_ATTR, "api1")) // EDS
            .addChildFilter(generateEQFilter(API_NUM_CALLS_ATTR, "20")) // QS
            .addChildFilter(generateEQFilter(API_DISCOVERY_STATE, "DISCOVERED")) // EDS and QS
            .build();

    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder().setEntityType("API").setFilter(filter).build();
    ExecutionContext executionContext =
        ExecutionContext.from(
            attributeMetadataProvider,
            entityIdColumnsConfigs,
            entitiesRequest,
            entitiesRequestContext);

    Map<String, List<Expression>> sourceToFilterExpressionMap =
        executionContext.getSourceToFilterExpressionMap();
    Map<String, Set<String>> sourceToFilterAttributeMap =
        executionContext.getSourceToFilterAttributeMap();

    assertEquals(2, sourceToFilterExpressionMap.size());
    assertEquals(2, sourceToFilterAttributeMap.size());

    assertTrue(sourceToFilterExpressionMap.containsKey("QS"));
    assertEquals(2, sourceToFilterExpressionMap.get("QS").size());
    assertEquals(
        Set.of(API_NUM_CALLS_ATTR, API_DISCOVERY_STATE), sourceToFilterAttributeMap.get("QS"));

    assertTrue(sourceToFilterExpressionMap.containsKey("EDS"));
    assertEquals(2, sourceToFilterExpressionMap.get("EDS").size());
    assertEquals(Set.of(API_NAME_ATTR, API_DISCOVERY_STATE), sourceToFilterAttributeMap.get("EDS"));

    Map<String, Set<String>> filterAttributeToSourcesMap =
        executionContext.getFilterAttributeToSourceMap();
    assertEquals(3, filterAttributeToSourcesMap.size());
    assertEquals(Set.of("EDS"), filterAttributeToSourcesMap.get(API_NAME_ATTR));
    assertEquals(Set.of("QS"), filterAttributeToSourcesMap.get(API_NUM_CALLS_ATTR));
    assertEquals(Set.of("QS", "EDS"), filterAttributeToSourcesMap.get(API_DISCOVERY_STATE));
  }

  private static final Map<String, AttributeMetadata> attributeSources =
      new HashMap<>() {
        {
          put(
              API_API_ID_ATTR,
              buildAttributeMetadataForSources(
                  API_API_ID_ATTR,
                  AttributeScope.API.name(),
                  "apiId",
                  List.of(AttributeSource.EDS)));
          put(
              API_PATTERN_ATTR,
              buildAttributeMetadataForSources(
                  API_PATTERN_ATTR,
                  AttributeScope.API.name(),
                  "urlPattern",
                  List.of(AttributeSource.EDS)));
          put(
              API_NAME_ATTR,
              buildAttributeMetadataForSources(
                  API_NAME_ATTR, AttributeScope.API.name(), "name", List.of(AttributeSource.EDS)));
          put(
              API_TYPE_ATTR,
              buildAttributeMetadataForSources(
                  API_TYPE_ATTR,
                  AttributeScope.API.name(),
                  "apiType",
                  List.of(AttributeSource.EDS)));
          put(
              API_START_TIME_ATTR,
              buildAttributeMetadataForSources(
                  API_START_TIME_ATTR,
                  AttributeScope.API.name(),
                  "startTime",
                  List.of(AttributeSource.QS)));
          put(
              API_END_TIME_ATTR,
              buildAttributeMetadataForSources(
                  API_END_TIME_ATTR,
                  AttributeScope.API.name(),
                  "endTime",
                  List.of(AttributeSource.QS)));
          put(
              API_NUM_CALLS_ATTR,
              buildAttributeMetadataForSources(
                  API_NUM_CALLS_ATTR,
                  AttributeScope.API.name(),
                  "numCalls",
                  List.of(AttributeSource.QS)));
          put(
              API_STATE_ATTR,
              buildAttributeMetadataForSources(
                  API_STATE_ATTR, AttributeScope.API.name(), "state", List.of(AttributeSource.QS)));
          put(
              API_DISCOVERY_STATE,
              buildAttributeMetadataForSources(
                  API_DISCOVERY_STATE,
                  AttributeScope.API.name(),
                  "apiDiscoveryState",
                  List.of(AttributeSource.EDS, AttributeSource.QS)));
          put(
              API_ID_ATTR,
              buildAttributeMetadataForSources(
                  API_ID_ATTR,
                  AttributeScope.API.name(),
                  "id",
                  List.of(AttributeSource.EDS, AttributeSource.QS)));
        }
      };

  private static AttributeMetadata buildAttributeMetadataForSources(
      String attributeId, String scope, String key, List<AttributeSource> sources) {
    return AttributeMetadata.newBuilder()
        .setId(attributeId)
        .setScopeString(scope)
        .setKey(key)
        .addAllSources(sources)
        .build();
  }
}
