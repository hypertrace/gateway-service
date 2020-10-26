package org.hypertrace.gateway.service.common.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ScopeFilterConfigsTest {
  @Test
  public void testScopeFilterConfig() {
    String scopeFiltersConfig =
        "scopeFiltersConfig = [\n"
            + "  {\n"
            + "    scope = API_TRACE\n"
            + "    filters = [\n"
            + "       {\n"
            + "         scope = API_TRACE\n"
            + "         key = apiBoundaryType\n"
            + "         op = EQ\n"
            + "         value = ENTRY\n"
            + "       },\n"
            + "       {\n"
            + "         scope = API_TRACE\n"
            + "         key = apiId\n"
            + "         op = NEQ\n"
            + "         value = \"null\"\n"
            + "       },\n"
            + "    ]\n"
            + "  }\n"
            + "]";
    Config config = ConfigFactory.parseString(scopeFiltersConfig);
    ScopeFilterConfigs scopeFilterConfigs = new ScopeFilterConfigs(config);

    Filter filter1 =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.id"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-api-trace-id")))
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.some_col"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-col-value")))
            .build();
    Filter filter2 = Filter.newBuilder().build();
    Filter filter3 =
        Filter.newBuilder()
            .setOperator(Operator.OR)
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.id"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-api-trace-id")))
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.some_col"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-col-value")))
            .build();

    RequestContext requestContext = new RequestContext("some-tenant-id", Map.of());
    AttributeMetadataProvider attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    when(attributeMetadataProvider.getAttributeMetadata(
            requestContext, AttributeScope.API_TRACE.name(), "apiBoundaryType"))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiBoundaryType")
                    .setId("API_TRACE.apiBoundaryType")
                    .build()));
    when(attributeMetadataProvider.getAttributeMetadata(
            requestContext, AttributeScope.API_TRACE.name(), "apiId"))
        .thenReturn(
            Optional.of(
                AttributeMetadata.newBuilder()
                    .setScopeString(AttributeScope.API_TRACE.name())
                    .setKey("apiId")
                    .setId("API_TRACE.apiId")
                    .build()));

    Filter.Builder expectedExtraFilter =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.apiBoundaryType"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("ENTRY")))
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.apiId"))
                    .setOperator(Operator.NEQ)
                    .setRhs(createLiteralStringExpression("null")));

    Assertions.assertEquals(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter1)
            .addChildFilter(expectedExtraFilter)
            .build(),
        scopeFilterConfigs.createScopeFilter(
            AttributeScope.API_TRACE.name(), filter1, attributeMetadataProvider, requestContext));

    Assertions.assertEquals(
        expectedExtraFilter.build(),
        scopeFilterConfigs.createScopeFilter(
            AttributeScope.API_TRACE.name(), filter2, attributeMetadataProvider, requestContext));
    Assertions.assertEquals(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(filter3)
            .addChildFilter(expectedExtraFilter)
            .build(),
        scopeFilterConfigs.createScopeFilter(
            AttributeScope.API_TRACE.name(), filter3, attributeMetadataProvider, requestContext));

    // Test for scope without extra filters in the config
    Assertions.assertEquals(
        filter1,
        scopeFilterConfigs.createScopeFilter(
            AttributeScope.TRACE.name(), filter1, attributeMetadataProvider, requestContext));
  }

  @Test
  public void testScopeFilterConfigWithBogusOperator() {
    String scopeFiltersConfig =
        "scopeFiltersConfig = [\n"
            + "  {\n"
            + "    scope = API_TRACE\n"
            + "    filters = [\n"
            + "       {\n"
            + "         key = apiBoundaryType\n"
            + "         op = BOGUSOP\n"
            + "         value = ENTRY\n"
            + "       },\n"
            + "       {\n"
            + "         key = apiId\n"
            + "         op = NEQ\n"
            + "         value = \"null\"\n"
            + "       },\n"
            + "    ]\n"
            + "  }\n"
            + "]";
    Config config = ConfigFactory.parseString(scopeFiltersConfig);
    ScopeFilterConfigs scopeFilterConfigs = new ScopeFilterConfigs(config);

    Filter filter1 =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.id"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-api-trace-id")))
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.some_col"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-col-value")))
            .build();

    RequestContext requestContext = new RequestContext("some-tenant-id", Map.of());
    AttributeMetadataProvider attributeMetadataProvider = mock(AttributeMetadataProvider.class);

    // No change to filters since it will fail to parse the config because of the BOGUSOP unknown
    // operator.
    Assertions.assertEquals(
        filter1,
        scopeFilterConfigs.createScopeFilter(
            AttributeScope.API_TRACE.name(), filter1, attributeMetadataProvider, requestContext));
  }

  @Test
  public void testEmptyScopeFilterConfig() {
    String scopeFiltersConfig = "";
    Config config = ConfigFactory.parseString(scopeFiltersConfig);
    ScopeFilterConfigs scopeFilterConfigs = new ScopeFilterConfigs(config);

    Filter filter1 =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.id"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-api-trace-id")))
            .addChildFilter(
                Filter.newBuilder()
                    .setLhs(createColumnExpression("API_TRACE.some_col"))
                    .setOperator(Operator.EQ)
                    .setRhs(createLiteralStringExpression("some-col-value")))
            .build();

    RequestContext requestContext = new RequestContext("some-tenant-id", Map.of());
    AttributeMetadataProvider attributeMetadataProvider = mock(AttributeMetadataProvider.class);

    Assertions.assertEquals(
        filter1,
        scopeFilterConfigs.createScopeFilter(
            AttributeScope.API_TRACE.name(), filter1, attributeMetadataProvider, requestContext));

    Assertions.assertEquals(
        filter1,
        scopeFilterConfigs.createScopeFilter(
            AttributeScope.TRACE.name(), filter1, attributeMetadataProvider, requestContext));
  }

  private Expression.Builder createLiteralStringExpression(String str) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(Value.newBuilder().setValueType(ValueType.STRING).setString(str)));
  }

  private Expression.Builder createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName));
  }
}
