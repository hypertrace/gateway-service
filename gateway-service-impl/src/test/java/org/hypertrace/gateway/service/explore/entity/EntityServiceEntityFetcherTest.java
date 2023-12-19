package org.hypertrace.gateway.service.explore.entity;

import static org.hypertrace.core.grpcutils.context.RequestContext.forTenantId;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.entity.query.service.client.EntityQueryServiceClient;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.entity.config.EntityIdColumnsConfigs;
import org.hypertrace.gateway.service.explore.ExploreRequestContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.FunctionExpression;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.explore.ExploreRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EntityServiceEntityFetcherTest {
  private AttributeMetadataProvider attributeMetadataProvider;
  private EntityIdColumnsConfigs entityIdColumnsConfigs;
  private EntityQueryServiceClient entityQueryServiceClient;

  @Test
  void testGetEntities() {
    AttributeMetadataProvider attributeMetadataProvider = mock(AttributeMetadataProvider.class);
    EntityIdColumnsConfigs entityIdColumnsConfigs = mock(EntityIdColumnsConfigs.class);
    EntityQueryServiceClient entityQueryServiceClient = mock(EntityQueryServiceClient.class);
    EntityServiceEntityFetcher entityServiceEntityFetcher =
        new EntityServiceEntityFetcher(
            attributeMetadataProvider, entityIdColumnsConfigs, entityQueryServiceClient);

    when(entityQueryServiceClient.execute(any(), any())).thenReturn(mockResults());

    Expression aggregation = createFunctionExpression("API.external");
    ExploreRequest exploreRequest =
        ExploreRequest.newBuilder()
            .setContext("API")
            .setStartTimeMillis(123L)
            .setEndTimeMillis(234L)
            .setFilter(createEqFilter("API.name", "api1"))
            .addSelection(aggregation)
            .addGroupBy(createColumnExpression("API.type"))
            .build();
    ExploreRequestContext exploreRequestContext =
        new ExploreRequestContext(forTenantId("customer1"), exploreRequest);
    exploreRequestContext.mapAliasToFunctionExpression(
        "COUNT_API.external_[]", aggregation.getFunction());

    when(attributeMetadataProvider.getAttributesMetadata(exploreRequestContext, "API"))
        .thenReturn(
            Map.of(
                "API.type",
                AttributeMetadata.newBuilder()
                    .setKey("API.type")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .addSources(AttributeSource.EDS)
                    .build(),
                "API.external",
                AttributeMetadata.newBuilder()
                    .setKey("API.external")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .addSources(AttributeSource.EDS)
                    .build(),
                "API.name",
                AttributeMetadata.newBuilder()
                    .setKey("API.name")
                    .setValueKind(AttributeKind.TYPE_STRING)
                    .addSources(AttributeSource.EDS)
                    .build()));

    List<org.hypertrace.gateway.service.v1.common.Row> rows =
        entityServiceEntityFetcher.getResults(
            exploreRequestContext, exploreRequest, Set.of("api1", "api2"));
    Assertions.assertEquals(rows.size(), 2);
  }

  private Iterator<ResultSetChunk> mockResults() {
    ResultSetChunk chunk =
        ResultSetChunk.newBuilder()
            .setResultSetMetadata(
                ResultSetMetadata.newBuilder()
                    .addColumnMetadata(
                        ColumnMetadata.newBuilder().setColumnName("API.type").build())
                    .addColumnMetadata(
                        ColumnMetadata.newBuilder().setColumnName("COUNT_API.external_[]").build())
                    .build())
            .addRow(
                Row.newBuilder()
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                            .setString("HTTP")
                            .build())
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.LONG)
                            .setLong(12))
                    .build())
            .addRow(
                Row.newBuilder()
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.STRING)
                            .setString("GRPC")
                            .build())
                    .addColumn(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setValueType(org.hypertrace.entity.query.service.v1.ValueType.LONG)
                            .setLong(24))
                    .build())
            .build();

    return List.of(chunk).iterator();
  }

  private Expression createColumnExpression(String column) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(column).build())
        .build();
  }

  private Expression createFunctionExpression(String column) {
    return Expression.newBuilder()
        .setFunction(
            FunctionExpression.newBuilder()
                .setFunction(FunctionType.COUNT)
                .setAlias("COUNT_" + column + "_[]")
                .addArguments(createColumnExpression(column))
                .build())
        .build();
  }

  private Filter createEqFilter(String column, String value) {
    return Filter.newBuilder()
        .setLhs(createColumnExpression(column))
        .setOperator(Operator.EQ)
        .setRhs(
            Expression.newBuilder()
                .setLiteral(
                    LiteralConstant.newBuilder()
                        .setValue(
                            Value.newBuilder()
                                .setString(value)
                                .setValueType(ValueType.STRING)
                                .build())
                        .build())
                .build())
        .build();
  }
}
