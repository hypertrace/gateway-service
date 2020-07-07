package org.hypertrace.gateway.service.entity.query.visitor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.gateway.service.common.datafetcher.EntityFetcherResponse;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.entity.query.ExecutionContext;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Expression;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.common.Operator;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.hypertrace.gateway.service.v1.entity.Entity;
import org.hypertrace.gateway.service.v1.entity.Entity.Builder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExecutionVisitorTest {

  private EntityFetcherResponse result1 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id1"),
                  Entity.newBuilder().putAttribute("key11", getStringValue("value11")),
              EntityKey.of("id2"),
                  Entity.newBuilder().putAttribute("key12", getStringValue("value12")),
              EntityKey.of("id3"),
                  Entity.newBuilder().putAttribute("key13", getStringValue("value13"))));
  private EntityFetcherResponse result2 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id1"),
                  Entity.newBuilder().putAttribute("key21", getStringValue("value21")),
              EntityKey.of("id2"),
                  Entity.newBuilder().putAttribute("key22", getStringValue("value22"))));
  private EntityFetcherResponse result3 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id1"),
                  Entity.newBuilder().putAttribute("key31", getStringValue("value31")),
              EntityKey.of("id3"),
                  Entity.newBuilder().putAttribute("key33", getStringValue("value33"))));
  private EntityFetcherResponse result4 =
      new EntityFetcherResponse(
          Map.of(
              EntityKey.of("id4"),
              Entity.newBuilder().putAttribute("key41", getStringValue("value41"))));

  @Test
  public void testIntersect() {
    {
      Map<EntityKey, Builder> finalResult =
          ExecutionVisitor.intersect(Arrays.asList(result1, result2, result3))
              .getEntityKeyBuilderMap();
      Assertions.assertEquals(1, finalResult.size());
      Entity.Builder builder = finalResult.get(EntityKey.of("id1"));
      Assertions.assertNotNull(builder);
      Assertions.assertEquals("value11", builder.getAttributeMap().get("key11").getString());
      Assertions.assertEquals("value21", builder.getAttributeMap().get("key21").getString());
      Assertions.assertEquals("value31", builder.getAttributeMap().get("key31").getString());
    }
    {
      Map<EntityKey, Builder> finalResult =
          ExecutionVisitor.intersect(Arrays.asList(result1, result2, result4))
              .getEntityKeyBuilderMap();
      Assertions.assertTrue(finalResult.isEmpty());
    }
  }

  @Test
  public void testUnion() {
    {
      Map<EntityKey, Builder> finalResult =
          ExecutionVisitor.union(Arrays.asList(result1, result4)).getEntityKeyBuilderMap();
      Assertions.assertEquals(4, finalResult.size());
      Assertions.assertTrue(
          finalResult
              .keySet()
              .containsAll(
                  Stream.of("id1", "id2", "id3", "id4")
                      .map(EntityKey::of)
                      .collect(Collectors.toList())));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id1")),
          finalResult.get(EntityKey.of("id1")));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id2")),
          finalResult.get(EntityKey.of("id2")));
      Assertions.assertEquals(
          result1.getEntityKeyBuilderMap().get(EntityKey.of("id3")),
          finalResult.get(EntityKey.of("id3")));
      Assertions.assertEquals(
          result4.getEntityKeyBuilderMap().get(EntityKey.of("id4")),
          finalResult.get(EntityKey.of("id4")));
    }
  }

  @Test
  public void testConstructFilterFromChildNodesResultEmptyResults() {
    // Empty results.
    EntityFetcherResponse result = new EntityFetcherResponse();
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getEntityIdExpressions())
        .thenReturn(
            List.of(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName("API.id")
                            .setAlias("entityId0")
                            .build())
                    .build()));

    ExecutionVisitor executionVisitor = new ExecutionVisitor(executionContext);
    Filter filter = executionVisitor.constructFilterFromChildNodesResult(result);

    Assertions.assertEquals(Filter.getDefaultInstance(), filter);
  }

  @Test
  public void testConstructFilterFromChildNodesNonEmptyResultsSingleEntityIdExpression() {
    EntityFetcherResponse result =
        new EntityFetcherResponse(
            Map.of(
                EntityKey.of("api0"), Entity.newBuilder(),
                EntityKey.of("api1"), Entity.newBuilder(),
                EntityKey.of("api2"), Entity.newBuilder()));
    Expression entityIdExpression =
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("API.id").setAlias("entityId0").build())
            .build();
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getEntityIdExpressions()).thenReturn(List.of(entityIdExpression));

    ExecutionVisitor executionVisitor = new ExecutionVisitor(executionContext);
    Filter filter = executionVisitor.constructFilterFromChildNodesResult(result);

    Assertions.assertEquals(0, filter.getChildFilterCount());
    Assertions.assertEquals(entityIdExpression, filter.getLhs());
    Assertions.assertEquals(Operator.IN, filter.getOperator());
    Assertions.assertEquals(
        Set.of("api0", "api1", "api2"),
        new HashSet<>(filter.getRhs().getLiteral().getValue().getStringArrayList()));
    Assertions.assertEquals(
        ValueType.STRING_ARRAY, filter.getRhs().getLiteral().getValue().getValueType());
  }

  @Test
  public void testConstructFilterFromChildNodesNonEmptyResultsMultipleEntityIdExpressions() {
    EntityFetcherResponse result =
        new EntityFetcherResponse(
            Map.of(
                EntityKey.of("api0", "v10"), Entity.newBuilder(),
                EntityKey.of("api1", "v11"), Entity.newBuilder(),
                EntityKey.of("api2", "v12"), Entity.newBuilder()));
    Expression entityIdExpression0 =
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder().setColumnName("API.id").setAlias("entityId0").build())
            .build();
    Expression entityIdExpression1 =
        Expression.newBuilder()
            .setColumnIdentifier(
                ColumnIdentifier.newBuilder()
                    .setColumnName("API.someCol")
                    .setAlias("entityId1")
                    .build())
            .build();
    ExecutionContext executionContext = mock(ExecutionContext.class);
    when(executionContext.getEntityIdExpressions())
        .thenReturn(List.of(entityIdExpression0, entityIdExpression1));

    ExecutionVisitor executionVisitor = new ExecutionVisitor(executionContext);
    Filter filter = executionVisitor.constructFilterFromChildNodesResult(result);

    Assertions.assertEquals(3, filter.getChildFilterCount());
    Assertions.assertEquals(Operator.OR, filter.getOperator());
    Assertions.assertFalse(filter.hasLhs());
    Assertions.assertFalse(filter.hasRhs());
    Assertions.assertEquals(
        Set.of(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression0)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("api0")
                                                .setValueType(ValueType.STRING)))))
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression1)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("v10")
                                                .setValueType(ValueType.STRING)))))
                .build(),
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression0)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("api1")
                                                .setValueType(ValueType.STRING)))))
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression1)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("v11")
                                                .setValueType(ValueType.STRING)))))
                .build(),
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression0)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("api2")
                                                .setValueType(ValueType.STRING)))))
                .addChildFilter(
                    Filter.newBuilder()
                        .setLhs(entityIdExpression1)
                        .setOperator(Operator.EQ)
                        .setRhs(
                            Expression.newBuilder()
                                .setLiteral(
                                    LiteralConstant.newBuilder()
                                        .setValue(
                                            Value.newBuilder()
                                                .setString("v12")
                                                .setValueType(ValueType.STRING)))))
                .build()),
        new HashSet<>(filter.getChildFilterList()));
  }

  private Value getStringValue(String value) {
    return Value.newBuilder().setString(value).setValueType(ValueType.STRING).build();
  }
}
