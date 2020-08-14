package org.hypertrace.gateway.service.common;

import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildAggregateExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.buildOrderByExpression;
import static org.hypertrace.gateway.service.common.EntitiesRequestAndResponseUtils.getLiteralExpression;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.hypertrace.gateway.service.v1.common.FunctionType;
import org.hypertrace.gateway.service.v1.common.SortOrder;
import org.hypertrace.gateway.service.v1.entity.EntitiesRequest;
import org.junit.jupiter.api.Test;

public class OrderByPercentileSizeSetterTest {
  @Test
  public void testOrderByPercentileMissingSize() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .addSelection(buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(50))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of())))
            .build();
    EntitiesRequest newRequest = OrderByPercentileSizeSetter.setPercentileSize(entitiesRequest);

    assertEquals(
        EntitiesRequest.newBuilder()
            .addSelection(buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(50))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(50)))))
            .build(),
        newRequest
    );
  }

  @Test
  public void testOrderByPercentileSizePresent() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(78)))))
            .build();
    EntitiesRequest newRequest = OrderByPercentileSizeSetter.setPercentileSize(entitiesRequest);

    assertEquals(
        EntitiesRequest.newBuilder()
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(78)))))
            .build(),
        newRequest
    );
  }

  @Test
  public void testOrderByPercentileMissingSizeMultiplePercentilesInSelection_picksFirstOne() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .addSelection(buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(85))))
            .addSelection(buildAggregateExpression("col2", FunctionType.PERCENTILE,
                "col2_alias", List.of(getLiteralExpression(87))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of())))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col2", FunctionType.PERCENTILE,
                "col2_alias", List.of(getLiteralExpression(95)))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col3", FunctionType.PERCENTILE,
                "col3_alias", List.of())))
            .build();
    EntitiesRequest newRequest = OrderByPercentileSizeSetter.setPercentileSize(entitiesRequest);

    assertEquals(
        EntitiesRequest.newBuilder()
            .addSelection(buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(85))))
            .addSelection(buildAggregateExpression("col2", FunctionType.PERCENTILE,
                "col2_alias", List.of(getLiteralExpression(87))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(85)))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col2", FunctionType.PERCENTILE,
                "col2_alias", List.of(getLiteralExpression(95)))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col3", FunctionType.PERCENTILE,
                "col3_alias", List.of(getLiteralExpression(85)))))
            .build(),
        newRequest
    );
  }

  @Test
  public void testOrderByPercentileMissingSizeNoSelection_setSizeToDefault() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .addSelection(buildAggregateExpression("col1", FunctionType.AVG,
                "col1_alias", List.of(getLiteralExpression(85))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of())))
            .addOrderBy(buildOrderByExpression(SortOrder.ASC, buildAggregateExpression("col2", FunctionType.PERCENTILE,
                "col2_alias", List.of(getLiteralExpression(95)))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col3", FunctionType.PERCENTILE,
                "col3_alias", List.of())))
            .build();
    EntitiesRequest newRequest = OrderByPercentileSizeSetter.setPercentileSize(entitiesRequest);

    assertEquals(
        EntitiesRequest.newBuilder()
            .addSelection(buildAggregateExpression("col1", FunctionType.AVG,
                "col1_alias", List.of(getLiteralExpression(85))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col1", FunctionType.PERCENTILE,
                "col1_alias", List.of(getLiteralExpression(99)))))
            .addOrderBy(buildOrderByExpression(SortOrder.ASC, buildAggregateExpression("col2", FunctionType.PERCENTILE,
                "col2_alias", List.of(getLiteralExpression(95)))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col3", FunctionType.PERCENTILE,
                "col3_alias", List.of(getLiteralExpression(99)))))
            .build(),
        newRequest
    );
  }

  @Test
  public void testNoOrderByPercentile() {
    EntitiesRequest entitiesRequest =
        EntitiesRequest.newBuilder()
            .addSelection(buildAggregateExpression("col1", FunctionType.AVG,
                "col1_alias", List.of(getLiteralExpression(85))))
            .addOrderBy(buildOrderByExpression(SortOrder.DESC, buildAggregateExpression("col2", FunctionType.AVG,
                "col2_alias", List.of(getLiteralExpression(95)))))
            .build();
    EntitiesRequest newRequest = OrderByPercentileSizeSetter.setPercentileSize(entitiesRequest);

    assertEquals(entitiesRequest, newRequest);
  }
}
