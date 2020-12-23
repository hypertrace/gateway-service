package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.util.QueryRequestUtil;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.converters.QueryAndGatewayDtoConverter;
import org.hypertrace.gateway.service.common.util.MetricAggregationFunctionUtil;
import org.hypertrace.gateway.service.entity.EntityKey;
import org.hypertrace.gateway.service.v1.baseline.Baseline;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntitiesResponse;
import org.hypertrace.gateway.service.v1.baseline.BaselineEntity;
import org.hypertrace.gateway.service.v1.baseline.BaselineInterval;
import org.hypertrace.gateway.service.v1.baseline.BaselineMetricSeries;
import org.hypertrace.gateway.service.v1.baseline.BaselineTimeAggregation;
import org.hypertrace.gateway.service.v1.common.TimeAggregation;
import org.hypertrace.gateway.service.v1.common.Value;
import org.hypertrace.gateway.service.v1.common.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BaselineServiceQueryParser {
  private static final String QUERY_SERVICE_NULL = "null";
  private static final Logger LOG = LoggerFactory.getLogger(BaselineServiceQueryParser.class);
  private final AttributeMetadataProvider attributeMetadataProvider;

  public BaselineServiceQueryParser(
      AttributeMetadataProvider attributeMetadataProvider) {
    this.attributeMetadataProvider = attributeMetadataProvider;
  }

  public QueryRequest getQueryRequest(
      long startTimeInMillis,
      long endTimeInMillis,
      List<String> entityIds,
      String timeColumn,
      List<TimeAggregation> timeAggregationList,
      long periodSecs) {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    timeAggregationList.forEach(
        e ->
            builder.addSelection(
                QueryAndGatewayDtoConverter.convertToQueryExpression(e.getAggregation())));
    Filter.Builder queryFilter =
        constructQueryServiceFilter(startTimeInMillis, endTimeInMillis, entityIds, timeColumn);
    builder.setFilter(queryFilter);
    builder.addAllGroupBy(
        entityIds.stream()
            .map(QueryRequestUtil::createColumnExpression)
            .map(org.hypertrace.core.query.service.api.Expression.Builder::build)
            .collect(Collectors.toList()));

    builder.addGroupBy(
        org.hypertrace.core.query.service.api.Expression.newBuilder()
            .setFunction(QueryRequestUtil.createTimeColumnGroupByFunction(timeColumn, periodSecs)));

    return builder.build();
  }

  private Filter.Builder constructQueryServiceFilter(
      long startTimeInMillis,
      long endTimeInMillis,
      List<String> entityIdAttributes,
      String timeColumn) {
    // adds the Id != "null" filter to remove null entities.
    Filter.Builder filterBuilder =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addAllChildFilter(
                entityIdAttributes.stream()
                    .map(
                        entityIdAttribute ->
                            QueryRequestUtil.createColumnValueFilter(
                                entityIdAttribute, Operator.NEQ, QUERY_SERVICE_NULL))
                    .map(Filter.Builder::build)
                    .collect(Collectors.toList()));

    // Time range is a mandatory filter for query service, hence add it if it's not already present.
    filterBuilder.addChildFilter(
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addChildFilter(createTimeFilter(timeColumn, Operator.GE, startTimeInMillis))
            .addChildFilter(createTimeFilter(timeColumn, Operator.LT, endTimeInMillis)));

    return filterBuilder;
  }

  private static Filter createTimeFilter(String columnName, Operator op, long value) {
    ColumnIdentifier.Builder timeColumn = ColumnIdentifier.newBuilder().setColumnName(columnName);
    org.hypertrace.core.query.service.api.Expression.Builder lhs =
        org.hypertrace.core.query.service.api.Expression.newBuilder()
            .setColumnIdentifier(timeColumn);
    LiteralConstant.Builder constant =
        LiteralConstant.newBuilder()
            .setValue(
                org.hypertrace.core.query.service.api.Value.newBuilder()
                    .setValueType(org.hypertrace.core.query.service.api.ValueType.LONG)
                    .setLong(value));
    org.hypertrace.core.query.service.api.Expression.Builder rhs =
        org.hypertrace.core.query.service.api.Expression.newBuilder().setLiteral(constant);
    return Filter.newBuilder().setLhs(lhs).setOperator(op).setRhs(rhs).build();
  }

  public BaselineEntitiesResponse parseQueryResponse(
      Iterator<ResultSetChunk> resultSetChunkIterator,
      BaselineRequestContext requestContext,
      int idColumnsSize,
      String entityType,
      long periodSecs) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(requestContext, entityType);
    Map<EntityKey, Map<String, BaselineMetricSeries.Builder>> entityMetricSeriesMap =
        new LinkedHashMap<>();
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received chunk: " + chunk.toString());
      }

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        EntityKey entityKey =
            EntityKey.of(
                IntStream.range(0, idColumnsSize)
                    .mapToObj(value -> row.getColumn(value).getString())
                    .toArray(String[]::new));

        Map<String, BaselineMetricSeries.Builder> metricSeriesMap =
            entityMetricSeriesMap.computeIfAbsent(entityKey, k -> new LinkedHashMap<>());

        BaselineInterval.Builder intervalBuilder = BaselineInterval.newBuilder();

        // Second column is the time column
        Value value =
            QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(
                row.getColumn(idColumnsSize));
        if (value.getValueType() == ValueType.STRING) {
          long time = Long.parseLong(value.getString());
          intervalBuilder.setStartTimeMillis(time);
          intervalBuilder.setEndTimeMillis(time + TimeUnit.SECONDS.toMillis(periodSecs));

          for (int i = idColumnsSize + 1;
              i < chunk.getResultSetMetadata().getColumnMetadataCount();
              i++) {
            ColumnMetadata metadata = chunk.getResultSetMetadata().getColumnMetadata(i);
            BaselineTimeAggregation timeAggregation =
                requestContext.getTimeAggregationByAlias(metadata.getColumnName());

            if (timeAggregation == null) {
              LOG.warn("Couldn't find an aggregate for column: {}", metadata.getColumnName());
              continue;
            }

            Value convertedValue =
                QueryAndGatewayDtoConverter.convertToGatewayValueForMetricValue(
                    MetricAggregationFunctionUtil.getValueTypeFromFunction(
                        timeAggregation.getAggregation(), attributeMetadataMap),
                    attributeMetadataMap,
                    metadata,
                    row.getColumn(i));

            BaselineMetricSeries.Builder seriesBuilder =
                metricSeriesMap.computeIfAbsent(
                    metadata.getColumnName(), k -> getMetricSeriesBuilder(timeAggregation));
            seriesBuilder.addBaselineValue(
                BaselineInterval.newBuilder(intervalBuilder.build())
                    .setBaseline(Baseline.newBuilder().setValue(convertedValue).build())
                    .build());
          }
        } else {
          LOG.warn(
              "Was expecting STRING values only but received valueType: {}", value.getValueType());
        }
      }
    }

    List<BaselineEntity> baselineEntities = new ArrayList<>();
    for (Map.Entry<EntityKey, Map<String, BaselineMetricSeries.Builder>> entry :
        entityMetricSeriesMap.entrySet()) {
      BaselineEntity.Builder entityBuilder =
          BaselineEntity.newBuilder()
              .setEntityType(entityType)
              .setId(entry.getKey().toString())
              .putAllBaselineMetricSeries(
                  entry.getValue().entrySet().stream()
                      .collect(
                          Collectors.toMap(
                              Map.Entry::getKey, e -> getSortedMetricSeries(e.getValue()))));
      baselineEntities.add(entityBuilder.build());
    }
    return BaselineEntitiesResponse.newBuilder().addAllBaselineEntity(baselineEntities).build();
  }

  BaselineMetricSeries getSortedMetricSeries(BaselineMetricSeries.Builder builder) {
    List<BaselineInterval> sortedIntervals = new ArrayList<>(builder.getBaselineValueList());
    sortedIntervals.sort(Comparator.comparingLong(BaselineInterval::getStartTimeMillis));
    return BaselineMetricSeries.newBuilder()
        .addAllBaselineValue(sortedIntervals)
        .build();
  }

  private BaselineMetricSeries.Builder getMetricSeriesBuilder(
      BaselineTimeAggregation timeAggregation) {
    BaselineMetricSeries.Builder series = BaselineMetricSeries.newBuilder();
    series.setAggregation(timeAggregation.getAggregation().getFunction().name());
    series.setPeriod(timeAggregation.getPeriod());
    return series;
  }
}
