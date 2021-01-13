package org.hypertrace.gateway.service.baseline;

import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.QueryRequest;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.client.QueryServiceClient;
import org.hypertrace.gateway.service.common.converters.QueryRequestUtil;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringArrayLiteralExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringNullLiteralExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createTimeColumnGroupByExpression;

public class BaselineServiceQueryParser {
  private static final Logger LOG = LoggerFactory.getLogger(BaselineServiceQueryParser.class);
  private final AttributeMetadataProvider attributeMetadataProvider;

  public BaselineServiceQueryParser(AttributeMetadataProvider attributeMetadataProvider) {
    this.attributeMetadataProvider = attributeMetadataProvider;
  }

  public QueryRequest getQueryRequest(
      long startTimeInMillis,
      long endTimeInMillis,
      List<String> entityIds,
      String timeColumn,
      List<TimeAggregation> timeAggregationList,
      long periodSecs,
      List<String> entityIdAttributes) {
    QueryRequest.Builder builder = QueryRequest.newBuilder();
    timeAggregationList.forEach(
        e ->
            builder.addSelection(
                QueryAndGatewayDtoConverter.convertToQueryExpression(e.getAggregation())));

    Filter.Builder queryFilter =
        constructQueryServiceFilter(
            startTimeInMillis, endTimeInMillis, entityIdAttributes, timeColumn, entityIds);
    builder.setFilter(queryFilter);

    builder.addAllGroupBy(
        entityIdAttributes.stream()
            .map(QueryRequestUtil::createColumnExpression)
            .collect(Collectors.toList()));

    builder.addGroupBy(createTimeColumnGroupByExpression(timeColumn, periodSecs));

    builder.setLimit(QueryServiceClient.DEFAULT_QUERY_SERVICE_GROUP_BY_LIMIT);

    return builder.build();
  }

  private Filter.Builder constructQueryServiceFilter(
      long startTimeInMillis,
      long endTimeInMillis,
      List<String> entityIdAttributes,
      String timeColumn,
      List<String> entityIds) {
    // adds the Id != "null" filter to remove null entities.
    Filter.Builder filterBuilder =
        Filter.newBuilder()
            .setOperator(Operator.AND)
            .addAllChildFilter(
                entityIdAttributes.stream()
                    .map(
                        entityIdAttribute ->
                            createFilter(
                                entityIdAttribute,
                                Operator.NEQ,
                                createStringNullLiteralExpression()))
                    .collect(Collectors.toList()));

    filterBuilder.addAllChildFilter(
        entityIdAttributes.stream()
            .map(
                entityIdAttribute ->
                    QueryRequestUtil.createFilter(
                        entityIdAttribute,
                        Operator.IN,
                        createStringArrayLiteralExpression(entityIds)))
            .collect(Collectors.toList()));

    // Time range is a mandatory filter for query service, hence add it if it's not already present.
    filterBuilder.addChildFilter(
        QueryRequestUtil.createBetweenTimesFilter(timeColumn, startTimeInMillis, endTimeInMillis));

    return filterBuilder;
  }

  public BaselineEntitiesResponse parseQueryResponse(
      Iterator<ResultSetChunk> resultSetChunkIterator,
      BaselineRequestContext requestContext,
      int idColumnsSize,
      String entityType,
      long startTime,
      long endTime) {
    Map<String, AttributeMetadata> attributeMetadataMap =
        attributeMetadataProvider.getAttributesMetadata(requestContext, entityType);
    Map<EntityKey, Map<String, BaselineMetricSeries.Builder>> entityMetricSeriesMap =
        new LinkedHashMap<>();
    boolean isFirstChunk = true;
    ResultSetMetadata resultMetadata = null;
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();
      LOG.debug("Received chunk: {} ", chunk);
      if (chunk.getRowCount() < 1) {
        break;
      }
      if (isFirstChunk) {
        resultMetadata = chunk.getResultSetMetadata();
        isFirstChunk = false;
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

        Value value =
            QueryAndGatewayDtoConverter.convertQueryValueToGatewayValue(
                row.getColumn(idColumnsSize));
        if (value.getValueType() == ValueType.STRING) {
          for (int i = idColumnsSize + 1;
              i < resultMetadata.getColumnMetadataCount();
              i++) {
            ColumnMetadata metadata = resultMetadata.getColumnMetadata(i);
            BaselineTimeAggregation timeAggregation =
                requestContext.getTimeAggregationByAlias(metadata.getColumnName());

            if (timeAggregation == null) {
              LOG.warn("Couldn't find an aggregate for column: {}", metadata.getColumnName());
              continue;
            }

            Value convertedValue = MetricAggregationFunctionUtil.getValueFromFunction(startTime, endTime, attributeMetadataMap,
                    row.getColumn(i), metadata, timeAggregation.getAggregation());

            BaselineMetricSeries.Builder seriesBuilder =
                metricSeriesMap.computeIfAbsent(
                    metadata.getColumnName(), k -> BaselineMetricSeries.newBuilder());
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


  private BaselineMetricSeries getSortedMetricSeries(BaselineMetricSeries.Builder builder) {
    List<BaselineInterval> sortedIntervals = new ArrayList<>(builder.getBaselineValueList());
    sortedIntervals.sort(Comparator.comparingLong(BaselineInterval::getStartTimeMillis));
    return BaselineMetricSeries.newBuilder().addAllBaselineValue(sortedIntervals).build();
  }
}
