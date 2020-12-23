package org.hypertrace.gateway.service.common;

import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createBetweenTimesFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createColumnExpression;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createFilter;
import static org.hypertrace.gateway.service.common.converters.QueryRequestUtil.createStringNullLiteralExpression;

import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
import org.hypertrace.core.query.service.api.OrderByExpression;
import org.hypertrace.core.query.service.api.ResultSetChunk;
import org.hypertrace.core.query.service.api.ResultSetMetadata;
import org.hypertrace.core.query.service.api.Row;
import org.hypertrace.core.query.service.api.SortOrder;
import org.hypertrace.core.query.service.api.Value;
import org.hypertrace.core.query.service.api.ValueType;

public class QueryServiceRequestAndResponseUtils {
  /**
   * resultsTable contains a String 2 dimensional array aka a table. We are converting that table
   * to the format the query-service will send back. query-service is expected to
   * return String types but this can be modified to handle other types.
   * @param columnNames
   * @param resultsTable
   * @return
   */
  public static ResultSetChunk getResultSetChunk(List<String> columnNames, String[][] resultsTable) {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();

    // ColumnMetadata from the keyset
    List<ColumnMetadata> columnMetadataBuilders = columnNames.stream()
        .map((columnName) -> ColumnMetadata.newBuilder().setColumnName(columnName).setValueType(ValueType.STRING).build())
        .collect(Collectors.toList());
    resultSetChunkBuilder.setResultSetMetadata(
        ResultSetMetadata.newBuilder()
            .addAllColumnMetadata(
                columnMetadataBuilders
            )
    );

    // Add the rows.
    for (int i = 0; i < resultsTable.length; i++) {
      Row.Builder rowBuilder = Row.newBuilder();
      for (int j = 0; j < resultsTable[i].length; j++) {
        rowBuilder.addColumn(
            Value.newBuilder().setString(resultsTable[i][j]).setValueType(ValueType.STRING)
        );
      }
      resultSetChunkBuilder.addRow(rowBuilder);
    }

    return resultSetChunkBuilder.build();
  }

  public static Expression createQsAggregationExpression(String functionName, String functionAlias, String columnName, String columnAlias) {
    return Expression.newBuilder()
        .setFunction(Function.newBuilder()
            .setFunctionName(functionName)
            .setAlias(functionAlias)
            .addArguments(createColumnExpression(columnName, columnAlias))
        )
        .build();
  }

  public static Expression createQsAggregationExpression(String functionName, String columnName, String alias) {
    return Expression.newBuilder()
        .setFunction(Function.newBuilder()
            .setFunctionName(functionName)
            .setAlias(alias)
            .addArguments(createColumnExpression(columnName))
        )
        .build();
  }

  public static Expression createQsAggregationExpression(String functionName, String columnName) {
    return Expression.newBuilder()
        .setFunction(Function.newBuilder()
            .setFunctionName(functionName)
            .addArguments(createColumnExpression(columnName))
        )
        .build();
  }

  public static OrderByExpression createQsOrderBy(Expression expression, SortOrder sortOrder) {
    return OrderByExpression.newBuilder()
        .setOrder(sortOrder)
        .setExpression(expression)
        .build();
  }

  public static Filter createQsRequestFilter(String timestampColumnName,
                                             String entityIdColumnName,
                                             long startTime,
                                             long endTime,
                                             Filter requestFilter) {
    return Filter.newBuilder()
        .setOperator(Operator.AND)
        .addChildFilter(
            createFilter(
                createColumnExpression(entityIdColumnName),
                Operator.NEQ,
                createStringNullLiteralExpression()))
        .addChildFilter(createBetweenTimesFilter(timestampColumnName, startTime, endTime))
        .addChildFilter(requestFilter)
        .build();
  }

  public static Filter createQsDefaultRequestFilter(String timestampColumnName,
                                                    String entityIdColumnName,
                                                    long startTime,
                                                    long endTime) {
    return Filter.newBuilder()
        .setOperator(Operator.AND)
        .addChildFilter(
            createFilter(
                createColumnExpression(entityIdColumnName),
                Operator.NEQ,
                createStringNullLiteralExpression()
            )
        )
        .addAllChildFilter(createBetweenTimesFilter(timestampColumnName, startTime, endTime).getChildFilterList())
        .build();
  }
}
