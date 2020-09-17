package org.hypertrace.gateway.service.common;

import java.util.List;
import java.util.stream.Collectors;
import org.hypertrace.core.query.service.api.ColumnIdentifier;
import org.hypertrace.core.query.service.api.ColumnMetadata;
import org.hypertrace.core.query.service.api.Expression;
import org.hypertrace.core.query.service.api.Filter;
import org.hypertrace.core.query.service.api.Function;
import org.hypertrace.core.query.service.api.LiteralConstant;
import org.hypertrace.core.query.service.api.Operator;
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

  public static Expression createQsAggregationExpression(String functionName, String columnName, String alias) {
    return Expression.newBuilder()
        .setFunction(Function.newBuilder()
            .setFunctionName(functionName)
            .setAlias(alias)
            .addArguments(createQsColumnExpression(columnName))
        )
        .build();
  }

  public static Expression createQsAggregationExpression(String functionName, String columnName) {
    return Expression.newBuilder()
        .setFunction(Function.newBuilder()
            .setFunctionName(functionName)
            .addArguments(createQsColumnExpression(columnName))
        )
        .build();
  }

  public static Expression createQsColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(
            ColumnIdentifier.newBuilder()
                .setColumnName(columnName)
        )
        .build();
  }

  public static Expression createQsColumnExpression(String columnName, String alias) {
    return Expression.newBuilder()
        .setColumnIdentifier(
            ColumnIdentifier.newBuilder()
                .setColumnName(columnName)
                .setAlias(alias)
        )
        .build();
  }

  public static Expression createQsStringLiteralExpression(String val) {
    return Expression.newBuilder()
        .setLiteral(
            LiteralConstant.newBuilder()
                .setValue(
                    Value.newBuilder()
                        .setString(val)
                        .setValueType(ValueType.STRING)
                )
        )
        .build();
  }

  public static Filter createQsFilter(Expression lhs, Operator operator, Expression rhs) {
    return Filter.newBuilder()
        .setLhs(lhs)
        .setOperator(operator)
        .setRhs(rhs)
        .build();
  }

  public static org.hypertrace.core.query.service.api.OrderByExpression createQsOrderBy(Expression expression, SortOrder sortOrder) {
    return org.hypertrace.core.query.service.api.OrderByExpression.newBuilder()
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
        .addChildFilter(
            Filter.newBuilder()
                .addChildFilter(
                    createQsFilter(
                        createQsColumnExpression(timestampColumnName),
                        Operator.GE,
                        createQsStringLiteralExpression(Long.toString(startTime))
                    )
                )
                .addChildFilter(
                    createQsFilter(
                        createQsColumnExpression(timestampColumnName),
                        Operator.LT,
                        createQsStringLiteralExpression(Long.toString(endTime))
                    )
                )
        )
        .addChildFilter(
            requestFilter
        )
        .addChildFilter(
            Filter.newBuilder()
                .addChildFilter(
                    createQsFilter(
                        createQsColumnExpression(entityIdColumnName),
                        Operator.NEQ,
                        createQsStringLiteralExpression("null")
                    )
                )
        ).build();
  }

  public static Filter createQsDefaultRequestFilter(String timestampColumnName,
                                                    String entityIdColumnName,
                                                    long startTime,
                                                    long endTime) {
    return Filter.newBuilder()
        .addChildFilter(
            createQsFilter(
                createQsColumnExpression(timestampColumnName),
                Operator.GE,
                createQsStringLiteralExpression(Long.toString(startTime))
            )
        )
        .addChildFilter(
            createQsFilter(
                createQsColumnExpression(timestampColumnName),
                Operator.LT,
                createQsStringLiteralExpression(Long.toString(endTime))
            )
        )
        .addChildFilter(
            Filter.newBuilder()
                .addChildFilter(
                    createQsFilter(
                        createQsColumnExpression(entityIdColumnName),
                        Operator.NEQ,
                        createQsStringLiteralExpression("null")
                    )
                )
        ).build();
  }
}
