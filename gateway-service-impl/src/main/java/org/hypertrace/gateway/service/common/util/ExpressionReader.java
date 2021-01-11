package org.hypertrace.gateway.service.common.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hypertrace.gateway.service.v1.common.Expression;

public class ExpressionReader {
  public static List<Expression> getFunctionExpressions(Stream<Expression> expressionStream) {
    return expressionStream
        .filter(expression -> expression.getValueCase() == Expression.ValueCase.FUNCTION)
        .collect(Collectors.toList());
  }

  public static List<Expression> getColumnExpressions(Stream<Expression> expressionStream) {
    return expressionStream
        .filter(expression -> expression.getValueCase() == Expression.ValueCase.COLUMNIDENTIFIER)
        .collect(Collectors.toList());
  }

  public static Set<String> extractColumns(Expression expression) {
    Set<String> columns = new HashSet<>();
    extractColumns(columns, expression);
    return Collections.unmodifiableSet(columns);
  }

  private static void extractColumns(Set<String> columns, Expression expression) {
    switch (expression.getValueCase()) {
      case COLUMNIDENTIFIER:
        String columnName = expression.getColumnIdentifier().getColumnName();
        columns.add(columnName);
        break;
      case FUNCTION:
        for (Expression exp : expression.getFunction().getArgumentsList()) {
          extractColumns(columns, exp);
        }
        break;
      case ORDERBY:
        extractColumns(columns, expression.getOrderBy().getExpression());
      case LITERAL:
      case VALUE_NOT_SET:
        break;
    }
  }

  /**
   * Given a source to attributes, builds an attribute to sources map.
   * Basically, a reverse map of the map provided as input
   *
   * Example:
   * ("QS" -> API.id, "QS" -> API.name, "EDS" -> API.id) =>
   *
   * ("API.id" -> ["QS", "EDS"], "API.name" -> "QS")
   */
  public static Map<String, Set<String>> buildAttributeToSourcesMap(
      Map<String, Set<String>> sourcesToAttributeMap) {
    Map<String, Set<String>> attributeToSourcesMap = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : sourcesToAttributeMap.entrySet()) {
      String source = entry.getKey();
      for (String attribute : entry.getValue()) {
        attributeToSourcesMap.computeIfAbsent(attribute, k -> new HashSet<>()).add(source);
      }
    }
    return Collections.unmodifiableMap(attributeToSourcesMap);
  }
}
