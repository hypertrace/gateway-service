package org.hypertrace.gateway.service.common.util;

import java.util.List;
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
}
