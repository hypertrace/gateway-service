package org.hypertrace.gateway.service.common.validators;

import java.util.Formatter;
import org.slf4j.Logger;

public abstract class Validator {
  protected void checkArgument(boolean condition, String format, Object... args) {
    if (!condition) {
      String errorMessage = (new Formatter()).format(format, args).toString();
      getLogger().error(errorMessage);
      throw new IllegalArgumentException(errorMessage);
    }
  }

  protected void checkState(boolean condition, String format, Object... args) {
    if (!condition) {
      String errorMessage = (new Formatter()).format(format, args).toString();
      getLogger().error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }
  }

  protected abstract Logger getLogger();
}
