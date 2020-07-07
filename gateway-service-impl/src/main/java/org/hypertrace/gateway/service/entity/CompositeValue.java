package org.hypertrace.gateway.service.entity;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Parser;
import com.google.protobuf.util.JsonFormat.Printer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.hypertrace.gateway.service.v1.common.Value;

/**
 * A combination of one or more {@link Value}s
 */
public class CompositeValue {

  private static final String DELIMITER = ":";
  private static final Parser PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final Printer PRINTER = JsonFormat.printer();

  private List<Value> values;

  private CompositeValue(List<Value> values) {
    Preconditions.checkArgument(values != null);
    this.values = values;
  }

  public static CompositeValue of(Value... values) {
    return new CompositeValue(Arrays.asList(values));
  }

  public static CompositeValue from(String string) {
    return new CompositeValue(
        Arrays.stream(new String(Base64.getDecoder().decode(string)).split(DELIMITER))
            .map(Base64.getDecoder()::decode)
            .map(String::new)
            .map(CompositeValue::fromJsonString)
            .collect(Collectors.toList()));
  }

  private static Value fromJsonString(String str) {
    try {
      Value.Builder builder = Value.newBuilder();
      PARSER.merge(str, builder);
      return builder.build();
    } catch (InvalidProtocolBufferException ex) {
      throw new RuntimeException(String.format("Error getting Value from str:%s", str));
    }
  }

  public List<Value> getValues() {
    return values;
  }

  @Override
  public String toString() {
    return Base64.getEncoder()
        .encodeToString(
            values.stream()
                .map(this::toJsonString)
                .map(s -> Base64.getEncoder().encodeToString(s.getBytes()))
                .collect(Collectors.joining(DELIMITER))
                .getBytes());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompositeValue that = (CompositeValue) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  public int size() {
    return values.size();
  }

  public Value get(int i) {
    return values.get(i);
  }

  private String toJsonString(Value value) {
    try {
      return PRINTER.print(value);
    } catch (InvalidProtocolBufferException ex) {
      throw new RuntimeException(
          String.format("Error getting string representation of value:%s", value));
    }
  }
}
