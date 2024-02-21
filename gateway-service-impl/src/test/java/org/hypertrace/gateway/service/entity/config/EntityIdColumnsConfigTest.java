package org.hypertrace.gateway.service.entity.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class EntityIdColumnsConfigTest {

  @Test
  public void testOnlyEntityIdColumnsConfigPresent() {
    String appConfigStr =
        "entity.idcolumn.config = [\n"
            + "  {\n"
            + "    scope = FOO\n"
            + "    key = id\n"
            + "  },\n"
            + "  {\n"
            + "    scope = BAR\n"
            + "    key = id2\n"
            + "  }\n"
            + "]";
    Config appConfig = ConfigFactory.parseString(appConfigStr);
    EntityIdColumnsConfig entityIdColumnsConfig = EntityIdColumnsConfig.fromConfig(appConfig);

    assertEquals(Optional.of("id"), entityIdColumnsConfig.getIdKey("FOO"));
    assertEquals(Optional.of("id2"), entityIdColumnsConfig.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("BAZ"));
  }

  @Test
  public void testOnlyDomainConfigPresent() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = FOO\n"
            + "    key = id\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = FOO\n"
            + "        key = id\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  {\n"
            + "    scope = BAR\n"
            + "    key = id2\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = BAR\n"
            + "        key = id2\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "]";
    Config appConfig = ConfigFactory.parseString(domainObjectConfig);
    EntityIdColumnsConfig entityIdColumnsConfig = EntityIdColumnsConfig.fromConfig(appConfig);

    assertEquals(Optional.of("id"), entityIdColumnsConfig.getIdKey("FOO"));
    assertEquals(Optional.of("id2"), entityIdColumnsConfig.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("BAZ"));
  }

  @Test
  public void testBothDomainAndEntityIdColumnConfigPresent() {
    String appConfigStr =
        "entity.idcolumn.config = [\n"
            + "  {\n"
            + "    scope = FOO\n"
            + "    key = id\n"
            + "  },\n"
            + "  {\n"
            + "    scope = BAR\n"
            + "    key = id2\n"
            + "  }\n"
            + "]\n"
            + "domainobject.config = [\n"
            + "  {\n"
            + "    scope = FOO\n"
            + "    key = id3\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = FOO\n"
            + "        key = id3\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  {\n"
            + "    scope = BAR\n"
            + "    key = id4\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = BAR\n"
            + "        key = id4\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "]";
    Config appConfig = ConfigFactory.parseString(appConfigStr);
    EntityIdColumnsConfig entityIdColumnsConfig = EntityIdColumnsConfig.fromConfig(appConfig);

    assertEquals(Optional.of("id"), entityIdColumnsConfig.getIdKey("FOO"));
    assertEquals(Optional.of("id2"), entityIdColumnsConfig.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("BAZ"));
  }

  @Test
  public void testNonEmptyDomainAndEmptyEntityIdColumnConfig() {
    String appConfigStr =
        "entity.idcolumn.config = [\n"
            + "]\n"
            + "domainobject.config = [\n"
            + "  {\n"
            + "    scope = FOO\n"
            + "    key = id3\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = FOO\n"
            + "        key = id3\n"
            + "      }\n"
            + "    ]\n"
            + "  },\n"
            + "  {\n"
            + "    scope = BAR\n"
            + "    key = id4\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = BAR\n"
            + "        key = id4\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "]";
    Config appConfig = ConfigFactory.parseString(appConfigStr);
    EntityIdColumnsConfig entityIdColumnsConfig = EntityIdColumnsConfig.fromConfig(appConfig);

    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("FOO"));
    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("BAZ"));
  }

  @Test
  public void testNoDomainNorEntityIdColumnConfig() {
    String appConfigStr = "";
    Config appConfig = ConfigFactory.parseString(appConfigStr);
    EntityIdColumnsConfig entityIdColumnsConfig = EntityIdColumnsConfig.fromConfig(appConfig);

    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("FOO"));
    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfig.getIdKey("BAZ"));
  }
}
