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
    EntityIdColumnsConfigs entityIdColumnsConfigs = EntityIdColumnsConfigs.fromConfig(appConfig);

    assertEquals(Optional.of("id"), entityIdColumnsConfigs.getIdKey("FOO"));
    assertEquals(Optional.of("id2"), entityIdColumnsConfigs.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("BAZ"));
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
    EntityIdColumnsConfigs entityIdColumnsConfigs = EntityIdColumnsConfigs.fromConfig(appConfig);

    assertEquals(Optional.of("id"), entityIdColumnsConfigs.getIdKey("FOO"));
    assertEquals(Optional.of("id2"), entityIdColumnsConfigs.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("BAZ"));
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
    EntityIdColumnsConfigs entityIdColumnsConfigs = EntityIdColumnsConfigs.fromConfig(appConfig);

    assertEquals(Optional.of("id"), entityIdColumnsConfigs.getIdKey("FOO"));
    assertEquals(Optional.of("id2"), entityIdColumnsConfigs.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("BAZ"));
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
    EntityIdColumnsConfigs entityIdColumnsConfigs = EntityIdColumnsConfigs.fromConfig(appConfig);

    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("FOO"));
    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("BAZ"));
  }

  @Test
  public void testNoDomainNorEntityIdColumnConfig() {
    String appConfigStr = "";
    Config appConfig = ConfigFactory.parseString(appConfigStr);
    EntityIdColumnsConfigs entityIdColumnsConfigs = EntityIdColumnsConfigs.fromConfig(appConfig);

    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("FOO"));
    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("BAR"));
    assertEquals(Optional.empty(), entityIdColumnsConfigs.getIdKey("BAZ"));
  }
}
