package org.hypertrace.gateway.service.entity.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DomainObjectConfigsTest {
  @AfterEach
  public void teardown() {
    DomainObjectConfigs.clearDomainObjectConfigs();
  }

  @Test
  public void testWithoutFilter() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = EVENT\n"
            + "    key = id\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = SERVICE\n"
            + "        key = id\n"
            + "      },\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = isExternal\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);

    DomainObjectConfigs.init(config);

    DomainObjectConfig domainObject = DomainObjectConfigs.getDomainObjectConfig("EVENT").get();
    Assertions.assertEquals("EVENT", domainObject.getType());
    DomainObjectMapping serviceDomainObject =
        domainObject
            .getAttributeMappings()
            .get(
                new DomainObjectConfig.DomainAttribute(
                    AttributeScope.EVENT.name(), "id", false))
            .get(0);
    Assertions.assertEquals("SERVICE", serviceDomainObject.getScope());
    Assertions.assertEquals("id", serviceDomainObject.getKey());

    DomainObjectMapping apiDomainObject =
        domainObject
            .getAttributeMappings()
            .get(
                new DomainObjectConfig.DomainAttribute(
                    AttributeScope.EVENT.name(), "id", false))
            .get(1);
    Assertions.assertEquals("API", apiDomainObject.getScope());
    Assertions.assertEquals("isExternal", apiDomainObject.getKey());
  }

  @Test
  public void testWithFilter() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = EVENT\n"
            + "    key = id\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = SERVICE\n"
            + "        key = id\n"
            + "      },\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = isExternal\n"
            + "        filter {\n"
            + "          value = true\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);

    DomainObjectConfigs.init(config);

    DomainObjectConfig domainObject = DomainObjectConfigs.getDomainObjectConfig("EVENT").get();
    Assertions.assertEquals("EVENT", domainObject.getType());
    DomainObjectMapping serviceDomainObject =
        domainObject
            .getAttributeMappings()
            .get(
                new DomainObjectConfig.DomainAttribute(
                    AttributeScope.EVENT.name(), "id", false))
            .get(0);
    Assertions.assertEquals("SERVICE", serviceDomainObject.getScope());
    Assertions.assertEquals("id", serviceDomainObject.getKey());

    DomainObjectMapping apiDomainObject =
        domainObject
            .getAttributeMappings()
            .get(
                new DomainObjectConfig.DomainAttribute(
                    AttributeScope.EVENT.name(), "id", false))
            .get(1);
    Assertions.assertEquals("API", apiDomainObject.getScope());
    Assertions.assertEquals("isExternal", apiDomainObject.getKey());
    Assertions.assertEquals("true", apiDomainObject.getFilter().getValue());
  }
}
