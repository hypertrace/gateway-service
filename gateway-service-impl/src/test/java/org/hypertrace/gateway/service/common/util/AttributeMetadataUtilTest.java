package org.hypertrace.gateway.service.common.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeScope;
import org.hypertrace.gateway.service.common.AttributeMetadataProvider;
import org.hypertrace.gateway.service.common.RequestContext;
import org.hypertrace.gateway.service.entity.config.DomainObjectConfigs;
import org.hypertrace.gateway.service.v1.common.DomainEntityType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AttributeMetadataUtilTest {
  @AfterEach
  public void teardown() {
    DomainObjectConfigs.clearDomainObjectConfigs();
  }

  @Test
  public void testIdAttributesNotInDomainConfig() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = API\n"
            + "    key = apiId\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = apiId\n"
            + "      }\n"
            + "    ]\n"
            + "  },"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);
    DomainObjectConfigs.init(config);

    AttributeMetadataProvider provider = mock(AttributeMetadataProvider.class);
    String entityType = DomainEntityType.SERVICE.name();

    Assertions.assertEquals(
        List.of(),
        AttributeMetadataUtil.getIdAttributeIds(provider, mock(RequestContext.class), entityType));
  }

  @Test
  public void testApiIdAttributeInDomainConfig() {
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = API\n"
            + "    key = apiId\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = apiId\n"
            + "      }\n"
            + "    ]\n"
            + "  },"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);
    DomainObjectConfigs.init(config);

    AttributeMetadataProvider provider = mock(AttributeMetadataProvider.class);
    String entityType = DomainEntityType.API.name();
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API), eq("apiId")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("API.apiId").build()));

    Assertions.assertEquals(
        List.of("API.apiId"),
        AttributeMetadataUtil.getIdAttributeIds(provider, mock(RequestContext.class), entityType));
  }

  @Test
  public void testKnownEntitiesIdAttributesInDomainConfig() {
    // This is a section of the same config in application.conf
    String domainObjectConfig =
        "domainobject.config = [\n"
            + "  {\n"
            + "    scope = API\n"
            + "    key = apiId\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = API\n"
            + "        key = apiId\n"
            + "      }\n"
            + "    ]\n"
            + "  },"
            + "  {\n"
            + "    scope = SERVICE\n"
            + "    key = id\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = SERVICE\n"
            + "        key = id\n"
            + "      }\n"
            + "    ]\n"
            + "  },"
            + "  {\n"
            + "    scope = BACKEND\n"
            + "    key = id\n"
            + "    primaryKey = true\n"
            + "    mapping = [\n"
            + "      {\n"
            + "        scope = BACKEND\n"
            + "        key = id\n"
            + "      }\n"
            + "    ]\n"
            + "  },"
            + "]";

    Config config = ConfigFactory.parseString(domainObjectConfig);
    DomainObjectConfigs.init(config);

    AttributeMetadataProvider provider = mock(AttributeMetadataProvider.class);
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.API), eq("apiId")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("API.apiId").build()));
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.SERVICE), eq("id")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("SERVICE.id").build()));
    when(provider.getAttributeMetadata(
            any(RequestContext.class), eq(AttributeScope.BACKEND), eq("id")))
        .thenReturn(Optional.of(AttributeMetadata.newBuilder().setId("BACKEND.id").build()));

    Assertions.assertEquals(
        List.of("API.apiId"),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, mock(RequestContext.class), DomainEntityType.API.name()));
    Assertions.assertEquals(
        List.of("SERVICE.id"),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, mock(RequestContext.class), DomainEntityType.SERVICE.name()));
    Assertions.assertEquals(
        List.of("BACKEND.id"),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, mock(RequestContext.class), DomainEntityType.BACKEND.name()));

    // Unsupported entities
    Assertions.assertEquals(
        List.of(),
        AttributeMetadataUtil.getIdAttributeIds(
            provider, mock(RequestContext.class), DomainEntityType.NAMESPACE.name()));
  }
}
