plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(project(":gateway-service-api"))

  implementation("org.hypertrace.core.query.service:query-service-client:0.1.14")
  implementation("org.hypertrace.core.attribute.service:attribute-service-client:0.3.0")
  implementation("org.hypertrace.entity.service:entity-service-client:0.1.24-SNAPSHOT")
  implementation("org.hypertrace.entity.service:entity-service-api:0.1.24-SNAPSHOT")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.0")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.14")

  // Config
  implementation("com.typesafe:config:1.4.0")

  // Common utilities
  implementation("org.apache.commons:commons-lang3:3.10")
  implementation("com.google.protobuf:protobuf-java-util:3.13.0")

  // Metrics
  implementation("io.dropwizard.metrics:metrics-core:4.1.9")

  // Needed by clusters snapshots
  implementation("com.fasterxml.jackson.core:jackson-annotations:2.11.1")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  testImplementation("org.mockito:mockito-core:3.3.3")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
  testImplementation("io.grpc:grpc-netty:1.32.1")
}
