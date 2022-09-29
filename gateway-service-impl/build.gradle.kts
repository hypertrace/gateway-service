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
  api(project(":gateway-service-baseline-lib"))

  implementation("org.hypertrace.core.query.service:query-service-client:0.8.0")
  implementation("org.hypertrace.core.attribute.service:attribute-service-client:0.13.13")

  implementation("org.hypertrace.entity.service:entity-service-client:0.8.46")
  implementation("org.hypertrace.entity.service:entity-service-api:0.8.46")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.7.5")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.7.5")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.37")

  // Config
  implementation("com.typesafe:config:1.4.1")

  // Common utilities
  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("com.google.protobuf:protobuf-java-util:3.19.4")
  implementation("com.google.guava:guava:31.0.1-jre")

  implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.2")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
  testImplementation("org.mockito:mockito-core:4.3.1")
  testImplementation("org.mockito:mockito-inline:4.3.1")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
  testImplementation("io.grpc:grpc-netty:1.44.0")

  constraints {
    testRuntimeOnly("io.netty:netty-common:4.1.77.Final")
  }
}
