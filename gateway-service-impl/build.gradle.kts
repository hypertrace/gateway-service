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

  annotationProcessor("org.projectlombok:lombok:1.18.22")
  compileOnly("org.projectlombok:lombok:1.18.18")

  implementation("org.hypertrace.core.query.service:query-service-client:0.8.0")
  implementation("org.hypertrace.core.attribute.service:attribute-service-client:0.14.13")

  implementation("org.hypertrace.entity.service:entity-service-client:0.8.56")
  implementation("org.hypertrace.entity.service:entity-service-api:0.8.56")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.11.2")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.12.0")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.52")

  // Config
  implementation("com.typesafe:config:1.4.1")

  // Common utilities
  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("com.google.protobuf:protobuf-java-util:3.21.12")
  implementation("com.google.guava:guava:32.0.1-jre")
  implementation("com.google.inject:guice:5.0.1")

  implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.2")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")

  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
  testImplementation("org.mockito:mockito-junit-jupiter:4.10.0")
  testImplementation("org.mockito:mockito-core:4.10.0")
  testImplementation("org.mockito:mockito-inline:4.10.0")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
  testImplementation("io.grpc:grpc-netty:1.50.0")
}
