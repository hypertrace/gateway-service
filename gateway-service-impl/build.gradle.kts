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

  implementation("org.hypertrace.core.query.service:query-service-client:0.7.0")
  implementation("org.hypertrace.core.attribute.service:attribute-service-client:0.12.5")
  implementation("org.hypertrace.entity.service:entity-service-client:0.8.11")
  implementation("org.hypertrace.entity.service:entity-service-api:0.8.11")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.7.0")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.29")

  // Config
  implementation("com.typesafe:config:1.4.1")

  // Common utilities
  implementation("org.apache.commons:commons-lang3:3.10")
  implementation("org.apache.commons:commons-math:2.2")
  implementation("com.google.protobuf:protobuf-java-util:3.19.2")

  implementation("com.fasterxml.jackson.core:jackson-annotations:2.12.4")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.12.4")

  constraints {
    implementation("com.google.protobuf:protobuf-java:3.19.2") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEPROTOBUF-2331703")
    }
  }

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.9.0")
  testImplementation("org.mockito:mockito-inline:3.9.0")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
  testImplementation("io.grpc:grpc-netty:1.43.2")
}
