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

  constraints {
    implementation("com.google.guava:guava:30.0-jre") {
      because("Information Disclosure [Medium Severity][https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415] in com.google.guava:guava@29.0-android")
    }
    testRuntimeOnly("io.netty:netty-codec-http2:4.1.53.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439")
    }
    testRuntimeOnly("io.netty:netty-handler-proxy:4.1.53.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439s")
    }
  }

  implementation("org.hypertrace.core.query.service:query-service-client:0.5.2")
  implementation("org.hypertrace.core.attribute.service:attribute-service-client:0.9.3")
  implementation("org.hypertrace.entity.service:entity-service-client:0.5.0")
  implementation("org.hypertrace.entity.service:entity-service-api:0.5.0")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.2")
  implementation("org.hypertrace.core.serviceframework:platform-metrics:0.1.19")

  // Config
  implementation("com.typesafe:config:1.4.1")

  // Common utilities
  implementation("org.apache.commons:commons-lang3:3.10")
  implementation("org.apache.commons:commons-math:2.2")
  implementation("com.google.protobuf:protobuf-java-util:3.13.0")

  // Metrics
  implementation("io.dropwizard.metrics:metrics-core:4.1.9")

  // Needed by clusters snapshots
  implementation("com.fasterxml.jackson.core:jackson-annotations:2.11.1")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.11.1")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.6.28")
  testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")
  testImplementation("io.grpc:grpc-netty:1.33.1")
}
