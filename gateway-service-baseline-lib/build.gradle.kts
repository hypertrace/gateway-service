plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

tasks.test {
  useJUnitPlatform()
}

dependencies {
  api(project(":gateway-service-api"))

  // Common utilities
  implementation("org.apache.commons:commons-lang3:3.12.0")
  implementation("org.apache.commons:commons-math3:3.6.1")
  implementation("com.google.protobuf:protobuf-java-util:3.21.12")
  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}
