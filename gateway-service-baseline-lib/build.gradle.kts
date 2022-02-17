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
  implementation("com.google.protobuf:protobuf-java-util:3.19.4")
  constraints {
    implementation("com.google.protobuf:protobuf-java:3.19.2") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEPROTOBUF-2331703")
    }
    implementation("com.google.code.gson:gson:2.8.9") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLECODEGSON-1730327")
    }
  }
  testImplementation("org.junit.jupiter:junit-jupiter:5.8.2")
}
