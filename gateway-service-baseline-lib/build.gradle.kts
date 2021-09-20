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
    implementation("org.apache.commons:commons-lang3:3.10")
    implementation("org.apache.commons:commons-math:2.2")
    implementation("com.google.protobuf:protobuf-java-util:3.17.3")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
}
