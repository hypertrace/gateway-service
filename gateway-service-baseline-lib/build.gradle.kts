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

    // Common utilities
    implementation("org.apache.commons:commons-lang3:3.10")
    implementation("org.apache.commons:commons-math:2.2")
    implementation("com.google.protobuf:protobuf-java-util:3.17.3")

    implementation("com.fasterxml.jackson.core:jackson-annotations:2.12.4")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.12.4")

    testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
    testImplementation("org.mockito:mockito-core:3.9.0")
    testImplementation("org.mockito:mockito-inline:3.9.0")
    testImplementation("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")
    testImplementation("io.grpc:grpc-netty:1.39.0")
}
