plugins {
  java
  application
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
}

dependencies {
  implementation(project(":gateway-service-impl"))

  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.3.2")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.18")

  implementation("io.grpc:grpc-netty:1.33.1")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.apache.logging.log4j:log4j-api:2.13.3")
  implementation("org.apache.logging.log4j:log4j-core:2.13.3")
  implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  // Config
  implementation("com.typesafe:config:1.4.1")

  constraints {
    runtimeOnly("io.netty:netty-codec-http2:4.1.54.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439")
    }
    runtimeOnly("io.netty:netty-handler-proxy:4.1.54.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1020439s")
    }
  }
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:${projectDir}/src/main/resources/configs", "-Dservice.name=${project.name}")
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      port.set(50072)
    }
  }
}