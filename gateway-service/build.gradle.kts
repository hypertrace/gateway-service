plugins {
  java
  application
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
}

dependencies {
  implementation(project(":gateway-service-impl"))

  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.6.2")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.29")

  implementation("io.grpc:grpc-netty:1.42.0")

  // Logging
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.apache.logging.log4j:log4j-api:2.16.0")
  implementation("org.apache.logging.log4j:log4j-core:2.16.0")
  implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.17.0")

  // Config
  implementation("com.typesafe:config:1.4.1")

  constraints {
    runtimeOnly("io.netty:netty-codec-http2:4.1.71.Final")
    runtimeOnly("io.netty:netty-handler-proxy:4.1.71.Final")
  }
}

application {
  mainClassName = "org.hypertrace.core.serviceframework.PlatformServiceLauncher"
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dbootstrap.config.uri=file:$projectDir/src/main/resources/configs", "-Dservice.name=${project.name}")
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      port.set(50072)
    }
  }
}
