plugins {
  java
  application
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
}

dependencies {
  implementation(project(":gateway-service-factory"))

  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.12.2")
  implementation("org.hypertrace.core.serviceframework:platform-grpc-service-framework:0.1.59")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("com.typesafe:config:1.4.1")

  runtimeOnly("io.grpc:grpc-netty:1.57.2")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.1")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dservice.name=${project.name}")
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      ports.add(50071)
      adminPort.set(50072)
    }
  }
}
