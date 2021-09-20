rootProject.name = "gateway-service-root"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://hypertrace.jfrog.io/artifactory/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.2.0"
}

include(":gateway-service-api")
include(":gateway-service-impl")
include(":gateway-service")
include("gateway-service-baseline-lib")
