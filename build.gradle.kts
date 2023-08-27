plugins {
  id("org.hypertrace.repository-plugin") version "0.4.1"
  id("org.hypertrace.ci-utils-plugin") version "0.3.0"
  id("org.hypertrace.publish-plugin") version "1.0.2" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.2.0" apply false
  id("org.hypertrace.docker-java-application-plugin") version "0.9.4" apply false
  id("org.hypertrace.docker-publish-plugin") version "0.9.4" apply false
  id("org.hypertrace.code-style-plugin") version "1.1.0" apply false
  id("org.owasp.dependencycheck") version "8.1.2"
}

subprojects {
  group = "org.hypertrace.gateway.service"
  pluginManager.withPlugin("org.hypertrace.publish-plugin") {
    configure<org.hypertrace.gradle.publishing.HypertracePublishExtension> {
      license.set(org.hypertrace.gradle.publishing.License.TRACEABLE_COMMUNITY)
    }
  }

  pluginManager.withPlugin("java") {
    configure<JavaPluginExtension> {
      sourceCompatibility = JavaVersion.VERSION_11
      targetCompatibility = JavaVersion.VERSION_11
    }

    apply(plugin = "org.hypertrace.code-style-plugin")
  }
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
  failBuildOnCVSS = 7.0F
}